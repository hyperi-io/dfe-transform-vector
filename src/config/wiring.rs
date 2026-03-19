// Project:   dfe-transform-vector
// File:      src/config/wiring.rs
// Purpose:   DAG wiring and validation for Vector component topology
// Language:  Rust
//
// License:   FSL-1.1-ALv2
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! DAG wiring and validation.
//!
//! Parses component labels and `inputs` references from loaded transform
//! YAMLs, auto-wires source→transforms→sink, and validates the
//! resulting topology.

use std::collections::{HashMap, HashSet};

use serde_yaml_ng::Value;
use tracing::debug;

use super::generate::{SINK_LABEL, SOURCE_LABEL};
use super::transforms::LoadedTransform;
use crate::Result;

/// A parsed component extracted from YAML.
#[derive(Debug, Clone)]
pub struct Component {
    /// Component label (e.g., "parse", "enrich", "filter").
    pub label: String,
    /// Component kind: "source", "transform", or "sink".
    pub kind: String,
    /// Labels this component takes input from.
    pub inputs: Vec<String>,
}

/// Result of DAG wiring: the list of sink inputs (last transform labels).
#[derive(Debug)]
pub struct WiringResult {
    /// Labels to wire as inputs to dfe_sink.
    pub sink_inputs: Vec<String>,
    /// All components in the topology (for validation diagnostics).
    pub components: Vec<Component>,
}

/// Extract all components from loaded transform YAMLs.
///
/// Parses `sources`, `transforms`, and `sinks` top-level keys.
pub fn extract_components(transforms: &[LoadedTransform]) -> Result<Vec<Component>> {
    let mut components = Vec::new();

    for lt in transforms {
        for kind in &["sources", "transforms", "sinks"] {
            if let Some(Value::Mapping(entries)) = lt.yaml.get(Value::String(kind.to_string())) {
                for (label_val, component_val) in entries {
                    let label = label_val
                        .as_str()
                        .ok_or_else(|| {
                            crate::Error::Config(format!(
                                "non-string component label in {}",
                                lt.path.display()
                            ))
                        })?
                        .to_string();

                    let inputs = extract_inputs(component_val);

                    let component_kind = kind.trim_end_matches('s').to_string();
                    debug!(label = %label, kind = %component_kind, inputs = ?inputs, "extracted component");
                    components.push(Component {
                        label,
                        kind: component_kind,
                        inputs,
                    });
                }
            }
        }
    }

    Ok(components)
}

/// Auto-wire the DAG: ensure first transforms get `dfe_source` as input,
/// discover the last transforms for wiring to `dfe_sink`.
///
/// Returns a `WiringResult` with the labels that should be wired as
/// sink inputs, and the full component list.
pub fn auto_wire(mut components: Vec<Component>) -> Result<WiringResult> {
    if components.is_empty() {
        // No transforms: source wires directly to sink
        debug!("no transforms — wiring source directly to sink");
        return Ok(WiringResult {
            sink_inputs: vec![SOURCE_LABEL.to_string()],
            components,
        });
    }

    // Build a set of all defined component labels (including our canonical ones)
    let mut defined: HashSet<String> = HashSet::new();
    defined.insert(SOURCE_LABEL.to_string());
    defined.insert(SINK_LABEL.to_string());
    for c in &components {
        defined.insert(c.label.clone());
    }

    // Auto-wire: components with no inputs get wired to dfe_source
    for c in &mut components {
        if c.kind == "transform" && c.inputs.is_empty() {
            debug!(label = %c.label, "auto-wiring transform to dfe_source");
            c.inputs = vec![SOURCE_LABEL.to_string()];
        }
    }

    // Normalise "source" references to "dfe_source" for backwards compat
    for c in &mut components {
        for input in &mut c.inputs {
            if input == "source" {
                *input = SOURCE_LABEL.to_string();
            }
        }
    }

    // Build set of labels that are referenced as inputs by other components
    let mut referenced_as_input: HashSet<String> = HashSet::new();
    for c in &components {
        for input in &c.inputs {
            referenced_as_input.insert(input.clone());
        }
    }

    // Discover "terminal" transforms: transforms not referenced by any other
    // user component. These are the last in the chain and should feed dfe_sink.
    let user_labels: HashSet<String> = components.iter().map(|c| c.label.clone()).collect();

    let terminal: Vec<String> = components
        .iter()
        .filter(|c| {
            c.kind == "transform"
                && !user_labels.iter().any(|other_label| {
                    components
                        .iter()
                        .find(|oc| &oc.label == other_label)
                        .is_some_and(|oc| oc.inputs.contains(&c.label))
                })
        })
        .map(|c| c.label.clone())
        .collect();

    // If no terminal found (shouldn't happen if we have transforms), fallback
    #[allow(clippy::unwrap_used)]
    let sink_inputs = if terminal.is_empty() {
        // Fall back: use the last component in file-order (vec is non-empty — checked above)
        vec![components.last().unwrap().label.clone()]
    } else {
        terminal
    };

    debug!(sink_inputs = ?sink_inputs, "discovered terminal transforms for sink");

    Ok(WiringResult {
        sink_inputs,
        components,
    })
}

/// Validate the wired DAG topology.
///
/// Checks:
/// - All `inputs` references resolve to defined components
/// - No orphaned transforms (every transform is reachable from dfe_source)
/// - No cycles
pub fn validate_dag(result: &WiringResult) -> Result<()> {
    // Build the full set of defined labels
    let mut defined: HashSet<String> = HashSet::new();
    defined.insert(SOURCE_LABEL.to_string());
    defined.insert(SINK_LABEL.to_string());
    defined.insert("internal_metrics".to_string());
    defined.insert("prometheus_exporter".to_string());
    for c in &result.components {
        defined.insert(c.label.clone());
    }

    // Check all inputs references resolve
    for c in &result.components {
        for input in &c.inputs {
            if !defined.contains(input) {
                return Err(crate::Error::Validation(format!(
                    "component '{}' references undefined input '{}'",
                    c.label, input
                )));
            }
        }
    }

    // Check reachability from dfe_source (no orphans)
    let mut reachable: HashSet<String> = HashSet::new();
    reachable.insert(SOURCE_LABEL.to_string());

    // BFS from source
    let mut queue: Vec<String> = vec![SOURCE_LABEL.to_string()];
    while let Some(current) = queue.pop() {
        for c in &result.components {
            if c.inputs.contains(&current) && !reachable.contains(&c.label) {
                reachable.insert(c.label.clone());
                queue.push(c.label.clone());
            }
        }
    }

    // Check all transform components are reachable
    for c in &result.components {
        if c.kind == "transform" && !reachable.contains(&c.label) {
            return Err(crate::Error::Validation(format!(
                "transform '{}' is not reachable from {} (orphaned)",
                c.label, SOURCE_LABEL
            )));
        }
    }

    // Check for cycles using DFS with colouring
    detect_cycles(&result.components)?;

    Ok(())
}

/// Detect cycles in the component graph using DFS with white/grey/black colouring.
fn detect_cycles(components: &[Component]) -> Result<()> {
    // Build adjacency list: label → list of downstream labels
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    adjacency.insert(SOURCE_LABEL.to_string(), vec![]);

    for c in components {
        adjacency.entry(c.label.clone()).or_default();
        for input in &c.inputs {
            adjacency
                .entry(input.clone())
                .or_default()
                .push(c.label.clone());
        }
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Colour {
        White,
        Grey,
        Black,
    }

    let mut colours: HashMap<String, Colour> = HashMap::new();
    for key in adjacency.keys() {
        colours.insert(key.clone(), Colour::White);
    }

    fn dfs(
        node: &str,
        adjacency: &HashMap<String, Vec<String>>,
        colours: &mut HashMap<String, Colour>,
    ) -> std::result::Result<(), String> {
        colours.insert(node.to_string(), Colour::Grey);
        if let Some(neighbours) = adjacency.get(node) {
            for next in neighbours {
                match colours.get(next.as_str()) {
                    Some(Colour::Grey) => {
                        return Err(format!("cycle detected: '{node}' → '{next}'"));
                    }
                    Some(Colour::White) => {
                        dfs(next, adjacency, colours)?;
                    }
                    _ => {}
                }
            }
        }
        colours.insert(node.to_string(), Colour::Black);
        Ok(())
    }

    for label in adjacency.keys() {
        if colours.get(label) == Some(&Colour::White) {
            dfs(label, &adjacency, &mut colours).map_err(crate::Error::Validation)?;
        }
    }

    Ok(())
}

/// Extract the `inputs` field from a YAML component value.
fn extract_inputs(value: &Value) -> Vec<String> {
    match value.get(Value::String("inputs".to_string())) {
        Some(Value::Sequence(seq)) => seq
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => vec![],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_transform(yaml_str: &str) -> LoadedTransform {
        LoadedTransform {
            path: "test.yaml".into(),
            yaml: serde_yaml_ng::from_str(yaml_str).unwrap(),
        }
    }

    #[test]
    fn extract_components_parses_transforms() {
        let lt = make_transform(
            r#"
transforms:
  parse:
    type: remap
    inputs: ["dfe_source"]
    source: ". = parse_json!(.message)"
  enrich:
    type: remap
    inputs: ["parse"]
    source: ".x = 1"
"#,
        );

        let components = extract_components(&[lt]).unwrap();
        assert_eq!(components.len(), 2);
        assert_eq!(components[0].label, "parse");
        assert_eq!(components[0].inputs, vec!["dfe_source"]);
        assert_eq!(components[1].label, "enrich");
        assert_eq!(components[1].inputs, vec!["parse"]);
    }

    #[test]
    fn auto_wire_no_transforms() {
        let result = auto_wire(vec![]).unwrap();
        assert_eq!(result.sink_inputs, vec!["dfe_source"]);
    }

    #[test]
    fn auto_wire_injects_source_input() {
        let components = vec![Component {
            label: "parse".into(),
            kind: "transform".into(),
            inputs: vec![], // No inputs — should be auto-wired
        }];
        let result = auto_wire(components).unwrap();
        assert_eq!(result.components[0].inputs, vec!["dfe_source"]);
        assert_eq!(result.sink_inputs, vec!["parse"]);
    }

    #[test]
    fn auto_wire_normalises_source_ref() {
        let components = vec![Component {
            label: "parse".into(),
            kind: "transform".into(),
            inputs: vec!["source".into()], // "source" → "dfe_source"
        }];
        let result = auto_wire(components).unwrap();
        assert_eq!(result.components[0].inputs, vec!["dfe_source"]);
    }

    #[test]
    fn auto_wire_chain_discovers_terminal() {
        let components = vec![
            Component {
                label: "parse".into(),
                kind: "transform".into(),
                inputs: vec!["dfe_source".into()],
            },
            Component {
                label: "enrich".into(),
                kind: "transform".into(),
                inputs: vec!["parse".into()],
            },
            Component {
                label: "filter".into(),
                kind: "transform".into(),
                inputs: vec!["enrich".into()],
            },
        ];
        let result = auto_wire(components).unwrap();
        assert_eq!(result.sink_inputs, vec!["filter"]);
    }

    #[test]
    fn validate_dag_catches_undefined_input() {
        let result = WiringResult {
            sink_inputs: vec!["filter".into()],
            components: vec![Component {
                label: "parse".into(),
                kind: "transform".into(),
                inputs: vec!["nonexistent".into()],
            }],
        };
        let err = validate_dag(&result).unwrap_err();
        assert!(err.to_string().contains("undefined input 'nonexistent'"));
    }

    #[test]
    fn validate_dag_catches_orphan() {
        let result = WiringResult {
            sink_inputs: vec!["connected".into()],
            components: vec![
                Component {
                    label: "connected".into(),
                    kind: "transform".into(),
                    inputs: vec!["dfe_source".into()],
                },
                Component {
                    label: "orphan".into(),
                    kind: "transform".into(),
                    inputs: vec!["orphan_input".into()],
                },
            ],
        };
        // orphan_input is not defined — this should fail on undefined input
        let err = validate_dag(&result).unwrap_err();
        assert!(err.to_string().contains("undefined input"));
    }

    #[test]
    fn validate_dag_catches_cycle() {
        // a takes from dfe_source and b, b takes from a → cycle between a and b
        let result = WiringResult {
            sink_inputs: vec!["b".into()],
            components: vec![
                Component {
                    label: "a".into(),
                    kind: "transform".into(),
                    inputs: vec!["dfe_source".into(), "b".into()],
                },
                Component {
                    label: "b".into(),
                    kind: "transform".into(),
                    inputs: vec!["a".into()],
                },
            ],
        };
        let err = validate_dag(&result).unwrap_err();
        assert!(err.to_string().contains("cycle detected"));
    }

    #[test]
    fn validate_dag_accepts_valid_chain() {
        let result = WiringResult {
            sink_inputs: vec!["filter".into()],
            components: vec![
                Component {
                    label: "parse".into(),
                    kind: "transform".into(),
                    inputs: vec!["dfe_source".into()],
                },
                Component {
                    label: "enrich".into(),
                    kind: "transform".into(),
                    inputs: vec!["parse".into()],
                },
                Component {
                    label: "filter".into(),
                    kind: "transform".into(),
                    inputs: vec!["enrich".into()],
                },
            ],
        };
        validate_dag(&result).unwrap();
    }
}
