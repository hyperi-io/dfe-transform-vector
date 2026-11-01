// Project:   dfe-transform-vector
// File:      src/config/wiring.rs
// Purpose:   DAG wiring and validation for Vector component topology
// Language:  Rust
//
// License:   BUSL-1.1
// Copyright: (c) 2026 HYPERI PTY LIMITED

//! DAG wiring and validation.
//!
//! Parses component labels and `inputs` references from loaded transform
//! YAMLs, auto-wires source->transforms->sink, and validates the
//! resulting topology.
//!
//! An input names a component's default output (`parse`) or one of its named
//! outputs (`split.accepted`). A `route` or `exclusive_route` has named outputs
//! only, read from its config, so every output that nothing consumes is wired
//! to the sink, `_unmatched` included.

use std::collections::{HashMap, HashSet};

use serde_json::Value;
use tracing::debug;

use super::generate::{SINK_LABEL, SOURCE_LABEL};
use super::transforms::LoadedTransform;
use crate::Result;

/// Vector's named output for events no route matched.
const UNMATCHED: &str = "_unmatched";

/// A parsed component extracted from YAML.
#[derive(Debug, Clone)]
pub struct Component {
    /// Component label (e.g., "parse", "enrich", "filter").
    pub label: String,
    /// Component kind: "source", "transform", or "sink".
    pub kind: String,
    /// Outputs this component takes input from: a label, or `label.port`.
    pub inputs: Vec<String>,
    /// Its named outputs when it has no default one, as a `route` does. Empty
    /// for a component whose output is its label.
    pub outputs: Vec<String>,
}

impl Component {
    /// The outputs other components and the sink read, as inputs name them.
    fn output_ids(&self) -> Vec<String> {
        if self.outputs.is_empty() {
            vec![self.label.clone()]
        } else {
            self.outputs
                .iter()
                .map(|port| format!("{}.{port}", self.label))
                .collect()
        }
    }
}

/// Result of DAG wiring: the list of sink inputs (last transform labels).
#[derive(Debug)]
pub struct WiringResult {
    /// Outputs to wire as inputs to dfe_sink.
    pub sink_inputs: Vec<String>,
    /// All components in the topology (for validation diagnostics).
    pub components: Vec<Component>,
}

/// Extract all components from loaded transform YAMLs.
///
/// Parses `sources`, `transforms`, and `sinks` top-level keys.
///
/// # Errors
///
/// Currently never fails: every input yields a component list.
pub fn extract_components(transforms: &[LoadedTransform]) -> Result<Vec<Component>> {
    let mut components = Vec::new();

    for lt in transforms {
        for kind in &["sources", "transforms", "sinks"] {
            if let Some(Value::Object(entries)) = lt.yaml.get(*kind) {
                for (label, component_val) in entries {
                    let label = label.clone();

                    let inputs = extract_inputs(component_val);
                    let outputs = named_outputs(component_val);

                    let component_kind = kind.trim_end_matches('s').to_string();
                    debug!(label = %label, kind = %component_kind, inputs = ?inputs, "extracted component");
                    components.push(Component {
                        label,
                        kind: component_kind,
                        inputs,
                        outputs,
                    });
                }
            }
        }
    }

    Ok(components)
}

/// Auto-wire the DAG: ensure first transforms get `dfe_source` as input,
/// discover the outputs nothing consumes for wiring to `dfe_sink`.
///
/// Returns a `WiringResult` with the outputs that should be wired as
/// sink inputs, and the full component list.
///
/// # Errors
///
/// Currently never fails: every component list yields a wiring.
pub fn auto_wire(mut components: Vec<Component>) -> Result<WiringResult> {
    if components.is_empty() {
        // No transforms: source wires directly to sink
        debug!("no transforms -- wiring source directly to sink");
        return Ok(WiringResult {
            sink_inputs: vec![SOURCE_LABEL.to_string()],
            components,
        });
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

    // Terminal outputs: transform outputs no user component reads. These are
    // the ends of the chain and feed dfe_sink.
    let consumed: HashSet<&str> = components
        .iter()
        .flat_map(|c| c.inputs.iter().map(String::as_str))
        .collect();
    let terminal: Vec<String> = components
        .iter()
        .filter(|c| c.kind == "transform")
        .flat_map(Component::output_ids)
        .filter(|id| !consumed.contains(id.as_str()))
        .collect();

    // If no terminal found (shouldn't happen if we have transforms), fallback
    let sink_inputs = if terminal.is_empty() {
        // Fall back: use the last component in file-order (vec is non-empty -- checked above)
        components
            .last()
            .map(Component::output_ids)
            .unwrap_or_default()
    } else {
        terminal
    };

    debug!(sink_inputs = ?sink_inputs, "discovered terminal outputs for sink");

    Ok(WiringResult {
        sink_inputs,
        components,
    })
}

/// The component an input reads from, and the named output it names, if any.
///
/// Vector component labels carry no `.`, so the first one splits off the port.
fn split_input(input: &str) -> (&str, Option<&str>) {
    match input.split_once('.') {
        Some((label, port)) => (label, Some(port)),
        None => (input, None),
    }
}

/// Validate the wired DAG topology.
///
/// Checks:
/// - All `inputs` references resolve to defined components, and to an output
///   the component has
/// - No orphaned transforms (every transform is reachable from dfe_source)
/// - No cycles
///
/// # Errors
///
/// Returns [`crate::Error::Validation`] for an undefined input or output, an orphaned transform or a cycle.
pub fn validate_dag(result: &WiringResult) -> Result<()> {
    // Every defined label, with the named outputs of those that have only those
    let mut defined: HashMap<&str, &[String]> = HashMap::new();
    for builtin in [
        SOURCE_LABEL,
        SINK_LABEL,
        "internal_metrics",
        "prometheus_exporter",
    ] {
        defined.insert(builtin, &[]);
    }
    for c in &result.components {
        defined.insert(c.label.as_str(), c.outputs.as_slice());
    }

    // Check all inputs references resolve
    for c in &result.components {
        for input in &c.inputs {
            let (label, port) = split_input(input);
            let Some(outputs) = defined.get(label) else {
                return Err(crate::Error::Validation(format!(
                    "component '{}' references undefined input '{}'",
                    c.label, input
                )));
            };
            match port {
                None if !outputs.is_empty() => {
                    return Err(crate::Error::Validation(format!(
                        "component '{}' references '{label}', which has named outputs only: \
                         read one of {}",
                        c.label,
                        outputs
                            .iter()
                            .map(|o| format!("{label}.{o}"))
                            .collect::<Vec<_>>()
                            .join(", ")
                    )));
                }
                Some(port) if !outputs.is_empty() && !outputs.iter().any(|o| o == port) => {
                    return Err(crate::Error::Validation(format!(
                        "component '{}' references '{input}', but '{label}' has no output \
                         '{port}'",
                        c.label
                    )));
                }
                None | Some(_) => {}
            }
        }
    }

    // Check reachability from dfe_source (no orphans)
    let mut reachable: HashSet<&str> = HashSet::new();
    reachable.insert(SOURCE_LABEL);

    // BFS from source
    let mut queue: Vec<&str> = vec![SOURCE_LABEL];
    while let Some(current) = queue.pop() {
        for c in &result.components {
            if !reachable.contains(c.label.as_str())
                && c.inputs.iter().any(|i| split_input(i).0 == current)
            {
                reachable.insert(c.label.as_str());
                queue.push(c.label.as_str());
            }
        }
    }

    // Check all transform components are reachable
    for c in &result.components {
        if c.kind == "transform" && !reachable.contains(c.label.as_str()) {
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
    #[derive(Clone, Copy, PartialEq)]
    enum Colour {
        White,
        Grey,
        Black,
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
                        return Err(format!("cycle detected: '{node}' -> '{next}'"));
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

    // Build adjacency list: label -> list of downstream labels
    let mut adjacency: HashMap<String, Vec<String>> = HashMap::new();
    adjacency.insert(SOURCE_LABEL.to_string(), vec![]);

    for c in components {
        adjacency.entry(c.label.clone()).or_default();
        for input in &c.inputs {
            adjacency
                .entry(split_input(input).0.to_string())
                .or_default()
                .push(c.label.clone());
        }
    }

    let mut colours: HashMap<String, Colour> = HashMap::new();
    for key in adjacency.keys() {
        colours.insert(key.clone(), Colour::White);
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
    match value.get("inputs") {
        Some(Value::Array(seq)) => seq
            .iter()
            .filter_map(|v| v.as_str().map(String::from))
            .collect(),
        Some(Value::String(s)) => vec![s.clone()],
        _ => vec![],
    }
}

/// The named outputs of a component with no default output: a `route`'s keys
/// or an `exclusive_route`'s route names, then `_unmatched` unless
/// `reroute_unmatched` is off. Empty for every other type.
fn named_outputs(value: &Value) -> Vec<String> {
    let mut outputs: Vec<String> = match value.get("type").and_then(Value::as_str) {
        Some("route") => value
            .get("route")
            .and_then(Value::as_object)
            .into_iter()
            .flat_map(|routes| routes.keys().cloned())
            .collect(),
        Some("exclusive_route") => value
            .get("routes")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|r| r.get("name").and_then(Value::as_str).map(String::from))
            .collect(),
        _ => return Vec::new(),
    };
    if value.get("reroute_unmatched").and_then(Value::as_bool) != Some(false) {
        outputs.push(UNMATCHED.to_string());
    }
    outputs
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_transform(yaml_str: &str) -> LoadedTransform {
        LoadedTransform {
            path: "test.yaml".into(),
            yaml: scalo::yaml::from_str(yaml_str, "test.yaml", scalo::yaml::Profile::Config)
                .unwrap(),
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
            outputs: vec![],
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
            outputs: vec![],
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
                outputs: vec![],
            },
            Component {
                label: "enrich".into(),
                kind: "transform".into(),
                inputs: vec!["parse".into()],
                outputs: vec![],
            },
            Component {
                label: "filter".into(),
                kind: "transform".into(),
                inputs: vec!["enrich".into()],
                outputs: vec![],
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
                outputs: vec![],
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
                    outputs: vec![],
                },
                Component {
                    label: "orphan".into(),
                    kind: "transform".into(),
                    inputs: vec!["orphan_input".into()],
                    outputs: vec![],
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
                    outputs: vec![],
                },
                Component {
                    label: "b".into(),
                    kind: "transform".into(),
                    inputs: vec!["a".into()],
                    outputs: vec![],
                },
            ],
        };
        let err = validate_dag(&result).unwrap_err();
        assert!(err.to_string().contains("cycle detected"));
    }

    const ROUTED: &str = r#"
transforms:
  split:
    type: route
    inputs: ["dfe_source"]
    route:
      accepted: '.ok == true'
      audit: '.audit == true'
  tag:
    type: remap
    inputs: ["split.accepted"]
    source: ".tagged = true"
"#;

    /// A route has named outputs only: each is an input in its own right, and
    /// each one nothing consumes feeds the sink, `_unmatched` included.
    #[test]
    fn named_outputs_wire_through_their_component() {
        let wiring = auto_wire(extract_components(&[make_transform(ROUTED)]).unwrap()).unwrap();
        validate_dag(&wiring).unwrap();
        let mut sink = wiring.sink_inputs.clone();
        sink.sort();
        assert_eq!(sink, ["split._unmatched", "split.audit", "tag"]);
    }

    #[test]
    fn exclusive_route_outputs_and_a_disabled_unmatched_are_read_from_the_config() {
        let lt = make_transform(
            r#"
transforms:
  pick:
    type: exclusive_route
    inputs: ["dfe_source"]
    routes:
      - name: errors
        condition: '.level == "error"'
      - name: rest
        condition: 'true'
  only:
    type: route
    inputs: ["pick.rest"]
    reroute_unmatched: false
    route:
      kept: 'true'
"#,
        );
        let wiring = auto_wire(extract_components(&[lt]).unwrap()).unwrap();
        validate_dag(&wiring).unwrap();
        let mut sink = wiring.sink_inputs.clone();
        sink.sort();
        assert_eq!(sink, ["only.kept", "pick._unmatched", "pick.errors"]);
    }

    /// A port the component does not have, a bare reference to a component
    /// with named outputs only, and a port on an undefined component, refused;
    /// a port on a component with a default output is Vector's to judge.
    #[test]
    fn a_reference_to_an_output_is_checked_against_its_component() {
        for (input, refused) in [
            ("split.missing", Some("has no output 'missing'")),
            ("split", Some("named outputs only")),
            ("nowhere.accepted", Some("undefined input")),
            ("split.accepted", None),
            ("dfe_source", None),
        ] {
            let lt = make_transform(&format!(
                "{ROUTED}  extra:\n    type: remap\n    inputs: [\"{input}\"]\n    source: '.x = 1'\n"
            ));
            let wiring = auto_wire(extract_components(&[lt]).unwrap()).unwrap();
            match (validate_dag(&wiring), refused) {
                (Ok(()), None) => {}
                (Err(e), Some(why)) => assert!(e.to_string().contains(why), "{input}: {e}"),
                (Ok(()), Some(why)) => panic!("{input}: passed, expected '{why}'"),
                (Err(e), None) => panic!("{input}: refused: {e}"),
            }
        }
    }

    #[test]
    fn a_cycle_through_a_named_output_is_caught() {
        let lt = make_transform(
            r#"
transforms:
  split:
    type: route
    inputs: ["dfe_source", "back"]
    route:
      loop: 'true'
  back:
    type: remap
    inputs: ["split.loop"]
    source: '.x = 1'
"#,
        );
        let wiring = auto_wire(extract_components(&[lt]).unwrap()).unwrap();
        let err = validate_dag(&wiring).unwrap_err();
        assert!(err.to_string().contains("cycle detected"), "{err}");
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
                    outputs: vec![],
                },
                Component {
                    label: "enrich".into(),
                    kind: "transform".into(),
                    inputs: vec!["parse".into()],
                    outputs: vec![],
                },
                Component {
                    label: "filter".into(),
                    kind: "transform".into(),
                    inputs: vec!["enrich".into()],
                    outputs: vec![],
                },
            ],
        };
        validate_dag(&result).unwrap();
    }
}
