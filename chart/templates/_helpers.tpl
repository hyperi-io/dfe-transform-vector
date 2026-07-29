{{/*
Expand the name of the chart.
*/}}
{{- define "dfe-transform-vector.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
Truncated at 63 chars because some K8s name fields are limited.
*/}}
{{- define "dfe-transform-vector.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "dfe-transform-vector.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "dfe-transform-vector.labels" -}}
helm.sh/chart: {{ include "dfe-transform-vector.chart" . }}
{{ include "dfe-transform-vector.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "dfe-transform-vector.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dfe-transform-vector.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Service account name.
*/}}
{{- define "dfe-transform-vector.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dfe-transform-vector.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Refuse to template a disk buffer with nowhere durable to put it.

`sink.buffer.type: disk` asks Vector for durability across restarts. Without a
claim the buffer goes to an emptyDir, so every write succeeds, nothing errors,
and the buffered events are discarded the moment the pod moves. That is worse
than no buffer -- it looks like a guarantee and is not one.

Failing at template time is the right place: this is the only point where both
the buffer type and the storage decision are visible together.
*/}}
{{- define "dfe-transform-vector.persistenceRequired" -}}
{{- $buffer := (.Values.config.sink).buffer | default dict -}}
{{- if and (eq ($buffer.type | default "memory") "disk") (not .Values.persistence.enabled) -}}
{{- fail "config.sink.buffer.type is 'disk' but persistence.enabled is false. A disk buffer on an emptyDir is discarded when the pod moves, so the durability it promises is absent. Set persistence.enabled=true (and persistence.size >= the buffer's max_size), or use buffer type 'memory'." -}}
{{- end -}}
{{- end }}

{{/*
kafka secret name — use existing or generate from fullname.
*/}}
{{- define "dfe-transform-vector.kafkaSecretName" -}}
{{- if .Values.kafka.existingSecret }}
{{- .Values.kafka.existingSecret }}
{{- else }}
{{- printf "%s-kafka" (include "dfe-transform-vector.fullname" .) }}
{{- end }}
{{- end }}
