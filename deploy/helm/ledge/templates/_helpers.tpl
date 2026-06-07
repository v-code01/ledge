{{/* Expand the name of the chart. */}}
{{- define "ledge.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Fully qualified app name (used for the StatefulSet + pod hostnames). */}}
{{- define "ledge.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/* Headless service name (peer DNS for Raft). */}}
{{- define "ledge.headless" -}}
{{- printf "%s-headless" (include "ledge.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Secret name (existing or chart-managed). */}}
{{- define "ledge.secretName" -}}
{{- if .Values.auth.existingSecret -}}
{{- .Values.auth.existingSecret -}}
{{- else -}}
{{- printf "%s-secret" (include "ledge.fullname" .) -}}
{{- end -}}
{{- end -}}

{{- define "ledge.labels" -}}
app.kubernetes.io/name: {{ include "ledge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version }}
{{- end -}}

{{- define "ledge.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ledge.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}
