# Telemetry

Feature | How
--------|-----
Disable all telemetry | `DISABLE_TELEMETRY=true`
Service name override | `OTEL_SERVICE_NAME=feed-summarizer-prod`
Environment tag | `OTEL_ENVIRONMENT=production`
Azure exporter | Provide `APPLICATIONINSIGHTS_CONNECTION_STRING` (or legacy instrumentation key)

If no connection string is set, spans stay in-process (no console spam). Logs can also be exported when the Azure exporter is available.
