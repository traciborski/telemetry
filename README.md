# Telemetry POC

4 serwisy + pełny stos Grafany (Loki/Tempo/Mimir na Azure Blob/Azurite) + Kafka + Redis + Elasticsearch + SQL Server.

## Projects
- **ServiceA** - `POST /orders {product, quantity}` → persists Order + OutboxMessage in SQL Server (one transaction) → background `OutboxPublisherService<AppDbContext>` (from `Shared.Outbox`) polls and publishes `orders.created` to Kafka (transactional outbox pattern).
- **ServiceB** - konsumuje `orders.created`, podbija licznik w Redisie, publikuje `orders.processed`.
- **ServiceC** - konsumuje `orders.processed`, woła ServiceD (z retry), indeksuje w Elasticsearch.
- **ServiceD** - `POST /notifications`, ~1/10 requestów rzuca wyjątek (500), reszta odpytuje SQL Server (`SELECT GETDATE()`).
- **Shared.Telemetry** - konfiguracja OTel (logi/trace/metryki, eksport OTLP) dla wszystkich serwisów.
- **Shared.Messaging** - kontrakty + `KafkaProducer`/`KafkaConsumerBackgroundService<T>` z propagacją trace-context.
- **Shared.Outbox** - transactional outbox

## Stack OTL

| Sygnał  | Backend | Storage | Port UI 
|---------|---------|---------|-------------------
| Traces  | Tempo   | Blobs   | Grafana 
| Logs    | Loki    | Blobs   | Grafana 
| Metrics | Mimir   | Blobs   | Grafana 

## Uruchamianie

```powershell
podman compose up -d --build --force-recreate
```

## Testy
```powershell
1..50 | ForEach-Object { Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8081/orders" -ContentType "application/json" -Body '{"product":"Widget","quantity":3}' }
```

## Topics
* kafka batch handler with tracing
* outbox with tracing 

## Wnioski
* Libki nie powinny robic logow, tylko trace'y (a replicator?)
* OTel Span (in Traces) = AppInsights Dependency OR Request = .NET Activity
* Serilog niepotrzebny
* mozna zrobic SetStatus i RecordException na Trace'ach
* jakie libki / potrzebujemy? jakis helper dla libek?
* tooling / dashboards / guidelines needed
  * no kusto => LogQL + PromQL + TraceQL
* Kafka propaguje `ActivityContext`; Baggage nie jest używane.
