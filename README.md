# Telemetry POC

4 serwisy + pełny stos Grafany (Loki/Tempo/Mimir na Azure Blob/Azurite) + Kafka + Redis + Elasticsearch + SQL Server.

## Projects
- **ServiceA** - `POST /orders {product, quantity}` → Kafka `orders.created`.
- **ServiceB** - konsumuje `orders.created`, podbija licznik w Redisie, publikuje `orders.processed`.
- **ServiceC** - konsumuje `orders.processed`, woła ServiceD (z retry), indeksuje w Elasticsearch.
- **ServiceD** - `POST /notifications`, ~1/10 requestów rzuca wyjątek (500), reszta odpytuje SQL Server (`SELECT GETDATE()`).
- **Shared.Telemetry** - konfiguracja OTel (logi/trace/metryki, eksport OTLP) dla wszystkich serwisów.
- **Shared.Messaging** - kontrakty + `KafkaProducer<T>`/`KafkaConsumerBackgroundService<T>` z propagacją trace-context.

## Stack OTL

| Sygnał  | Backend | Storage | Port UI 
|---------|---------|---------|-------------------
| Traces  | Tempo   | Blobs   | Grafana 
| Logs    | Loki    | Blobs   | Grafana 
| Metrics | Mimir   | Blobs   | Grafana 

## Uruchamianie

```powershell
podman compose -f docker-compose.infrastructure.yml up -d
```

```powershell
.\start-services.ps1
```

```powershell
docker compose up --detach --no-deps --build service-a service-b service-c service-d
```

## Testy
```powershell
1..50 | ForEach-Object { Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8081/orders" -ContentType "application/json" -Body '{"product":"Widget","quantity":3}' }
```

## Wnioski
* Libki nie powinny robic logow, tylko trace'y
* OTel Span (in Traces) = AppInsights Dependency OR Request = .NET Activity
* Serilog niepotrzebny
* mozna zrobic SetStatus i RecordException na Trace'ach
* jakie libki / potrzebujemy? jakis helper dla libek?
* tooling / dashboards / guidelines needed
  * no kusto => LogQL + PromQL + TraceQL
* Kafka propaguje tylko `ActivityContext`; Baggage nie jest używane.
