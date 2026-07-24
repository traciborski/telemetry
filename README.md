# Telemetry POC

4 serwisy + pełny stos Grafany (Loki/Tempo/Mimir na Azure Blob/Azurite) + Kafka + Redis + Elasticsearch + SQL Server.

## Flow
- **ServiceA** - `POST /orders {product, quantity}` → persists Order + OutboxMessage in SQL Server (one transaction) → background `OutboxPublisherService<AppDbContext>` (from `Shared.Outbox`) polls and publishes `orders.created` to Kafka (transactional outbox pattern).
- **ServiceB** - konsumuje `orders.created`, podbija licznik w Redisie, publikuje `orders.processed`.
- **ServiceC** - konsumuje `orders.processed`, woła ServiceD (z retry), indeksuje w Elasticsearch, ma buga z mieszaniem tenantow.
- **ServiceD** - `POST /notifications`, ~1/10 requestów rzuca wyjątek (500), reszta odpytuje SQL Server (`SELECT GETDATE()`).
- **Shared.Telemetry** - konfiguracja OTel (logi/trace/metryki, eksport OTLP) dla wszystkich serwisów.
- **Shared.Messaging** - kontrakty + `KafkaProducer`/`KafkaConsumerBackgroundService<T>` z propagacją trace-context.
- **Shared.Outbox** - transactional outbox

## multi tenancy
- **Wymagany header**: kazdy request HTTP musi miec `tenant-id` (poza `/health`). `TenantTelemetryExtensions.UseTenantTelemetry()` czyta go i rzuca wyjatek jak go nie ma - zadnego domyslnego tenanta.
- **Gdzie sie to ustawia**: wartosc headera ląduje jako tag na `Activity` i jako W3C `Baggage` (`tenant.id`), wiec leci dalej do logow (przez `logger.BeginScope`), do trace'ow (tag na spanie) i do dalszej propagacji baggage.
- **HTTP → HTTP**: `AddTenantHeaderPropagation()` na wychodzacym `HttpClient` bierze tenanta z aktualnej `Activity`/baggage i nadpisuje nim `tenant-id` w wychodzacym requescie (ServiceC → ServiceD), nieważne co tam bylo wczesniej.
- **HTTP/worker → Kafka**: `MessagingTelemetry.InjectTraceContext` wstrzykuje trace context + baggage (W3C) w headery Kafki, a osobno dopisuje tenanta do wlasnego headera `tenant-id`.
- **Kafka → consumer/outbox**: `KafkaConsumerWorker` i `OutboxWorker` wyciagaja z wiadomosci `tenant-id` i tenanta z propagowanego baggage, wymagaja zeby oba byly, i naklejaja je z powrotem jako tag/baggage na nowa `Activity` po stronie konsumenta.
- **Guardrail na mismatch**: na kazdym hopie (middleware HTTP, produkcja/konsumpcja Kafki, relay outboxa) tenant z headera jest porownywany z tenantem juz siedzacym w trace/baggage; jak sie nie zgadza, leci `InvalidOperationException` zamiast slepo ufac ktoremus zrodlu - ma to lapac podszywanie sie / przeciek tenanta miedzy requestami.
- W skrocie: `tenant.id` jest zawsze dostepny jako tag na spanie/baggage/pole w logach na calej trasie, i jest to egzekwowane (nie tylko zapisywane) na kazdej granicy serwisu i messagingu.

## Stack OTL

| Sygnał  | Backend | Storage | UI 
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

1..200 | ForEach-Object { Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8081/orders" -Headers @{"tenant-id" = (1..5 | Get-Random)} -ContentType "application/json" -Body (@{ product = "Widget-$(1..3 | Get-Random)"; quantity = (1..10 | Get-Random) } | ConvertTo-Json) }
```

## Podsumowanie
* Libki nie powinny robic logow, tylko trace'y (a np replicator?)
* OTel Span (in Traces) = AppInsights Dependency OR Request = .NET Activity
* Serilog niepotrzebny
* mozna zrobic SetStatus i RecordException na Trace'ach
* tooling / dashboards / guidelines needed
  * no kusto => LogQL + PromQL + TraceQL
* Kafka propaguje `ActivityContext`; Baggage nie jest używane.
* koszty ingestii, bez samplingu chyba mozliwe
* time / memory costs for searching logs and traces
 * search traces by trace id or tags
 * search logs by indexed fields (defined in default_resource_attributes_as_index_labels or custom)
* multi-tenancy?
* czasem tracing mozna wylaczac
* mozna miec customowe metryki