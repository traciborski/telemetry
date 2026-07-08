# Telemetry POC: 4 serwisy + Kafka + pełny stos Grafany (Loki/Tempo/Mimir)

Proof-of-concept pokazujący end-to-end distributed tracing w .NET przez HTTP **i** Kafkę,
z logami, trace'ami i metrykami zbieranymi przez OpenTelemetry.

To jest kod POC - nie jest przygotowany pod produkcję (brak retry/DLQ na Kafce, brak auth,
hasła w plaintext w compose, storage lokalny na dysku itd. - celowo, dla prostoty).

## Architektura

```
HTTP POST /orders          Kafka                    Kafka                HTTP POST /notifications
   ───────────►  ServiceA ────────► ServiceB ────────► ServiceC ────────►  ServiceD
                (produces)  orders.created   (produces)  orders.processed
                                                                    (consumes, calls ServiceD)
```

- **ServiceA** - `POST /orders {product, quantity}` → publikuje `OrderCreatedMessage` na topic `orders.created`.
- **ServiceB** - konsumuje `orders.created`, "przetwarza" (symulowane opóźnienie), publikuje `OrderProcessedMessage` na `orders.processed`.
- **ServiceC** - konsumuje `orders.processed`, woła `POST /notifications` na ServiceD.
- **ServiceD** - przyjmuje `POST /notifications`, zwraca potwierdzenie.

Cały łańcuch to **jeden spójny trace** w Tempo: `HttpClient`/ASP.NET Core mają propagację
`traceparent` "za darmo" (wbudowana instrumentacja), a Kafka - która nie ma automatycznej
instrumentacji w .NET - dostaje ją ręcznie w `Shared.Messaging/KafkaTelemetry.cs`
(trace-context wstrzykiwany/wyciągany z nagłówków wiadomości Kafki przez
`Propagators.DefaultTextMapPropagator`).

Każdy serwis loguje minimum jeden `LogInformation` na request HTTP i na przetworzenie
wiadomości z Kafki - te logi trafiają przez OTLP do Loki.

## Współdzielone biblioteki

- **Shared.Telemetry** - jedno miejsce konfigurujące OpenTelemetry (logi/trace/metryki, eksport OTLP
  do otel-collectora) dla wszystkich 4 serwisów.
- **Shared.Messaging** - kontrakty wiadomości, `KafkaProducer<T>`/`KafkaConsumerBackgroundService<T>`
  (Confluent.Kafka) z wbudowaną propagacją trace-context.

## Stos obserwowalności

| Sygnał   | Backend | Jak trafia |
|----------|---------|------------|
| Trace'y  | Grafana Tempo | serwisy → OTLP (gRPC 4317) → otel-collector → OTLP → Tempo |
| Logi     | Grafana Loki | serwisy → OTLP → otel-collector → natywny endpoint OTLP Loki (`/otlp`) |
| Metryki  | Grafana Mimir | serwisy → OTLP → otel-collector → natywny endpoint OTLP Mimir (`/otlp`) |

Wszystko na wolumenach dockerowych (lokalny dysk) - zgodnie z ustaleniami, bez Azure Blob/Azurite.

Grafana ma już wpięte wszystkie 3 źródła danych (auto-provisioning) + link trace→logi
(kliknięcie w span w Tempo pokazuje powiązane logi z Loki).

Kafka to **Redpanda** (jeden kontener, kompatybilny z klientem Kafka .NET) + **Redpanda Console**
do podglądu topiców/wiadomości.

## Uruchomienie

```powershell
cd services
docker compose up --build
```

Poczekaj ok. 15-20s po starcie, aż Redpanda i otel-collector w pełni wstaną, zanim wyślesz
pierwszy request (Kafka topic tworzy się automatycznie przy pierwszej publikacji).

### Porty

| Serwis | Port hosta | Uwaga |
|---|---|---|
| ServiceA | 8081 | `POST /orders` |
| ServiceB | 8082 | tylko `/health` (konsument+producent Kafki w tle) |
| ServiceC | 8083 | tylko `/health` (konsument Kafki w tle, woła ServiceD) |
| ServiceD | 8084 | `POST /notifications` |
| Grafana | 3000 | login `admin`/`admin` |
| Redpanda Console | 8085 | podgląd topiców/wiadomości |
| Tempo | 3200 | query API |
| Loki | 3100 | query API |
| Mimir | 9009 | `/prometheus` - Prometheus-compatible query API |
| Redpanda Kafka API | 19092 | z hosta (wewnątrz sieci dockerowej: `redpanda:9092`) |

> **Uwaga:** porty 3000, 3100, 4317, 4318 pokrywają się z Twoim istniejącym root
> `docker-compose.yml` (SampleApi + Jaeger/Prometheus). Nie odpalaj obu stacków naraz.

### Test end-to-end

```powershell
curl -Method Post -Uri http://localhost:8081/orders `
  -ContentType "application/json" `
  -Body '{"product":"Widget","quantity":3}'
```

Powinieneś dostać `202 Accepted` z `OrderId`. Następnie:

1. **Redpanda Console** (http://localhost:8085) → Topics → zobacz wiadomość w `orders.created` i `orders.processed`.
2. **Grafana → Explore → Tempo** (http://localhost:3000) → wyszukaj trace po `OrderId` albo po service
   `ServiceA` - zobaczysz jeden trace ze spanami ze wszystkich 4 serwisów (HTTP + Kafka publish/process + HTTP).
3. **Grafana → Explore → Loki** → `{service_name="service-a"}` (albo b/c/d) - zobaczysz logi `LogInformation`
   z każdego kroku.
4. **Grafana → Explore → Mimir** → np. metryka `http_server_request_duration_seconds_count`.

## Struktura katalogów

```
services/
  Services.slnx
  docker-compose.yml
  ServiceA/ ServiceB/ ServiceC/ ServiceD/    - 4 minimalne ASP.NET Core (net10.0)
  Shared.Telemetry/                          - konfiguracja OpenTelemetry
  Shared.Messaging/                          - Kafka producer/consumer + kontrakty + trace propagation
  observability/                             - configi Tempo, Mimir, Loki, otel-collector, Grafana datasources
```
