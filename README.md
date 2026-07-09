# Telemetry POC: 4 serwisy + Kafka + pełny stos Grafany (Loki/Tempo/Mimir)

## Flow

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
docker compose up --build
```

Poczekaj ok. 15-20s po starcie, aż Redpanda i otel-collector w pełni wstaną, zanim wyślesz
pierwszy request (Kafka topic tworzy się automatycznie przy pierwszej publikacji).

### Tests
```powershell
Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8081/orders" -ContentType "application/json" -Body '{"product":"Widget","quantity":3}'
```

dostajesz `202 Accepted` z `OrderId`. 
1. **Redpanda Console** (http://localhost:8085) 
2. **Grafana → Explore → Tempo** (http://localhost:3000) → trace po `OrderId` albo po service
   `ServiceA` - zobaczysz jeden trace ze spanami ze wszystkich 4 serwisów (HTTP + Kafka publish/process + HTTP).
3. **Grafana → Explore → Loki** → `{service_name="ServiceA"}` (albo `ServiceB`/`ServiceC`/`ServiceD`) - zobaczysz
   logi `LogInformation` z każdego kroku.
4. **Grafana → Explore → Mimir** → np. metryka `http_server_request_duration_count`.


### Wnioski
Libki nie powinny robic logow, tylko trace'y (moze wyjatek dla exceptionow niektorych?).
OTL trace = Appinsights dependency = dotnet Activity
Serilog niepotrzebny
traces SetStatus i RecordException
jakie libki / potrzebujemy?
   jakis helper dla libek?
tooling
kusto