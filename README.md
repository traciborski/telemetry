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
```

## Wnioski
* Libki nie powinny robic logow, tylko trace'y (a np replicator?)
* OTel Span (in Traces) = AppInsights Dependency OR Request = .NET Activity
* Serilog niepotrzebny
* mozna zrobic SetStatus i RecordException na Trace'ach
* jakie libki / potrzebujemy? jakis helper dla libek?
* tooling / dashboards / guidelines needed
  * no kusto => LogQL + PromQL + TraceQL
* Kafka propaguje `ActivityContext`; Baggage nie jest używane.
* koszty ingestii

## Do przemyślenia przed pokazaniem architektom

### Niezawodność Kafka / outbox
* Brak idempotencji konsumentów (at-least-once + redelivery po dowolnym błędzie w batchu) - Redis `INCR` w ServiceB liczy podwójnie, Elasticsearch w ServiceC dostaje duplikaty (brak jawnego `_id`), ServiceD dostaje powtórne wywołania.
* Brak DLQ / poison message handling - wiadomość, która zawsze failuje, blokuje partycję w nieskończoność (head-of-line blocking).
* Producer ma `Acks.Leader` zamiast `Acks.All` i brak `EnableIdempotence` - słabsze gwarancje trwałości niż mogłoby się wydawać przy outboxie.
* Brak obsługi rebalancingu konsumenta (partition revoked/assigned handlers, cooperative-sticky assignor) - istotne przy wielu replikach w AKS.
* Brak Schema Registry (Redpanda go udostępnia, port 18081, ale nieużywany) - kontrakty JSON bez wersjonowania/walidacji przy N zespołach.
* ~~Outbox: brak metryki na wiek najstarszej nieopublikowanej wiadomości~~ - dodano `outbox.oldest_pending_age` (ObservableGauge, sekundy, tag `db.context`) w `Shared.Outbox/OutboxWorker.cs`. Wciąż brakuje reguły alertowej w Grafanie/Mimirze na tę metrykę.

### Observability
* **Decyzja: bez samplingu, 100% capture** (nie SaaS per-GB jak App Insights, tylko self-hosted stack na Blob Storage - patrz sekcja Backend niżej). Argument za: sampling podważa zaufanie do trace'ów w incydencie (nie wiadomo, czy dla akurat tego requestu trace przetrwał), więc i tak trzeba trzymać kompletne logi jako fallback - a skoro logi muszą być kompletne, sampling trace'ów niewiele daje operacyjnie, tylko komplikuje debugging. Przy taniej cenie blobów (rzędy wielkości niżej niż per-GB billing SaaS) 100% capture jest finansowo do udźwignięcia.
* Samo "skalowanie collectora" to za mało - w tym PoC Tempo/Loki/Mimir działają jako pojedyncze instancje (monolithic mode; `mimir.yaml` wprost ostrzega "Do not use this configuration in production"). Przy pełnym wolumenie ~100 serwisów bez samplingu potrzeba ich dystrybuowanej/skalowalnej konfiguracji (osobno skalowane distributor/ingester/compactor/querier) - wszystkie trzy są do tego zaprojektowane (blob-native od podstaw), ale to nietrywialna zmiana operacyjna względem dzisiejszego single-container setupu.
* Retencja dalej ma sens jako dźwignia - nie kosztowa (bloby są tanie), tylko dla rozmiaru indeksu/czasu kompakcji/wydajności zapytań przy 100% wolumenie. Dziś skonfigurowane 24h (Tempo `compactor.block_retention`, Loki `retention_period`) to placeholder z PoC; Mimir w ogóle nie ma jawnie ustawionej retencji bloków (do zweryfikowania jaki jest efektywny default) - obie rzeczy do świadomego ustawienia przed produkcją.
* Collector bez `memory_limiter` i bez `sending_queue`/`retry_on_failure` na exporterach - ryzyko OOM i utraty danych przy chwilowej niedostępności backendu.
* ~~Brak metryk biznesowych~~ - dodano 1 przykładową (`orders.created`, Counter z tagiem `product`, w ServiceA). Throughput per serwis, lag konsumenta i głębokość outboxa (poza wiekiem najstarszej wiadomości) nadal niepokryte; uwaga na kardynalność tagów przy realnych metrykach (patrz komentarz w `ServiceA/OrdersTelemetry.cs`).
* `/health` zawsze zwraca "healthy" - nie sprawdza SQL/Kafka/Redis/Elasticsearch, za mało dla liveness/readiness w AKS.
* `resource` processor w collectorze hardkoduje `environment: poc` dla wszystkich serwisów - w realu potrzeba `resourcedetection`/atrybutów k8s (pod/node/namespace) i realnego `deployment.environment`.
* Baggage nieużywane - jeśli potrzebne (np. tenant/correlation id), wymaga ręcznej propagacji przez Kafkę analogicznej do tej dla `ActivityContext`.

### Backend / AKS
* **Kierunek: odejście od Application Insights** na rzecz self-hosted stacku z tego PoC (Tempo/Loki/Mimir na Azure Blob Storage) dla wszystkich ~100 serwisów - to świadoma decyzja, nie tylko dev/test sandbox. Do zweryfikowania z architektami: to zamiana modelu kosztowego (SaaS per-GB → infra + operacyjny koszt utrzymania własnego klastra Tempo/Loki/Mimir) i utrata gotowych funkcji App Insights (Application Map, Live Metrics, Smart Detection, istniejące alerty/dashboardy, na których ~100 zespołów już dziś polega) - potrzebny plan migracji/odtworzenia tych funkcji w Grafanie, nie tylko przełącznik exportera w collectorze.
* Brak manifestów K8s/Helm - topologia collectora (agent DaemonSet + gateway czy tylko gateway?), resource requests/limits.
* Sekrety w `docker-compose.yml` są plaintext (hasło SQL) - w AKS powinny iść przez Key Vault + Workload Identity.
* Grafana ma włączony anonymous Admin access - tylko do PoC, nie pokazywać jako wzorzec.
* Brak CI/CD i brak jakichkolwiek testów automatycznych w repo.

### Governance
* Kto utrzymuje `Shared.*` (platform team?), jak są dystrybuowane/wersjonowane (NuGet feed) i jak komunikowane są breaking changes do ~100 zespołów.
* Redakcja PII w logach/trace'ach (np. `redaction` processor w collectorze) jako siatka bezpieczeństwa.
* Kafka propaguje `ActivityContext`; Baggage nie jest używane/potrzebne
* time / memory costs for searching logs and traces
 * search traces by trace id 
 * search logs by indexed fields (defined in default_resource_attributes_as_index_labels or custom)
