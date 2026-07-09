# Telemetry POC: 4 serwisy + Kafka + pełny stos Grafany (Loki/Tempo/Mimir)

## Flow

HTTP POST /orders          Kafka                    Kafka                HTTP POST /notifications
   ───────────►  ServiceA ────────► ServiceB ────────► ServiceC ────────►  ServiceD
                (produces)  orders.created   (produces)  orders.processed
                                                                    (consumes, calls ServiceD)
```

- **ServiceA** - `POST /orders {product, quantity}` → publikuje `OrderCreatedMessage` na topic `orders.created`.
- **ServiceB** - konsumuje `orders.created`, "przetwarza" (symulowane opóźnienie), podbija licznik
  `orders:processed:count` w Redisie, publikuje `OrderProcessedMessage` na `orders.processed`.
- **ServiceC** - konsumuje `orders.processed`, woła `POST /notifications` na ServiceD, indeksuje
  zamówienie w Elasticsearch (`orders-processed`).
- **ServiceD** - przyjmuje `POST /notifications`, dopytuje SQL Server (EF Core, `SELECT GETDATE()`)
  o czas serwera, zwraca potwierdzenie.

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

Dane Tempo/Loki/Mimir (bloki, chunki, indeks, trace'y - czyli wszystko poza lokalnym
WAL-em/cache) trzymane są w **Azure Blob Storage**, lokalnie emulowanym przez
**Azurite** - tak, żeby setup jak najbardziej odzwierciedlał docelowy deployment na
Azure (gdzie te same trzy backendy piszą do prawdziwego Storage Account). Wolumeny
dockerowe (`tempo-data`, `loki-data`, `mimir-data`) służą już tylko jako lokalny
scratch/WAL - żadne z nich nie trzyma docelowych danych.

| Kontener Azurite | Zawartość |
|-------------------|-----------|
| `tempo-traces` | bloki trace'ów z Tempo |
| `loki-chunks`  | chunki + indeks (TSDB) z Loki |
| `mimir-blocks` | bloki metryk (TSDB) z Mimir |
| `mimir-ruler`  | reguły alertów/recording rules Mimira (nieużywane w tym POC, ale wymagają skonfigurowanego backendu) |

Grafana ma już wpięte wszystkie 3 źródła danych (auto-provisioning) + link trace→logi
(kliknięcie w span w Tempo pokazuje powiązane logi z Loki).

Kafka to **Redpanda** (jeden kontener, kompatybilny z klientem Kafka .NET) + **Redpanda Console**
do podglądu topiców/wiadomości.

## Azure Blob Storage (lokalnie) i podgląd blobów

**Azurite** (`mcr.microsoft.com/azure-storage/azurite`) to oficjalny emulator Azure
Storage od Microsoftu - udostępnia lokalnie API Blob/Queue/Table identyczne z
prawdziwym Azure, więc Tempo/Loki/Mimir łączą się z nim dokładnie tak samo jak
łączyłyby się z prawdziwym Storage Accountem (ta sama konfiguracja `azure:` w ich
plikach YAML, zmienia się tylko connection string/endpoint).

Przy starcie stosu jednorazowy kontener `azurite-init` tworzy w Azurite wymagane
kontenery blobowe (Azurite, tak jak prawdziwy Azure, nie tworzy ich automatycznie).

Do podglądu zawartości blobów służy **[Azurite UI](https://github.com/adrianhall/azurite-ui)**
- lekka, webowa konsola do przeglądania kontenerów i blobów Azurite (odpowiednik
Azure Storage Explorer, ale bez instalowania czegokolwiek na hoście). Budowana jest
lokalnie wprost z repo Git (ma `Dockerfile` w roocie), a nie pobierana jako gotowy
obraz z `ghcr.io` - dzięki temu nie zależy od uwierzytelniania do zewnętrznego
rejestru kontenerów, tylko od zwykłego `git clone` + `docker/podman build`.

- **Azurite UI** → http://localhost:8086 - przeglądanie kontenerów/blobów (m.in.
  `tempo-traces`, `loki-chunks`, `mimir-blocks`).
- Port hosta **10000** (blob) można też podpiąć bezpośrednio z Azure Storage
  Explorer albo `az storage` CLI, connection string:
  `DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;`
  (to publiczne, dobrze znane dane deweloperskie emulatora, nie prawdziwe
  poświadczenia Azure).

## Redis (tylko ServiceB)

**ServiceB** dodatkowo łączy się z **Redisem** (`redis:7-alpine`) i przy każdym
przetworzonym zamówieniu podbija prosty licznik `orders:processed:count`
(`StringIncrementAsync`) - efekt widać w logach ServiceB (`Redis counter ... is now ...`)
i bezpośrednio w Redisie.

Redis jest instrumentowany OpenTelemetry (`OpenTelemetry.Instrumentation.StackExchangeRedis`)
**wyłącznie w ServiceB** - to jedyny serwis, który go używa. Pozostałe 3 serwisy nie mają
ani pakietu Redis, ani jego instrumentacji; `Shared.Telemetry` (wspólna konfiguracja OTel)
też nie zna Redisa - instrumentacja jest dokładana lokalnie w `ServiceB/Program.cs`
(`builder.Services.AddOpenTelemetry().WithTracing(t => t.AddRedisInstrumentation(redisConnection))`),
dzięki czemu spany zapytań do Redisa trafiają do tego samego trace'u co reszta łańcucha
(widoczne w Tempo obok spanów HTTP/Kafka dla ServiceB).

Do podglądu zawartości Redisa służy **[Redis Commander](https://github.com/joeferner/redis-commander)**
- lekka, webowa konsola do przeglądania kluczy/wartości (bez instalowania czegokolwiek
na hoście):

- **Redis Commander** → http://localhost:8087 - podgląd klucza `orders:processed:count`
  i jego wartości na żywo.
- Port hosta **6379** można też podpiąć bezpośrednio z `redis-cli`/RedisInsight:
  `redis-cli -h 127.0.0.1 -p 6379 get orders:processed:count`.

## Elasticsearch (tylko ServiceC)

**ServiceC** dodatkowo indeksuje każde przetworzone zamówienie jako dokument w
**Elasticsearch** (`docker.elastic.co/elasticsearch/elasticsearch`, jeden węzeł,
bez security - to dev/POC) w indeksie `orders-processed`. Bez dedykowanego UI
(świadomie - do sprawdzenia wystarczy REST API).

Instrumentacja OTel działa **wyłącznie w ServiceC**: oficjalny klient
`Elastic.Clients.Elasticsearch` sam publikuje trace'y przez własny `ActivitySource`
(`"Elastic.Transport"`), więc wystarczy dopiąć go lokalnie w `ServiceC/Program.cs`
(`builder.Services.AddOpenTelemetry().WithTracing(t => t.AddSource("Elastic.Transport"))`)
- tak samo jak w przypadku Redisa w ServiceB, `Shared.Telemetry` nic o
Elasticsearch nie wie, a pozostałe serwisy nie mają ani pakietu, ani instrumentacji.

- Port hosta **9200** - REST API do szybkiego sprawdzenia:
  `curl http://localhost:9200/orders-processed/_search?pretty`.

## SQL Server / EF Core (tylko ServiceD)

**ServiceD** dodatkowo łączy się z **SQL Serverem** (`mcr.microsoft.com/mssql/server`) przez
**EF Core**, ale celowo bez żadnych encji/`DbSet`/migracji - `AppDbContext` jest pusty,
a jedyne co robi to `db.Database.SqlQueryRaw<DateTime>("SELECT GETDATE() AS Value")`
przy każdym `POST /notifications`, żeby pokazać integrację EF Core + OTel na absolutnym minimum.

Instrumentacja OTel (`OpenTelemetry.Instrumentation.EntityFrameworkCore`) działa **wyłącznie
w ServiceD** - tak samo jak Redis w ServiceB i Elasticsearch w ServiceC: dokładana lokalnie
w `ServiceD/Program.cs` (`builder.Services.AddOpenTelemetry().WithTracing(t => t.AddEntityFrameworkCoreInstrumentation())`),
`Shared.Telemetry` i pozostałe serwisy nic o SQL Serverze nie wiedzą.

- Port hosta **1433** - `sa` / `YourStrong!Passw0rd` (SQL Server Management Studio, Azure Data
  Studio albo `sqlcmd`). Bez dedykowanego UI w tym repo.

## Uruchomienie

```powershell
docker compose up --build
```

Poczekaj ok. 15-20s po starcie, aż Redpanda i otel-collector w pełni wstaną, zanim wyślesz
pierwszy request (Kafka topic tworzy się automatycznie przy pierwszej publikacji).
Tempo/Loki/Mimir startują dopiero po tym, jak `azurite-init` utworzy kontenery
blobowe w Azurite (`depends_on: service_completed_successfully`), więc pierwszy
start stosu może potrwać chwilę dłużej.

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
5. **Azurite UI** (http://localhost:8086) - zobaczysz kontenery `tempo-traces`, `loki-chunks`,
   `mimir-blocks` z realnymi blobami (po chwili od wysłania requestu i ewentualnym flushu/kompakcji).
6. **Redis Commander** (http://localhost:8087) - klucz `orders:processed:count` powinien
   rosnąć o 1 po każdym przetworzonym zamówieniu (widoczne też w spanie Redisa w Tempo
   i w logu ServiceB).
7. **Elasticsearch** - `curl http://localhost:9200/orders-processed/_search?pretty` powinno
   zwrócić zaindeksowany dokument zamówienia (widoczne też w spanie `Elastic.Transport`
   w Tempo i w logu ServiceC).
8. **SQL Server / EF Core** - odpowiedź `POST /orders` (a właściwie log/trace ServiceD) powinna
   zawierać `SqlServerTime` z realnym czasem z SQL Servera (widoczne też w spanie EF Core
   w Tempo i w logu ServiceD: `SQL Server time is ...`).


### Wnioski
Libki nie powinny robic logow, tylko trace'y (moze wyjatek dla exceptionow niektorych?).
OTL trace = Appinsights dependency = dotnet Activity
Serilog niepotrzebny
traces SetStatus i RecordException
jakie libki / potrzebujemy?
   jakis helper dla libek?
tooling
kusto