# Datavision Easy Store – ToDo (droga do produkcji)

## Cel dokumentu

Zebrać najważniejsze kroki, które trzeba wykonać, aby DES stał się:

- **profesjonalnym, skalowalnym i bezpiecznym** systemem archiwizacji plików,
- łatwym do integracji dla „Systemu Nadrzędnego”,
- przewidywalnym w utrzymaniu (DevOps/On-Call).

Zadania są pogrupowane tematycznie, nie według sprintów.

---

## 1. Architektura i kontrakt z Systemem Nadrzędnym

1.1. **Model danych w bazie Nadrzędnej**

- Ustalić i udokumentować:
  - pola wymagane przez Marker (ID pliku, ścieżka, data, rozmiar, status),
  - pola uzupełniane przez DES (nazwa DES, shard-id, lokalizacja archiwum, timestamp archiwizacji).
- Zdefiniować **stanową maszynę statusów** (np. `ACTIVE → MARKED → PACKED → DELETED_SOURCE`).

1.2. **Idempotencja i powtarzalność operacji**

- Ustalić zasady:
  - Marker może wielokrotnie próbować oznaczyć ten sam rekord – aktualizacja powinna być idempotentna.
  - Packer przy retrach nie może:
    - podwójnie spakować tego samego pliku,
    - nadpisać shardów bez kontrolowanych wersji.
- Dodać „bezpieczne” kolumny (np. `archive_version`, `last_archive_attempt_at`).

1.3. **Kontrakt API z Systemem Nadrzędnym**

- Zaprojektować i opisać:
  - endpointy odczytu (`GET /files/{name}`, ewentualnie `GET /files/{id}`),
  - format odpowiedzi (nagłówki, statusy, błędy).
- Dostarczyć **OpenAPI (Swagger)** i przykładowe scenariusze integracyjne.

---

## 2. Router + Retrievery (czytanie plików)

2.1. **Lekkie API Routera**

- Zaimplementować usługę:
  - przyjmuje nazwę pliku / ID,
  - wylicza hash + shard-id,
  - mapuje shard-id → adres Retrivera (lista / service discovery),
  - przekazuje żądanie i strumieniuje odpowiedź z powrotem.

2.2. **Retrievery per grupa shardów**

- Zaimplementować usługę Retriver:
  - odpowiedzialną za określony zakres shardów (np. wszystkie, w których high-bits = X),
  - pobierającą metadane z bazy DES,
  - używającą `S3DesReader` (z cache indexu) do odczytu pliku.
- Zapewnić:
  - logiczny podział odpowiedzialności (brak „dwu Retriverów do jednego pliku”),
  - stabilny interfejs API między Routerem a Retriverami (REST / gRPC).

2.3. **Strategia cache indeksów DES**

- Określić:
  - gdzie trzymany jest cache (lokalnie, Redis, Redis-cluster),
  - politykę cache (TTL, max rozmiar, mechanizmy prewarmingu),
  - wpływ na memory footprint przy wielu Retriverach.

---

## 3. Bezpieczeństwo (Security Hardening)

3.1. **Transport**

- Wymusić TLS:
  - pomiędzy klientami a Routerem,
  - pomiędzy Routerem a Retriverami (mTLS w klastrze),
  - pomiędzy usługami a bazami danych, o ile to możliwe.

3.2. **Autentykacja i autoryzacja**

- Zdefiniować model:
  - system nadrzędny jako jedyny klient Routera,
  - opcjonalne role (np. admin/ops do narzędzi diagnostycznych).
- Wybrać mechanizm:
  - JWT (np. od IdP nadrzędnego systemu),
  - lub mTLS + whitelistowanie CIDRów.

3.3. **Uprawnienia do DB i S3**

- Wprowadzić zasady **least privilege**:
  - oddzielne konta DB dla Markera, Packerów, Retriverów,
  - dedykowane role/storage policies na S3 (tylko `GetObject`/`PutObject` na odpowiednie buckety/prefiksy).

3.4. **Audyt i logowanie wrażliwych operacji**

- Logować (strukturalnie, bez danych wrażliwych):
  - kto/który system odczytał plik (ID klienta, nazwa pliku, timestamp, shard),
  - krytyczne operacje (kasowanie oryginałów, re-pack, zmiana konfiguracji).

3.5. **Walidacja wejścia**

- Zabezpieczyć API przed:
  - path traversal (np. nazwy plików z `../`),
  - injection w parametrach (SQL, header),
  - dziwnymi/unicodowymi nazwami plików (ustalić dopuszczalny alfabet).

---

## 4. Skalowalność i niezawodność

4.1. **Autoscaling i limity**

- Ustawić:
  - limity CPU/RAM dla Markera, Packerów, Retriverów (K8s requests/limits),
  - HPA oparte na metrykach (np. liczba rekordów w kolejce, latency).
- Oddzielić:
  - skalowanie „write-path” (Marker + Packer),
  - skalowanie „read-path” (Router + Retrievery).

4.2. **Backpressure i sterowanie tempem**

- Określić:
  - maksymalną liczbę plików do archiwizacji dziennie,
  - mechanizmy spowalniania Markera, gdy:
    - system nadrzędny ma okno serwisowe,
    - storage jest przeładowany.
- Dodać proste API lub config do tymczasowego „throttlingu”.

4.3. **Crash recovery – domknięcie scenariuszy**

- Rozszerzyć `des.packer.recovery`:
  - scenariusze przerwanego uploadu,
  - sprzątanie pół-gotowych shardów,
  - double-check metadanych w DB vs. realne obiekty na S3.
- Dodać analogiczny mechanizm dla Retriverów:
  - timeouty przy odczycie z S3,
  - mechanizm circuit breaker / retry.

4.4. **SLA / SLO**

- Zdefiniować:
  - docelowe SLO latency dla:
    - odczytu pliku (np. P95, P99),
    - opóźnienia archiwizacji (czas od MARKED do PACKED),
  - error budget i zasady reagowania (alerty).

---

## 5. Operacje, observability i DevOps

5.1. **Metryki i logi**

- Ujednolicić metryki:
  - prefiksy (`des_packer_*`, `des_marker_*`, `des_retriever_*`, `des_router_*`),
  - standardowe wskaźniki: request count, latency, errors, queue depth.
- Wprowadzić logowanie strukturalne:
  - JSON logs,
  - trace-id / correlation-id przenoszony przez Router i Retrievery.

5.2. **Dashboardy i alerty**

- Przygotować gotowe:
  - dashboardy Grafana dla:
    - archiwizacji (przepływ dzienny, zepsute rekordy),
    - odczytu (latency, błędy),
  - alerty Prometheus:
    - wzrost `5xx`,
    - pusty / przepełniony queue,
    - brak nowo produkowanych shardów (dead packer).

5.3. **Backup & Disaster Recovery**

- Określić strategię:
  - backup bazy DES,
  - polityka wersjonowania na S3 (versioned bucket),
  - procedury odtworzenia:
    - jak z metadanych i shardów odbudować informacje, gdy padnie baza DES.

5.4. **Runbooki**

- Spisać:
  - „Co zrobić, gdy …”:
    - packer stoi,
    - retriever rzuca timeoutami,
    - niespójność DB Nadrzędnej vs. DES,
  - prosty „first aid kit” dla on-call / operatorów.

---

## 6. Jakość kodu i testy

6.1. **Testy E2E**

- Zbudować pipeline E2E, który:
  - stawia mini-środowisko (Postgres, S3-mock, Marker, Packer, Router, jeden Retriever),
  - symuluje:
    - oznaczenie pliku,
    - spakowanie,
    - odczyt po nazwie i porównanie bytes z oryginałem.

6.2. **Testy wydajnościowe**

- Przygotować scenariusze:
  - masowe archiwizowanie (np. 10M plików, syntetycznie),
  - równoległe odczyty z wielu Retriverów.
- Pomiary:
  - throughput,
  - wpływ cache indeksów DES,
  - zachowanie przy cold start.

6.3. **Testy robustness / fuzzing**

- Fuzzing DES Reader/Writer:
  - zniekształcone nagłówki, uszkodzony footer, brakujące sekcje,
  - celem jest upewnienie się, że błędy są bezpiecznie obsługiwane, a nie powodują crasha.

6.4. **Static analysis i security scanning**

- Włączyć:
  - `ruff`, `mypy` jako obowiązkowe kroki w CI,
  - skanowanie dependency (np. `pip-audit` lub integracja z GitHub Security).

---

## 7. Dokumentacja i doświadczenie deweloperskie

7.1. **Dokumentacja architektoniczna**

- Rozwinąć i utrzymywać:
  - `arch_pl.md` + wersja angielska (`arch_en.md`),
  - diagramy:
    - kontekst (System Nadrzędny ↔ DES),
    - component-level (Marker, Packer, Router, Retrievery, DB, S3),
    - deployment (K8s, namespace, ingressy).

7.2. **Dokumentacja operatora**

- Stworzyć:
  - „Operator Guide” (jak postawić, jak skalować, jak diagnozować),
  - checklistę do wdrożenia na nowym środowisku.

7.3. **DX (Developer Experience)**

- Dopieścić:
  - `docker-compose.yml` tak, by dało się lokalnie przetestować end-to-end,
  - skrypty `make` / `scripts/*` ułatwiające:
    - migrację schematu,
    - start Markera/Packerów/Routera/Retriverów w trybie dev.

---

## 8. Kierunki rozwoju (nice-to-have)

8.1. **Szyfrowanie i prywatność**

- Warstwa szyfrowania per shard lub per plik:
  - integracja z KMS / Vault,
  - klucze per-tenant / per system.

8.2. **Re-pack / kompaktowanie**

- Mechanizm:
  - łączenia małych shardów,
  - czyszczenia bardzo starych danych według polityk retencji.

8.3. **Multi-region / multi-site**

- Rozciągnięcie DES:
  - replikacja shardów między regionami / DC,
  - świadomość lokalizacji w Routerze i Retriverach (wybór najbliższej kopii).

8.4. **Wsparcie dla innych backendów**

- Abstrakcja storage:
  - Ceph RGW, AWS S3, MinIO, Azure Blob, GCS – jako wymienne backendy pod wspólnym interfejsem.

---

## 9. Podsumowanie

As-Is DES jest mocnym **MVP write-pathu** z dopracowanym formatem pliku i pipeline’em pakowania. ToDo koncentruje się na:

- domknięciu **read-path (Router + Retrievery)**,
- **security hardening**,
- **operacyjności i testach E2E**,
- sformalizowaniu kontraktu z Systemem Nadrzędnym.

Po realizacji kluczowych punktów z sekcji 1–5 system będzie gotowy do odpowiedzialnej, produkcyjnej eksploatacji.
