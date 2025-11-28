# Datavision Easy Store – Architektura (PL, stan As-Is)

## 1. Cel systemu

Datavision Easy Store (DES) służy do:

- pakowania bardzo wielu małych plików w duże, sekwencyjne obiekty (shardy) w S3/HCP,
- redukcji liczby obiektów w storage przy zachowaniu szybkiego, losowego dostępu do pojedynczych plików,
- integracji z istniejącym „Systemem Nadrzędnym”, który:
  - ma własną bazę danych i własne miejsce składowania plików,
  - **nie** chce samodzielnie zarządzać archiwizacją staroci,
  - po archiwizacji odwołuje się do plików przez nowe API zamiast do oryginalnej lokalizacji.

System jest obecnie w fazie **technicznego MVP / alpha**:
- format DES, pakowanie, integracja z bazami źródłowymi i S3 są już zaimplementowane,
- część usług (np. routing do retrieverów, pełne API „czytające”) istnieje w formie szkiców / stubów,
- brakuje jeszcze pełnego „production hardening” (security, HA, operacje).

---

## 2. Kontekst i integracje

### 2.1. System Nadrzędny

- Utrzymuje swoją **bazę danych** z tabelą katalogową plików (ID, ścieżka, status, metadane).
- Utrzymuje **oryginalną lokalizację pliku** (filesystem / S3 / inny storage).
- Współpracuje z DES przez:
  - udostępnienie jednej tabeli / widoku, w którym DES może:
    - odczytywać listę plików kwalifikujących się do archiwizacji,
    - oznaczać rekordy jako „zajęte” i „spakowane”,
  - korzystanie z **API odczytu DES** zamiast bezpośredniego dostępu do oryginalnych plików (po archiwizacji).

### 2.2. Storage archiwalny

- Shardy DES są przechowywane w **S3/HCP-kompatybilnym** storage (np. CEPH RGW).
- Dostęp do danych odbywa się głównie przez **Range GET** (HTTP 1.1 z nagłówkiem `Range`).
- DES obsługuje:
  - lokalny odczyt shardów (DesReader),
  - odczyt z S3 (S3DesReader),
  - cache indeksu w pamięci / Redis.

---

## 3. Moduły systemu

### 3.1. Rdzeń: format DES i biblioteka

Pakiet `des.core`:

- Definiuje format **DES v1**: `HEADER + DATA + META + INDEX + FOOTER`.
- `DesWriter` – append-only writer:
  - przyjmuje pliki (nazwa, bytes, meta),
  - zapisuje dane binarne, metadane i indeks w jednym pliku DES,
  - opcjonalnie wynosi duże pliki do `_bigFiles/`.
- `DesReader` / `S3DesReader`:
  - umożliwiają listowanie plików, pobieranie metadanych i danych,
  - korzystają z pluggable cache (np. `InMemoryIndexCache`).

Ten moduł jest w praktyce **najbardziej dojrzały** – ma testy, jest użyteczny jako samodzielna biblioteka.

---

### 3.2. Moduł nadawania nazw i sharding

Pakiet `des.utils` / `des.assignment`:

- Generator nazw **Snowflake-like**:
  - unikalne nazwy plików, zawierające m.in. timestamp, node_id i bity shardu.
- Funkcje do wyliczania **ID shardu** z hasha nazwy pliku:
  - `DES_SHARD_BITS` definiuje liczbę bitów użytych na sharding,
  - nazwy plików są deterministycznie przypisywane do shardów.

Stan:
- Logika generowania nazw i shardowania jest zaimplementowana i używana przez Marker oraz Packer.
- Konfiguracja bitów shardingowych i nazwy prefiksu jest parametryzowana zmiennymi środowiskowymi.

---

### 3.3. Baza metadanych DES

Pakiet `des.db`:

- Asynchroniczny connector do **PostgreSQL**:
  - przechowuje:
    - locki i statusy shardów,
    - metadane kontenerów DES (bucket, key, rozmiar, status),
    - stan procesów pakowania (checkpointy).
- Mechanizm blokad:
  - `FOR UPDATE SKIP LOCKED` – umożliwia równoległą pracę wielu pakerów bez konfliktów.

Stan:
- Schemat i logika locków są zaimplementowane i używane przez packer pipeline.
- To jest fundament spójności i koordynacji w systemie.

---

### 3.4. Marker – oznaczanie plików w bazie nadrzędnej

Pakiet `des.marker.file_marker` (Marker Worker):

- Okresowo:
  - czyta z tabeli systemu nadrzędnego pliki spełniające kryteria (np. „starsze niż X dni”, status = „aktywne”),
  - dla każdego pliku:
    - generuje nazwę DES (Snowflake),
    - wylicza hash i bity shardu,
    - aktualizuje rekord:
      - status „do spakowania” / „oznaczony”,
      - przypisany shard,
      - docelowa nazwa pliku w DES.
- Cechy:
  - batch processing (`DES_MARKER_BATCH_SIZE`),
  - ograniczenie wieku (`DES_MARKER_MAX_AGE_DAYS`),
  - prosta obsługa błędów (retries, opcjonalny DLQ),
  - ekspozycja metryk Prometheus i health-prob.

Stan:
- Marker jest **gotowy do użycia** w typowym scenariuszu (jeden lub wiele workerów).
- Wymaga doprecyzowania kontraktu z bazą nadrzędną (sztywne nazwy kolumn/statusów vs. konfiguracja).

---

### 3.5. Packer – pakowanie zaznaczonych rekordów

Pakiet `des.packer` (Multi-shard Packer):

- Odpowiada za **budowę shardów DES** na podstawie rekordów oznaczonych przez Marker:
  - rezerwuje shard (lock w DB DES),
  - pobiera z bazy źródłowej listę plików do spakowania dla danego shardu,
  - czyta dane z oryginalnej lokalizacji (np. S3/bucket „source”),
  - buduje plik DES (`DesWriter`) lokalnie w workdir (`DES_PACKER_WORKDIR`),
  - wrzuca gotowy shard do bucketu archiwalnego (`DES_ARCHIVE_BUCKET`),
  - aktualizuje metadane:
    - w DB DES – status i lokalizacja shardu,
    - w bazie nadrzędnej – status pliku = „spakowany” + informacja o archiwum.

- Wspierane scenariusze:
  - multi-source (konfiguracja w `configs/source_databases.yaml`),
  - rozłożenie prac na wiele pakerów (K8s, shard-range na pod).

Stan:
- Pipeline packera jest **zaimplementowany i testowany** (przynajmniej do poziomu testów jednostkowych / integracyjnych).
- Obsługa crash recovery jest dostępna (pakiet `des.packer.recovery`), ale wymaga jeszcze dopracowania scenariuszy brzegowych.

---

### 3.6. API i moduły „czytające” (retrievery)

Obecnie:

- Istnieje **FastAPI** dla nadawania nazw (`des.assignment.service`).
- Istnieje szkic/draft API (`scripts/run_api.py`, `des.api.server`), który demonstruje:
  - integrację z `S3DesReader`,
  - podstawowe endpointy.

Docelowa koncepcja (częściowo jeszcze **niezaimplementowana**):

- **Lekkie API Routera**:
  - przyjmuje nazwę pliku (ID) od Systemu Nadrzędnego,
  - wylicza hash i bity shardu,
  - na tej podstawie kieruje żądanie do właściwego **Retrivera** (np. po HTTP albo przez message bus),
  - gwarantuje, że **dokładnie jeden** retriever obsłuży dany plik.

- **Retriver (per-grupa shardów)**:
  - działa na własnej grupie shardów (np. wycinek przestrzeni hashy),
  - korzysta z bazy DES i/lub metadanych, aby:
    - znaleźć, w którym shardzie znajduje się plik,
    - otworzyć shard przez `S3DesReader` i wyjąć konkretne bytes,
  - zwraca plik do Routera (lub bezpośrednio do klienta, zależnie od architektury).

Stan:
- Bibliotecznie wszystko, czego retriever potrzebuje (S3DesReader, cache, DES format), już istnieje.
- Brakuje docelowej implementacji:
  - routera shardingowego jako osobnej usługi,
  - retrieverów jako w pełni opisanych i osobno skalowalnych mikroserwisów,
  - stabilnego kontraktu API (OpenAPI) dla odczytu.

---

### 3.7. Monitoring, health-checki, operacje

- Prometheus:
  - metryki dla Markera i Packerów (liczniki, czasy, błędy).
- Health-checki:
  - podstawowe endpointy liveness/readiness.
- K8s:
  - istnieją manifesty dla Markera i Packerów,
  - wstępna integracja z HPA (auto-scaling) dla Markera.

Stan:
- Monitoring i health-checki są zrobione **na przyzwoitym poziomie MVP**,
- brak jeszcze:
  - pełnych dashboardów (Grafana tylko częściowo),
  - spójnej strategii logowania strukturalnego i korelacji requestów (trace-id).

---

## 4. Główne przepływy

### 4.1. Oznaczanie plików do archiwizacji

1. Marker łączy się z bazą Systemu Nadrzędnego.
2. Szuka rekordów spełniających warunki (wiek, status).
3. Generuje nazwy DES + shard-id, aktualizuje rekordy.
4. Te rekordy stają się „kolejką” dla Packerów.

### 4.2. Pakowanie i aktualizacja baz

1. Packer wybiera shard do obsługi (lock w DB DES).
2. Czyta rekordy oznaczone na dany shard z bazy Nadrzędnej.
3. Pobiera pliki z oryginalnego storage.
4. Tworzy lokalny plik DES, uploaduje do S3.
5. Aktualizuje:
   - bazę DES (shard gotowy),
   - bazę Nadrzędną (status = „spakowany”, informacje o archiwum).

### 4.3. Odczyt pliku po nazwie

Docelowo:

1. System Nadrzędny woła lekkie API: `GET /files/{name}`.
2. Router wylicza shard na podstawie hash nazwy.
3. Router przekazuje żądanie do właściwego Retrivera.
4. Retriver:
   - znajduje shard,
   - używa S3DesReader do odczytu pliku,
   - zwraca bytes do Routera.
5. Router podaje dane klientowi końcowemu.

W praktyce (As-Is):
- istnieje biblioteczna możliwość odczytu pliku z DES,
- API odczytujące w docelowym kształcie jest jeszcze w fazie **szkicu**.

---

## 5. Ocena dojrzałości

**Mocne strony As-Is:**

- Solidny, przetestowany format pliku DES oraz biblioteka odczytu/zapisu.
- Dobrze pomyślana obsługa S3 z Range GET i cache indeksu.
- Przemyślany model locków/shardów w bazie DES (koordynacja wielu pakerów).
- Działający Marker i Packer z obsługą wielu źródeł, docker/k8s, monitoringiem.

**Braki i ograniczenia As-Is (wysoki priorytet na przyszłość):**

- Brak produkcyjnie „domkniętego” API odczytu (Router + retrievery).
- Brak formalnego kontraktu z Systemem Nadrzędnym:
  - statusy, nazwy kolumn, idempotentne aktualizacje, wersjonowanie schematu.
- Bezpieczeństwo i dostęp:
  - brak spójnej historii authn/authz, TLS, rate limitów,
  - brak audytu kto/co czytał.
- Operacje:
  - brak pełnych runbooków, alertów, scenariuszy DR/backup.
- Brakuje pełnej dokumentacji architektonicznej dla operatorów oraz dla zespołów integrujących.

System jest więc na etapie:
- **Stabilne MVP „write-path” (mark & pack)**,
- **Prototypowy „read-path”**,
- gotowy, by wejść w fazę „production hardening” i scalania API odczytowego.

