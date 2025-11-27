# Datavision Easy Store – Architektura Systemu

### Podręcznik użytkownika / dokumentacja techniczna

---

## 1. Cel systemu

**Datavision Easy Store (DES)** jest wysokowydajnym systemem do hurtowego przechowywania i odczytu dużej liczby małych plików, opartym na:

* deterministycznym mapowaniu plików do kontenerów DES,
* stateless retrieverach HTTP (FastAPI),
* batchowym procesie pakowania plików,
* skalowalnym modelu shardowania opartym na n‑bitowym prefiksie hasha nazwy pliku,
* przechowywaniu kontenerów w storage obiektowym (S3 / CEPH RGW).

DES umożliwia:

* bardzo szybki odczyt po nazwie pliku,
* równoległe przetwarzanie wielu shardów,
* minimalne zależności runtime'u,
* łatwe skalowanie poziome packerów i retrieverów.

---

## 2. Ogólny model działania

System dzieli się na dwa niezależne światy:

### A. Świat batch (offline)

Odpowiada za przetworzenie plików do formatu DES.
Składa się z:

* **Oznaczacza (Marker Service)** — wykrywa nowe pliki, oblicza shard_id i oznacza je w DB,
* **Packerów** — pobierają z DB pliki do obsługi, pakują do kontenerów DES i wysyłają na S3.

### B. Świat online (HTTP / odczyt)

Zapewnia szybki odczyt plików po nazwie.
Składa się z:

* **Files-API** — stateless mikroserwisy zwracające pliki z DES,
* **Routera HTTP** — decyduje, który Files-API powinien obsłużyć dane żądanie,
* **S3 / CEPH RGW** — przechowuje kontenery DES.

Światy łączy tylko wspólny **Layout** — algorytm określający: *shard → kontener → byte range*.

---

## 3. Layout DES – wspólny kontrakt

### 3.1. Hash nazwy pliku

```
def file_hash(file_name: str) -> bytes:
    return sha256(file_name.encode("utf-8")).digest()
```

### 3.2. Obliczanie shard_id

```
def file_to_shard_id(file_name: str, shard_bits: int) -> int:
    h = file_hash(file_name)
    shard_mask = (1 << shard_bits) - 1
    hash_int = int.from_bytes(h, "big")
    return hash_int & shard_mask
```

### 3.3. Lokalizacja pliku w DES

```
@dataclass
class FileLocation:
    container_key: str
    byte_start: int
    byte_end: int
```

```
def locate_in_des(file_name: str, cfg: LayoutConfig) -> FileLocation:
    ...
```

`LayoutConfig` opisuje:

* shard_bits,
* wrap_bits,
* schemat katalogów,
* wersję layoutu.

---

## 4. Marker Service

Marker **nie skanuje filesystemu ani S3**. To nie jest jego zadanie.

Główny system (zewnętrzna aplikacja biznesowa) **już posiada własną tabelę plików** z pełną metadanyczną informacją:

* oryginalna ścieżka pliku,
* czas utworzenia,
* statusy biznesowe,
* właściciel, typ, itd.

Ten system **nie chce zajmować się archiwizacją starszych plików**, ale pozwala markerowi "grzebać" w jednej kolumnie lub podzbiorze kolumn, aby wspomóc proces archiwizacji.

### Rola markera — wyłącznie oznaczanie

Marker działa jak **kalkulator decyzji archiwizacyjnej**:

* wchodzi do *istniejącej tabeli źródłowej*,
* wyszukuje pliki, które spełniają kryteria archiwizacji (np. *starsze niż 7 dni*),
* dla takich plików uzupełnia pola zarządzane przez DES:

  * **DES_NAME** — deterministyczna nazwa pliku w systemie DES,
  * **DES_HASH_BITS** — shard_id wynikający z hasha nazwy,
  * **DES_STATUS = READY** — sygnał dla packerów, że plik czeka do spakowania.

### Charakterystyka działania:

* Marker **nie** pobiera ani nie pakuje plików.
* Marker **nie** współpracuje z S3.
* Marker **nie** tworzy nowej listy plików — korzysta z istniejącej.
* Marker tylko **oznacza rekordy** w kolumnach przeznaczonych dla DES.
* Po oznaczeniu kończy pracę.

### Częstotliwość działania:

* Marker uruchamia się **raz dziennie o północy**,
* przegląda tabelę z plikami,
* oznacza pliki do archiwizacji,
* po zakończeniu pracy **usypia na 24 godziny**.

### Efekt jego działania:

W tabeli głównego systemu pojawiają się rekordy z:

* DES_NAME,
* DES_HASH_BITS,
* DES_STATUS = READY.

Są to wpisy, które **packery konsumują**.

Cechy:

* korzysta tylko z DB i systemu źródłowego,
* nie współpracuje bezpośrednio z S3 używanym przez packery,
* jest logicznie pierwszy etapem pipeline'u,
* jego wynikiem jest **tabela pending_files** z pełnym opisem tego, co packery mają zrobić.

Cechy:

* działa cyklicznie (sleep w pętli),
* wymaga tylko DB.

---

## 5. Packery

Działanie packera:

1. Wylicza swoje shardy (pod_index → zakres shardów).
2. Pobiera z DB pliki o statusie `READY` z tych shardów.
3. Pobiera oryginalny plik ze źródła.
4. Wylicza `FileLocation`.
5. Pakowanie do kontenera DES.
6. Upload kontenera na S3.
7. Aktualizacja statusu w DB.

Cechy:

* pełna równoległość,
* odporność na restart,
* kontrolowane przez DB.

---

## 6. Files-API – stateless odczyt

Wejście:

```
GET /files/{file_name}
```

Działanie:

1. `loc = locate_in_des(file_name, cfg)`
2. Pobranie danych z S3 z nagłówkiem `Range`
3. Zwrot pliku jako stream lub Base64

Cechy:

* bezstanowe,
* brak DB,
* opcjonalny cache lokalny lub Redis.

---

## 7. Router HTTP – decydowanie po hashu

Router pełni rolę bramy do wielu instancji Files-API.

1. Oblicza `shard_id = file_to_shard_id(...)`.
2. Na podstawie zakresów shardów wybiera backend.
3. Proxy:

```
GET http://files-api-N/files/{file_name}
```

Router może działać w Dockerze lub Kubernetes.

---

## 8. Zależności

### Marker

* DB: wymagane
* S3: nie
* Layout: tak

### Packer

* DB: wymagane
* S3 (zapis): wymagane
* Layout: wymagane

### Files-API

* DB: nie
* S3 (odczyt): wymagane
* Layout: wymagane

### Router

* DB: nie
* Files-API: wymagane
* Layout: wymagane

---

## 9. Najważniejsze właściwości

* pełna skalowalność pozioma,
* stateless odczyt,
* batch i online oddzielone,
* DB tylko jako kolejka i metadane,
* Layout jako jedyne źródło prawdy,
* spójność między packerem i retrieverem.

---

## 10. Schemat wysokopoziomowy

```
             [ Źródła plików ]
                    |
           [ Marker Service ]
                    |
                   DB
                pending_files
                    |
                    v
       +---------------------------+
       |         PACKERY           |
       +---------------------------+
               |          |
               |  DES containers
               v          |
         [  S3 / CEPH RGW  ]  ← ← ←  [ Stateless Files-API ]
                                    (read via Range)
                                         ↑
                                         |
                                   [ Router ]
                                         ↑
                                         |
                                   [ Użytkownik ]
```

---

## 11. Zasady utrzymania

1. Wersjonować Layout jeśli zajdzie zmiana.
2. Packery i Files-API muszą używać tej samej wersji layoutu.
3. DB nie może być używana do odczytu pliku.
4. Router odpowiada wyłącznie za routing, nie za logikę.
5. Files-API pozostaje w pełni bezstanowe.

---

## 12. Podsumowanie

DES to modularny, skalowalny i bardzo szybki system do przechowywania dużych wolumenów małych plików, oparty na:

* deterministycznym layoutcie,
* oddzieleniu batch od online,
* packerach działających asynchronicznie,
* stateless retrieverach,
* minimalnych wymaganiach runtime.

To architektura gotowa do wdrożeń zarówno w Dockerze, jak i w Kubernetesie, nastawiona na wysoką dostępność oraz łatwe skalowanie.
