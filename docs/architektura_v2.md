# Algorytmiczny Routing i Shardowanie DES (Bez Wewnętrznej Bazy)

## 1. Wprowadzenie

Celem systemu Datavision Easy Store (DES) jest archiwizacja ogromnej liczby małych plików bez dodatkowych metadanych, bez wewnętrznej bazy DES oraz bez masowych aktualizacji w bazie systemu nadrzędnego.

System korzysta wyłącznie z:

* **UID** – globalnie unikalnego i niezmiennego identyfikatora pliku,
* **created_at** – daty utworzenia,
* **ARCHIVE_CUTOFF_DATE** – globalnej „linii wody”.

Cała lokalizacja pliku w DES opiera się na algorytmie `f(UID, created_at)`.

---

## 2. Architektura bez bazy metadanych DES

* DES **nie ma** własnej bazy.
* System nadrzędny **nie trzyma** statusów archiwizacyjnych.
* Packer, Router i Retriever działają **deterministycznie**, licząc lokalizację pliku z UID.

Wszystkie decyzje logiczne opierają się na:

```
(UID, created_at) + ARCHIVE_CUTOFF_DATE + f(UID)
```

---

## 3. Ogólna zasada działania

### 3.1. Kiedy plik trafia do DES

Packer wybiera:

```
created_at <= ARCHIVE_TARGET_DATE
```

### 3.2. Kiedy plik jest odczytywany z DES

```
if created_at > ARCHIVE_CUTOFF_DATE → czytaj z oryginału
else → spróbuj DES
```

`ARCHIVE_CUTOFF_DATE` aktualizowana jest rzadko (np. raz dziennie).

---

## 4. Algorytm lokalizacji pliku w DES

Wejście: **(UID, created_at)**.

### 4.1. Katalog dzienny

```
date_dir = YYYYMMDD(created_at)
```

### 4.2. Funkcja shardująca f(UID)

Parametry:

* `N_BITS` – liczba bitów identyfikujących shard (np. 8 → 256 shardów).
* `NUM_SHARDS = 2^N_BITS`.

#### Wariant A – UID numeryczny

```
shard_index = UID % NUM_SHARDS
```

#### Wariant B – UID tekstowy

```
h = CRC32(uid_bytes)
shard_index = h & ((1 << N_BITS) - 1)
```

### 4.3. Format shardu

```
shard_hex = hex(shard_index).uppercase().zfill(N_BITS/4)
```

### 4.4. Klucz obiektu w S3

```
object_key = "{date_dir}/{shard_hex}.des"
```

### 4.5. Logiczna nazwa pliku w DES

```
logical_name = UID
```

---

## 5. Packer

### 5.1. Wybór plików

```
SELECT UID, created_at, source_path
FROM files
WHERE created_at <= ARCHIVE_TARGET_DATE
```

### 5.2. Grupowanie i tworzenie shardów

* Grupowanie po `(date_dir, shard_hex)`.
* Każdy plik trafia do `date_dir/shard_hex.des` pod nazwą logiczną `UID`.

### 5.3. Upload

Shard zapisywany do:

```
s3://archive/{date_dir}/{shard_hex}.des
```

---

## 6. Retriever

Wejście: `(UID, created_at)`.

### 6.1. Cutoff

```
if created_at > ARCHIVE_CUTOFF_DATE → oryginał
```

### 6.2. Lokalizacja shardu

```
date_dir = YYYYMMDD(created_at)
shard_hex = f(UID)
object_key = date_dir + "/" + shard_hex + ".des"
```

### 6.3. Odczyt z DES

```
reader = S3DesReader(bucket, object_key)
return reader.get_file(UID)
```

---

## 7. Router

Router rozsyła żądania na podstawie shardu:

```
shard_hex = f(UID)
retriever_id = map(shard_hex)
```

Każdy retriever obsługuje określony zakres shardów.

---

## 8. ARCHIVE_CUTOFF_DATE – mechanizm bez statusów per plik

### 8.1. Jedyne dane w bazie

```
ARCHIVE_CUTOFF_DATE DATE
```

### 8.2. Aktualizacja

Proces nocny:

* weryfikuje kompletność archiwizacji,
* ustawia nową wartość cutoff.

To jedyny UPDATE w systemie.

---

## 9. Zalety rozwiązania

* brak statusów w bazie nadrzędnej,
* brak własnej bazy DES,
* w pełni deterministyczny routing,
* dowolna skalowalność,
* minimalne koszty operacyjne,
* naturalna odporność na re-pack.

---

## 10. TODO – zadania implementacyjne

### 10.1. Core

* [ ] tryb logicznej nazwy pliku = UID,
* [ ] indeksowanie po UID,
* [ ] obsługa dużych shardów.

### 10.2. Shardowanie UID

* [ ] parametr `N_BITS`,
* [ ] implementacja CRC32 dla UID tekstowych,
* [ ] testy równomierności.

### 10.3. Packer

* [ ] pełna rezygnacja z DES_NAME,
* [ ] grupowanie po `(date_dir, shard_hex)`.

### 10.4. Retriever

* [ ] odczyt `(UID, created_at)` → shard,
* [ ] fallback na oryginał.

### 10.5. Router

* [ ] mapowanie shardów na retrievery.

### 10.6. Cutoff Controller

* [ ] aktualizacja ARCHIVE_CUTOFF_DATE,
* [ ] walidacja kompletności archiwizacji.

### 10.7. Dokumentacja

* [ ] włączenie dokumentu do arch_pl.md,
* [ ] przykłady, diagramy przepływu.

---

Koniec dokumentu.
