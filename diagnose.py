#!/usr/bin/env python3
"""
DES Import Diagnostics - sprawdza co jest nie tak z importami.
"""
import os
import sys
from pathlib import Path

def check_structure():
    """Sprawdź strukturę katalogów."""
    print("=" * 70)
    print("1. STRUKTURA KATALOGÓW")
    print("=" * 70)
    
    # Find project root
    current = Path(__file__).parent
    src_dir = current / "src"
    
    if not src_dir.exists():
        src_dir = current.parent / "src"
    
    if not src_dir.exists():
        print("❌ Nie mogę znaleźć katalogu 'src'!")
        print(f"   Szukałem w: {current} i {current.parent}")
        return False
    
    print(f"✓ Znaleziono src: {src_dir}")
    
    # Check des package
    des_dir = src_dir / "des"
    if not des_dir.exists():
        print(f"❌ Brak katalogu: {des_dir}")
        return False
    print(f"✓ Katalog des: {des_dir}")
    
    # Check des/__init__.py
    des_init = des_dir / "__init__.py"
    if not des_init.exists():
        print(f"❌ BRAK PLIKU: {des_init}")
        print("   🔧 FIX: Musisz stworzyć src/des/__init__.py")
        return False
    print(f"✓ Plik des/__init__.py: {des_init}")
    
    # Check des/core
    core_dir = des_dir / "core"
    if not core_dir.exists():
        print(f"❌ Brak katalogu: {core_dir}")
        return False
    print(f"✓ Katalog core: {core_dir}")
    
    # Check des/core/__init__.py
    core_init = core_dir / "__init__.py"
    if not core_init.exists():
        print(f"❌ BRAK PLIKU: {core_init}")
        print("   🔧 FIX: Musisz stworzyć src/des/core/__init__.py")
        return False
    print(f"✓ Plik core/__init__.py: {core_init}")
    
    # List core files
    print("\nPliki w src/des/core/:")
    for f in sorted(core_dir.glob("*.py")):
        size = f.stat().st_size
        print(f"  - {f.name:30s} ({size:>6,} bytes)")
    
    return True


def check_imports():
    """Sprawdź czy importy działają."""
    print("\n" + "=" * 70)
    print("2. TESTY IMPORTÓW")
    print("=" * 70)
    
    # Add src to path
    current = Path(__file__).parent
    src_dir = current / "src"
    if not src_dir.exists():
        src_dir = current.parent / "src"
    
    sys.path.insert(0, str(src_dir))
    
    # Test 1: Import constants
    print("\n[1/7] Import des.core.constants...")
    try:
        from des.core import constants
        print(f"  ✓ OK - VERSION={constants.VERSION}")
    except Exception as e:
        print(f"  ❌ BŁĄD: {e}")
        return False
    
    # Test 2: Import models
    print("\n[2/7] Import des.core.models...")
    try:
        from des.core import models
        print(f"  ✓ OK - IndexEntry={models.IndexEntry}")
    except Exception as e:
        print(f"  ❌ BŁĄD: {e}")
        return False
    
    # Test 3: Import cache
    print("\n[3/7] Import des.core.cache...")
    try:
        from des.core import cache
        print(f"  ✓ OK - InMemoryIndexCache={cache.InMemoryIndexCache}")
    except Exception as e:
        print(f"  ❌ BŁĄD: {e}")
        return False
    
    # Test 4: Import des_writer
    print("\n[4/7] Import des.core.des_writer...")
    try:
        from des.core import des_writer
        print(f"  ✓ OK - DesWriter={des_writer.DesWriter}")
    except Exception as e:
        print(f"  ❌ BŁĄD: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    # Test 5: Import des_reader
    print("\n[5/7] Import des.core.des_reader...")
    try:
        from des.core import des_reader
        print(f"  ✓ OK - DesReader={des_reader.DesReader}")
    except Exception as e:
        print(f"  ❌ BŁĄD: {e}")
        return False
    
    # Test 6: Import s3_des_reader
    print("\n[6/7] Import des.core.s3_des_reader...")
    try:
        from des.core import s3_des_reader
        print(f"  ✓ OK - S3DesReader={s3_des_reader.S3DesReader}")
    except Exception as e:
        print(f"  ❌ BŁĄD: {e}")
        print("  ℹ️  Może brakować boto3: pip install boto3")
        return False
    
    # Test 7: Import from des.core
    print("\n[7/7] Import from des.core (główny)...")
    try:
        from des.core import DesWriter, DesReader, S3DesReader
        print(f"  ✓ OK - DesWriter={DesWriter}")
        print(f"  ✓ OK - DesReader={DesReader}")
        print(f"  ✓ OK - S3DesReader={S3DesReader}")
    except Exception as e:
        print(f"  ❌ BŁĄD: {e}")
        import traceback
        traceback.print_exc()
        return False
    
    return True


def show_fix():
    """Pokaż jak naprawić."""
    print("\n" + "=" * 70)
    print("🔧 JAK NAPRAWIĆ")
    print("=" * 70)
    
    print("""
Twoja struktura MUSI wyglądać tak:

Datavision-Easy-Store/
├── src/
│   └── des/
│       ├── __init__.py          ← MUSI ISTNIEĆ!
│       └── core/
│           ├── __init__.py      ← MUSI ISTNIEĆ!
│           ├── constants.py
│           ├── models.py
│           ├── cache.py
│           ├── des_writer.py
│           ├── des_reader.py
│           └── s3_des_reader.py
├── tests/
│   └── test_core.py
└── examples/
    └── usage_examples.py

KROKI NAPRAWY:

1. Sprawdź czy masz src/des/__init__.py:
   
   Zawartość (minimalna):
   '''
   __version__ = '1.0.0'
   '''

2. Sprawdź czy masz src/des/core/__init__.py:
   
   Zawartość - zobacz plik który Ci wysłałem w archiwum!

3. Uruchom testy z poziomu głównego katalogu:
   
   cd Datavision-Easy-Store
   python tests/test_core.py

4. Jeśli dalej nie działa, dodaj src do PYTHONPATH:
   
   # Windows CMD:
   set PYTHONPATH=%CD%\\src
   
   # Windows PowerShell:
   $env:PYTHONPATH = "$PWD\\src"
   
   # Linux/Mac:
   export PYTHONPATH=$PWD/src

5. Lub dodaj src do sys.path w każdym skrypcie:
   
   import sys
   import os
   sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))
""")


def main():
    print("\n" + "=" * 70)
    print("DES IMPORT DIAGNOSTICS")
    print("=" * 70 + "\n")
    
    # Check structure
    if not check_structure():
        show_fix()
        return 1
    
    # Check imports
    if not check_imports():
        show_fix()
        return 1
    
    print("\n" + "=" * 70)
    print("✅ WSZYSTKO OK - IMPORTY DZIAŁAJĄ!")
    print("=" * 70 + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())