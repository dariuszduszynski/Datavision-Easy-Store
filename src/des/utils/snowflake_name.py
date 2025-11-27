# snowflake_name.py
import threading
import time
from dataclasses import dataclass
from datetime import date


@dataclass
class SnowflakeNameConfig:
    node_id: int = 0  # 0–255
    prefix: str = "DES"  # zamiast "UserCustom"
    wrap_bits: int = 32  # ile mniej znaczących bitów epoch_ms bierzemy


class SnowflakeNameGenerator:
    """
    Generator nazw w formacie:
        <prefix>_YYYYMMDD_(FFFFFFFFFFFF_CC)

    F = 48-bit:
        [ t_low (wrap_bits, max 32) ][ node_id (8 b) ][ seq (8 b) ]

    t_low = lower wrap_bits of epoch_ms (ms od unix epoch)
    CC = checksum 1 bajt (suma bajtów F % 256)
    """

    def __init__(self, config: SnowflakeNameConfig | None = None):
        self.config = config or SnowflakeNameConfig()
        if not (0 <= self.config.node_id <= 0xFF):
            raise ValueError("node_id must be in [0, 255]")
        if not (1 <= self.config.wrap_bits <= 32):
            raise ValueError("wrap_bits must be in [1, 32]")
        self._validate_prefix(self.config.prefix)

        self._lock = threading.Lock()
        self._last_ms = -1
        self._seq = 0

    def _epoch_ms(self) -> int:
        return int(time.time() * 1000)

    def _next_f48(self) -> int:
        """
        Zwraca 48-bitowe F zgodne z opisem.
        """
        with self._lock:
            now_ms = self._epoch_ms()
            if now_ms < self._last_ms:
                # zegar się cofnął – przyklejamy do ostatniej wartości
                now_ms = self._last_ms

            if now_ms == self._last_ms:
                self._seq = (self._seq + 1) & 0xFF
                if self._seq == 0:
                    # overflow – czekamy na następny ms
                    while now_ms <= self._last_ms:
                        now_ms = self._epoch_ms()
            else:
                self._seq = 0

            self._last_ms = now_ms

            # bierzemy tylko wrap_bits najmniej znaczących bitów epoch_ms
            mask = (1 << self.config.wrap_bits) - 1
            t_low = now_ms & mask

            # składamy F: [t_low (wrap_bits)] [node_id (8)] [seq (8)]
            # wrzucamy t_low w najstarsze bity z dostępnych 48
            # czyli:
            #  F = (t_low << (16)) | (node_id << 8) | seq
            # wrap_bits <= 32, więc t_low pasuje w 32 bity,
            # a my i tak mamy 16 b na node+seq
            f = (t_low << 16) | ((self.config.node_id & 0xFF) << 8) | (self._seq & 0xFF)
            # upewniamy się, że to 48 bitów
            return f & ((1 << 48) - 1)

    @staticmethod
    def _checksum_byte(value_48bit: int) -> int:
        """
        Suma bajtów 48-bitowej liczby (big endian) % 256.
        """
        b = value_48bit.to_bytes(6, "big")
        return sum(b) & 0xFF

    def next_name(self, day: date | None = None) -> str:
        """
        Zwraca nazwę:
            <prefix>_YYYYMMDD_(FFFFFFFFFFFF_CC)
        """
        day = day or date.today()
        day_str = day.strftime("%Y%m%d")

        f = self._next_f48()
        cc = self._checksum_byte(f)

        f_hex = f"{f:012X}"
        cc_hex = f"{cc:02X}"

        return f"{self.config.prefix}_{day_str}_({f_hex}_{cc_hex})"

    @staticmethod
    def _validate_prefix(prefix: str) -> None:
        """
        Prefix może zawierać tylko litery i cyfry ASCII.
        """
        if not prefix:
            raise ValueError("prefix must be non-empty")
        if not prefix.isascii():
            raise ValueError("prefix must contain only ASCII characters")
        if not prefix.isalnum():
            raise ValueError("prefix may only use letters or digits")
