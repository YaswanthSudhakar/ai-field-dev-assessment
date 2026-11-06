"""Streaming PII redaction (emails, SSNs, card numbers).

These patterns only use letters, digits, . _ % + - @ and spaces between digit
groups. Any other character can't be inside a match, so everything up to the
last such character is safe to redact and send right away. Only the trailing
partial word is held back, which keeps latency to about one token.

If no safe character shows up for a long time (a base64 blob, say), the buffer
is capped at max_hold. Every pattern has a maximum length, so keeping the last
MAX_MATCH_LEN characters is enough to never cut a match in half.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

REDACTED = "[REDACTED]"

# The email pattern has no left anchor on purpose: over-redacting is safer than leaking.
_EMAIL = r"[A-Za-z0-9._%+\-]{1,64}@[A-Za-z0-9\-]{1,63}(?:\.[A-Za-z0-9\-]{1,63}){0,8}\.[A-Za-z]{2,24}"
_SSN = r"(?<![\w\-])(?!000|666|9\d\d)\d{3}(?P<sep>[- ]?)(?!00)\d{2}(?P=sep)(?!0000)\d{4}(?![\d\-])"
_CARD = r"(?<![\w\-])\d(?:[ \-]?\d){12,18}(?![\d\-])"

PII_RE = re.compile(rf"(?P<email>{_EMAIL})|(?P<ssn>{_SSN})|(?P<card>{_CARD})")

MAX_MATCH_LEN = 680

_INNER_CHARS = frozenset("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._%+-@")


def luhn_ok(digits: str) -> bool:
    total, alt = 0, False
    for ch in reversed(digits):
        d = ord(ch) - 48
        if alt:
            d *= 2
            if d > 9:
                d -= 9
        total += d
        alt = not alt
    return total % 10 == 0


def _replace(m: re.Match[str]) -> str:
    if m.lastgroup == "card":
        digits = re.sub(r"\D", "", m.group())
        if not luhn_ok(digits):
            return m.group()
    return REDACTED


def redact(text: str) -> str:
    return PII_RE.sub(_replace, text)


def _is_ascii_digit(ch: str) -> bool:
    return "0" <= ch <= "9"


def _digit_run_glued_to_word(buf: str, j: int) -> bool:
    """True for things like 'v12', where the digits are stuck to a word."""
    while j >= 0 and _is_ascii_digit(buf[j]):
        j -= 1
    if j < 0:
        return False
    prev = buf[j]
    return prev == "_" or (prev.isalnum() and not _is_ascii_digit(prev))


def _last_boundary(buf: str) -> int:
    for i in range(len(buf) - 1, -1, -1):
        ch = buf[i]
        if ch in _INNER_CHARS:
            continue
        if ch == " " and i > 0 and _is_ascii_digit(buf[i - 1]) and not _digit_run_glued_to_word(buf, i - 1):
            continue
        return i
    return -1


@dataclass
class StreamingRedactor:
    max_hold: int = 2048
    _pending: str = field(default="", repr=False)
    redactions: int = 0
    peak_pending: int = 0

    def feed(self, delta: str) -> str:
        """Returns the text that is now safe to send (can be empty)."""
        if not delta:
            return ""
        buf = self._pending + delta
        cut = _last_boundary(buf) + 1

        if cut == 0 and len(buf) > self.max_hold:
            cut = self._forced_cut(buf)

        self._pending = buf[cut:]
        self.peak_pending = max(self.peak_pending, len(self._pending))
        return self._redact(buf[:cut]) if cut else ""

    def flush(self) -> str:
        out, self._pending = self._redact(self._pending), ""
        return out

    def _forced_cut(self, buf: str) -> int:
        cut = len(buf) - MAX_MATCH_LEN
        for m in PII_RE.finditer(buf):
            if m.start() < cut < m.end():
                return m.start()
            if m.start() >= cut:
                break
        return cut

    def _redact(self, text: str) -> str:
        if not text:
            return text

        def repl(m: re.Match[str]) -> str:
            out = _replace(m)
            if out is REDACTED:
                self.redactions += 1
            return out

        return PII_RE.sub(repl, text)
