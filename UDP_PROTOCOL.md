# UDP protocol v1

Data plane бинарный, bounded (`96..256` bytes) и authenticated. Magic: `XCU1`, version: `1`.

| Offset | Size | Field |
|---:|---:|---|
| 0 | 4 | magic `XCU1` |
| 4 | 1 | version |
| 5 | 1 | message type |
| 6 | 2 | key version, big-endian |
| 8 | 8 | Receiver ID hash |
| 16 | 8 | Node stable ID hash |
| 24 | 8 | Probe ID |
| 32 | 8 | Sequence |
| 40 | 8 | sender Unix milliseconds |
| 48 | 12 | nonce |
| 60 | 2 | flags |
| 62 | 2 | payload length |
| 64 | N | optional payload |
| 64+N | 32 | HMAC-SHA256 |

Message types: `1 PROBE_REQUEST`, `2 PROBE_RESPONSE`, `3 PEER_REQUEST`, `4 PEER_RESPONSE`.

String stable IDs never отправляются в hot packet: wire ID — первые 64 bits SHA-256 от полной строки. Full ID остаётся в control plane.

HMAC покрывает весь header + payload до MAC. Decoder сначала ограничивает размер и проверяет framing, затем MAC. Receiver также проверяет allowed clock skew, bounded replay cache и per-source/global rate limit. Bootstrap/permanent HTTP credentials не являются UDP HMAC key.

Response сохраняет Probe ID, Sequence, Nonce, Receiver/Node ID и меняет только message type. Checker принимает response только если он совпал с существующей pending probe; поздние/неожиданные ответы игнорируются. Receiver не работает как unauthenticated echo.

## Nonce/replay

Checker/peer monitor генерирует случайный process prefix, а оставшиеся bytes связывает с monotonic probe ID. Receiver запоминает недавние authenticated nonces в bounded cache. Повтор не получает нормальный response и учитывается отдельным counter.

## Key lifecycle

Panel генерирует 32-byte data-plane key и хранит его через encrypted secret store. `keyVersion` позволяет совместимую ротацию. Receiver получает собственный key во время bootstrap/config sync; Checker получает необходимые keys только через authenticated internal API.
