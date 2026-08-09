# UDP settings

Panel DB — source of truth. Environment используется только для bootstrap/connectivity/secrets; runtime settings Checker регулярно получает через authenticated internal API.

## Основные defaults

| Setting | Default | Назначение |
|---|---:|---|
| UDP enabled | off | Глобальный switch |
| Probe interval | 100 ms | 10 Hz/path |
| Probe timeout | 1500 ms | Когда реально отправленная probe становится loss |
| Sliding window | 30 s | Online aggregation |
| Minimum samples | 20 | Защита от premature state |
| Minimum coverage | 0.70 | Минимум выполненных slots |
| Aggregation | 1 s | Runtime aggregates |
| Checker batch | 1 s | Checker → Panel |
| Browser batch | 1 s | SSE batch cadence |
| DB persist | 10 s | Batch DB writes |
| Playback delay | 1 s | Live graph buffer |
| Browser live window | 10 min | Bounded UI history |
| Raw retention | 24 h | Compact 100 ms RTT/loss slots |
| Aggregate retention | 30 d | Long-term rows |
| Direct Receiver probe | 1 s | Checker → Receiver health |
| Receiver peer probe | 2 s | Receiver mesh health |

Threshold defaults: degraded loss `5%`, offline loss `95%`, robust jitter `25 ms`, Receiver confidence `0.70`, receiver disagreement `20%`, stale data `5 s`, anti-flap streaks `3/3/3`.

## Receiver selection

- `All`: все enabled и не excluded Receiver.
- `Selected`: только явно выбранные.
- `Automatic`: deterministic subset; default upper limit `5`.

При небольшом числе Receiver нормальный выбор — `All`. Поле локальных VLESS SID поддерживает несколько значений через пробел; все co-located SID всегда возвращаются в exclusions даже после ручного редактирования списка.

## Overload automation

Все четыре решения независимы и отключаемы:

- `AutoManageRate` — снижать Hz только при подтверждённом `OVERLOADED`;
- `AutoManageReceivers` — при более тяжёлой перегрузке сокращать path fan-out;
- `AutoManageDisplayBuffer` — временно увеличивать UI buffer (это не снижает CPU);
- `PrioritizeProblemNodes` — только во время overload оставлять нестабильные nodes на полной детализации.

Default detector: scheduler lag `500 ms`, queue ratio `0.75`, sustain `5 s`. Rate steps соответствуют примерно `10 → 8 → 5 → 2 → 1 Hz`; сначала меняется rate, Receiver fan-out — только на более высоком overload level. Возврат идёт постепенно.

При `HEALTHY` или кратком `HIGH_LOAD` sampling не уменьшается, чтобы не терять microspikes.

## Presets

`realtime` соответствует defaults выше. Для ручного operational профиля можно использовать `balanced ≈ 1 Hz`, `low-load ≈ 0.2 Hz`; `diagnostic` предназначен только для выбранных nodes через per-node `diagnostic`, а не для глобального автоматического включения. `custom` — произвольные валидные значения.

Preset — лишь набор settings, отдельного runtime для него нет.

## Performance estimator

Admin API рассчитывает active paths с учётом node mode и exclusions, probes/replies/packets per second, ожидаемые persistent associations, samples/window, приблизительный network traffic, raw RAM и 24h compact payload. Это warning/estimate, а не искусственный hard limit.
