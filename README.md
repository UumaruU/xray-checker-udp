# xray-checker-udp

`xray-checker-udp` — форк Xray Checker с независимым TCP+UDP-мониторингом Xray/VLESS-узлов и Go Panel. TCP-проверки upstream (`ip`, `status`, `download`) сохранены, а UDP измеряется по реальному пути через Xray:

`Checker → локальный SOCKS5 UDP ASSOCIATE → Xray outbound → Receiver → Xray → Checker`.

Прямой `Checker → Receiver` UDP используется только для здоровья измерительной инфраструктуры и не подменяет VPN-измерение.

## Что уже есть

- Realtime по умолчанию: `10 probes/s` на каждый `VLESS × Receiver` path.
- Одна persistent SOCKS5 UDP association на Xray node, общая для всех его Receiver.
- Bounded scheduler/queues/pending map; нет goroutine или timer на каждую probe.
- Компактный HMAC-SHA256 UDP-протокол, replay protection, timestamp skew check и rate limit.
- Один Receiver работает полноценно; при `2+` автоматически работает взаимная Receiver mesh-диагностика.
- Receiver можно ставить на уже существующий VLESS-сервер. Его локальный VLESS SID автоматически исключается из `VLESS → Receiver`, но Receiver peer health остаётся включён.
- Выбор Receiver для node: `All`, `Selected`, `Automatic`.
- UDP states: `ONLINE`, `DEGRADED`, `OFFLINE`, `UNKNOWN`, `UNSUPPORTED`, `DISABLED`.
- Receiver states: `HEALTHY`, `DEGRADED`, `UNREACHABLE`, `UNKNOWN`.
- Неисправный/недостоверный Receiver не ухудшает статус VLESS. При одном исправном Receiver диагностика продолжает работать.
- 24 часа обработанных 100 ms `RTT/loss` slot-результатов в компактных DB blocks; исходные UDP-пакеты не сохраняются. Дальше — агрегаты.
- SQLite и PostgreSQL через существующий Panel store.
- SSE live stream, event IDs/heartbeat и playback buffer по умолчанию `1 s`: 10 реальных точек воспроизводятся по исходным timestamps, а не рисуются одним burst.
- Missing measurement из-за локальной перегрузки — просто отсутствие точки, не `LOSS` и не скрытый `LOCAL_DROP`.
- Self-monitoring `HEALTHY / HIGH_LOAD / OVERLOADED` и отдельно включаемая автоматика probe rate, Receiver count, display buffer и приоритета проблемных VLESS.
- UDP/Receiver/global events и Telegram alerts совместимы с существующим ботом.
- Prometheus UDP metrics и performance estimator в Panel.

## Быстрый старт

Нужны Docker Compose v2 и два разных случайных секрета для внутренних API.

```bash
export SUBSCRIPTION_URL='https://example.invalid/subscription'
export PANEL_CHECKER_INTERNAL_TOKEN='replace-with-long-random-token'
export PANEL_ADMIN_TOKEN='replace-with-another-long-random-token'
export PANEL_PORT='18080'
export PANEL_PUBLIC_URL='http://SERVER:18080'
docker compose pull
docker compose up -d
```

Обычная установка скачивает готовые `amd64/arm64`-образы Panel и Checker из
GHCR и ничего не компилирует на VPS. По умолчанию используется тег `latest`.
Для воспроизводимого обновления или rollback задайте одинаковый тег обоих
образов, например `XRAY_CHECKER_VERSION=v0.2.0`, перед `pull` и `up`.

Если нужно собрать текущие исходники локально, используйте запасной Compose и
последовательную сборку. Это особенно важно на VPS с 1 vCPU / 2 GB RAM:

```bash
./scripts/install-from-source.sh
```

Скрипт сначала собирает Panel, затем Checker, поэтому две тяжёлые Go-сборки не
конкурируют за CPU и RAM. BuildKit сохраняет кэш Go modules и build cache для
последующих сборок. На минимальной VPS перед первой сборкой всё равно нужен
swap и несколько гигабайт свободного диска. Ручной эквивалент:

```bash
docker compose -f docker-compose.yml -f docker-compose.build.yml build panel
docker compose -f docker-compose.yml -f docker-compose.build.yml build checker
docker compose -f docker-compose.yml -f docker-compose.build.yml up -d --no-build
```

`PANEL_PORT` задаёт внешний HTTP-порт Panel и по умолчанию равен `8080`. Если
порт занят, укажите любой свободный, например `18080`. Для Receiver задайте
`PANEL_PUBLIC_URL` как реально доступный адрес Panel (с тем же портом при
прямом HTTP-доступе; за reverse proxy это обычно обычный HTTPS-домен).

Panel: `http://SERVER:PANEL_PORT/`
UDP admin: `http://SERVER:PANEL_PORT/udp-admin`
Checker/Prometheus: `http://SERVER:2112/`

Если `SUBSCRIPTION_URL` уже управляется через существующий statuspage/Telegram workflow, его можно не задавать повторно.

UDP по умолчанию выключен. В `/udp-admin`:

1. Введите `PANEL_ADMIN_TOKEN`.
2. Создайте Receiver (`Name`, уникальный публичный IP/FQDN, UDP port, опционально один или несколько локальных VLESS SID через пробел).
3. Panel откроет pairing-окно с готовым Receiver Compose и будет ждать обе стороны соединения.
4. Откройте указанный UDP port на сервере Receiver и запустите Compose. Receiver сам повторяет Receiver → Panel handshake, а Panel проверяет обратный Panel → Receiver UDP path.
5. Только после успешной двусторонней проверки Receiver появляется в рабочем списке. Затем включите UDP monitoring и выберите `All / Automatic / Selected` для нужных nodes.

Сгенерированный Compose использует образ из `UDP_RECEIVER_IMAGE`. Для локальной разработки укажите собственный собранный tag.

## Receiver на том же сервере, что VLESS

Это рекомендуемый вариант, когда нет смысла покупать отдельные VPS. При создании Receiver укажите `Local VLESS SID`: можно передать несколько SID через пробел. Panel удерживает все эти SID в exclusions для данного Receiver. Нулевой локальный RTT поэтому не искажает VPN-статистику.

Например, при `DE/SE/FI/NL` каждый VLESS проверяется с остальных трёх серверов, а все четыре Receiver продолжают взаимные медленные peer probes. Центральный Checker коррелирует эти наблюдения; сами Receiver не решают, кто виноват.

## Статусы и доверие

- `ONLINE`: все достоверные path-наблюдения нормальные.
- `DEGRADED`: хотя бы один достоверный path подтверждённо плохой, при этом не все offline; regional disagreement не скрывается majority vote.
- `OFFLINE`: все доступные достоверные направления подтверждённо offline.
- `UNKNOWN`: достоверных Receiver/измерений недостаточно или monitoring infrastructure недостоверна.
- `UNSUPPORTED`: Xray node не поддерживает нужный UDP path.
- `DISABLED`: UDP выключен глобально или для node.

`DEGRADED/UNREACHABLE/UNKNOWN` Receiver с недостаточным confidence не может массово превратить VLESS nodes в outage.

## Перегрузка

Главный сигнал — фактическое отставание обработки: scheduler lag, bounded queues и локальные omissions. CPU/RAM сами по себе не являются причиной объявить VPN плохим.

При нормальной нагрузке никакого adaptive sampling нет. Только после подтверждённого `OVERLOADED` автоматика, если разрешена в Panel, сначала снижает Hz у стабильных nodes, затем при более тяжёлой перегрузке сокращает активные Receiver. Проблемные nodes можно оставить на полной частоте. После восстановления параметры возвращаются автоматически.

Display buffer (`1 s` по умолчанию) сглаживает доставку live batch, но не считается вычислительным запасом и не уменьшает CPU load.

## История и график

Каждый ожидаемый slot кодируется одним `uint16`: RTT с шагом 250 µs либо `MISSING / INVALID / LOSS`. Timestamp восстанавливается из начала блока и интервала. Высокоточные результаты хранятся 24 часа по умолчанию; пакетные payloads не сохраняются.

`/api/udp/live` — SSE, `/api/udp/history` — compact raw + aggregates. Browser хранит только короткое live-окно. Canvas соединяет соседние реальные RTT визуальной линией, но интерполяция никогда не попадает в DB/API/statistics.

## Prometheus

Основные серии: `udp_probe_sent_total`, `udp_probe_received_total`, `udp_probe_timeout_total`, `udp_probe_invalid_total`, `udp_scheduler_lag_seconds`, `udp_scheduler_queue`, `udp_scheduler_dropped_total`, `udp_active_associations`, `udp_association_reconnect_total`, `udp_rtt_milliseconds`, `udp_rtt_p95_milliseconds`, `udp_jitter_milliseconds`, `udp_loss_ratio`, Receiver health и config metrics.

## NAT, firewall и IPv6

- Receiver принимает UDP на заданном `RECEIVER_UDP_PORT`; этот порт должен быть достижим с Xray outbound-сетей и других Receiver.
- Heartbeat идёт исходящим HTTPS к `PANEL_URL`; отдельный входящий control port Receiver не нужен.
- За NAT нужен UDP port-forward. FQDN разрешён; IPv4/IPv6 адреса кодируются SOCKS5 UDP согласно ATYP.
- Не полагайтесь на IP whitelist как на аутентификацию: data plane подписан HMAC.

## Обновление, backup и rollback

Перед обновлением сохраните Panel volume/SQLite DB (или сделайте backup PostgreSQL) и Receiver `/data`. UDP migration добавляет новые таблицы и не меняет семантику существующей TCP history. UDP по умолчанию можно оставить выключенным до регистрации Receiver.

Обновление готовых образов:

```bash
docker compose pull
docker compose up -d
```

Для rollback задайте предыдущий release tag через `XRAY_CHECKER_VERSION`, снова
выполните `pull` и `up -d`, затем при необходимости восстановите backup DB.
Receiver credentials находятся только в его `/data`; потеря volume означает
повторную регистрацию. Никогда не используйте `docker compose down -v` при
обычном обновлении: `-v` удалит Panel volume.

## Диагностика

Проверяйте цепочку по порядку: global UDP → node UDP/support → config revision → Receiver heartbeat → direct UDP → Receiver peer mesh → SOCKS5 association → Xray outbound → proxied response → aggregation → Checker→Panel batch → DB/SSE → browser playback. Secrets в diagnostics не выводятся.

Подробности: [ARCHITECTURE.md](ARCHITECTURE.md), [UDP_PROTOCOL.md](UDP_PROTOCOL.md), [SETTINGS.md](SETTINGS.md), [SECURITY.md](SECURITY.md), [UPSTREAM.md](UPSTREAM.md).

## Upstream и лицензии

Основной код основан на `kutovoys/xray-checker` (AGPL-3.0). Go statuspage импортирован из `Mrvibecodic/xray-checker-statuspage` (MIT; его `statuspage/LICENSE` сохранён). Точные ветки/SHA/даты и границы модификаций указаны в [UPSTREAM.md](UPSTREAM.md).

Этот репозиторий распространяется по AGPL-3.0; см. `LICENSE`.
