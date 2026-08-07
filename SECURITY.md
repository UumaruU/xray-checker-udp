# Security

## Границы доверия

Panel — control plane и source of truth. Checker получает UDP config только с `PANEL_CHECKER_INTERNAL_TOKEN`; admin API защищён отдельным `PANEL_ADMIN_TOKEN`. Не используйте один token для обеих ролей и не публикуйте внутренний Panel port `8081` наружу.

Receiver registration использует single-use bootstrap token с expiry 30 минут. После первого успешного exchange Panel удаляет bootstrap hash и выдаёт permanent bearer credential. Receiver сохраняет его в `/data/receiver-state.json` с mode `0600`.

Data-plane keys генерируются случайно и Panel хранит их через existing encrypted secret store (`SECRET_KEY`/локальный secret key). API/логи/UI не должны показывать их после bootstrap/config transport.

## UDP data plane

- packet size жёстко ограничен `<=256` bytes;
- HMAC-SHA256 обязателен;
- framing/version/payload length проверяются до обработки;
- clock skew ограничен;
- replay cache bounded;
- per-source/global rate limiting bounded;
- response не больше request;
- неизвестные/invalid packets не превращают сервис в generic echo;
- IP whitelist можно использовать как дополнительный firewall, но не вместо cryptographic authentication.

При компрометации Receiver считайте его data/control credentials скомпрометированными. Для v0.1.0 безопасный operational recovery — отключить этот Receiver в Panel, удалить его `/data`, создать новую Receiver registration и заменить deployment.

## Network deployment

Публикуйте наружу только действительно нужные порты: Panel/reverse proxy, Checker metrics при необходимости и UDP port каждого Receiver. Внутренний Checker↔Panel API держите на loopback/private network. Receiver health HTTP listener по умолчанию только `127.0.0.1` внутри контейнера.

За reverse proxy используйте HTTPS для public Panel/Receiver heartbeat. Не вставляйте bearer/bootstrap/data keys в URL, labels Prometheus или diagnostics.

## Reporting

Для уязвимостей upstream-составляющих сначала сверяйтесь с их security policy. Для изменений этого fork не публикуйте рабочие credentials или subscription URLs в issue/логах; приложите минимальный redacted reproducer.
