# Upstream

## Checker

- Repository: `https://github.com/kutovoys/xray-checker`
- Branch при импорте: `main`
- Commit: `f829c2b7d44670f4db0f7355f445771e7cde7158`
- Commit date: `2026-07-02T00:33:31+02:00`
- License: AGPL-3.0 (`LICENSE` сохранён)

Изменения этого проекта добавляют UDP data/control plane, включают UDP в локальных SOCKS inbounds, сохраняют TCP semantics и добавляют Panel integration/metrics/tests.

## Go statuspage / Panel

- Repository: `https://github.com/Mrvibecodic/xray-checker-statuspage`
- Branch при импорте: `go-build`
- Commit: `2d3c621061e13faa67f0e00c59ec073e0018cf05`
- Commit date: `2026-07-24T18:32:22Z`
- License: MIT; исходный notice сохранён в `statuspage/LICENSE`.

Statuspage импортирован в `statuspage/` отдельным Go module. UDP tables/API/admin/live graph/alerts добавлены поверх upstream; существующие TCP tables и public API не заменяются.

Git remotes в development history сохранены как `upstream-checker` и `upstream-statuspage`, чтобы дальнейшие обновления можно было сравнивать с точной базой.
