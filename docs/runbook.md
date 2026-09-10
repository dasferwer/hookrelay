# Эксплуатация HookRelay

## Запуск и диагностика

```bash
docker compose up --build -d --wait --wait-timeout 180
docker compose ps
docker compose logs --tail=100 api worker
curl -fsS http://localhost:8110/health
```

Swagger: <http://localhost:8110/docs>. OpenAPI: `/openapi.json`.
HTTP-метрики: `/metrics`, формат Prometheus. `X-Request-ID` создаётся сервером
и записывается в HTTP-лог. Метки метрик используют шаблон маршрута, чтобы
UUID документов или заданий не создавали неограниченное число временных рядов.
Возраст heartbeat в `/health` позволяет отдельно видеть состояние фоновой обработки.
HTTP health проверяет доступность БД; отсутствие heartbeat не блокирует приём
работы в долговременное хранилище.

## Воспроизводимые проверки

```bash
uv sync --extra dev --frozen
uv run ruff check .
uv run ruff format --check .
docker compose --profile test build test
docker compose --profile test run --rm test
docker compose exec -T api python scripts/smoke.py
```

Тесты работают на отдельном PostgreSQL `test-db` с именем БД `*_test`.
Защитная проверка не позволяет запустить очистку таблиц на обычной базе.
Тестовый PostgreSQL использует tmpfs. После проверки его можно остановить:

```bash
docker compose --profile test stop test-db
```

Повторный запуск `scripts/seed.py` не создаёт дубли демонстрационных данных.
Образы `runtime` и `test` разделены: pytest/Ruff/HTTPX входят только в тестовый
образ. Python-пакеты фиксируются в `uv.lock` и `requirements.lock`;
Docker собирает wheelhouse из зафиксированных версий.

## Миграции и данные

Миграции запускаются отдельным одноразовым сервисом до API и воркера.
Начальная миграция поддерживает повторный `alembic upgrade head`.
Обратное удаление исходной схемы намеренно не реализовано: восстановление
выполняется из резервной копии, а изменения схемы добавляются новой миграцией.
Перед изменениями рабочего окружения нужно проверить резервную копию на
отдельной БД. Обычный `docker compose down` сохраняет том с данными.

## Границы окружения

Compose предназначен для локальной демонстрации. Порты опубликованы только
на `127.0.0.1`, PostgreSQL не публикует порт на хост. Значения паролей и JWT
в Compose и seed демонстрационные. Для внешнего развёртывания нужны свои
секреты, TLS, ограничения запросов на входе и резервное копирование.
Проект не заявляет эксплуатацию в коммерческом production.
