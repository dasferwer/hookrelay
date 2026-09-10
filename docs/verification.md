# Проверка HookRelay

Итог проверен 11 сентября 2026 года локально на Docker Desktop, ARM64.

| Проверка | Результат |
|---|---|
| Ruff lint / format | Пройдены |
| uv.lock | Согласован с pyproject.toml |
| PostgreSQL integration tests | **15 passed** |
| Docker Compose | API, БД, RabbitMQ, dispatcher, worker и приёмник healthy |
| Live HTTP smoke | Пройден |
| Seed дважды | Успешно, демонстрационный пользователь не дублируется |
| Runtime image | pytest и Ruff отсутствуют |
| OpenAPI snapshot | Совпадает с запущенным API |

- Временная ошибка получателя: 503 → 503 → 200, три попытки.
- Постоянная ошибка: dead, затем успешный ручной replay.
- Timeout после commit приёмника: два запроса, одно бизнес-действие.
- Событие принято при остановленных worker и dispatcher.
- Worker завершён SIGKILL после commit получателя; после перезапуска lease
  восстановлен, доставка завершена второй попыткой, уникальное действие одно.
- Проверены подпись, защита от устаревшей подписи, private/multicast адреса,
  смешанный DNS-ответ, устаревшие поколения заданий и небезопасные JSON-значения.

Машиночитаемый результат: [verification.json](verification.json).
Команды повторения: [runbook.md](runbook.md).
Это проверка модели отказов локального стенда, без заявления о production RPS.
