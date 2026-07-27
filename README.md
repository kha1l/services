# servises

`servises` это отдельная Dagster code location для синхронизации служебных данных из Dodo API в PostgreSQL. Проект собран по той же модели, что и `aida_functions`, но живет как самостоятельный Docker/gRPC-образ для запуска в Kubernetes.

## Что внутри

- `tokens_job`
  Обновляет `access/refresh` токены из `special.tokens`.
  При ошибках ведет антиспам-алерты, может деактивировать `account.active=false` после 24 часов и удалить stale токен после 48 часов.
- `subscribes_job`
  Синхронизирует premium-подписки и список заведений из `marketplace/subscriptions`.
  Удаляет локальные подписки, если точка стала `free` или пропала из ответа Dodo API.
  Пользователю отправляет только сообщение об изменении premium-подписок.
- `franchisee_job`
  Обновляет справочник франчайзи и сетевых UUID.
- `defs.py`
  Корневой `Definitions` со всеми jobs, schedules и ресурсами.
- `database/`
  SQLAlchemy-модели и запросы к PostgreSQL.
- `resources/database.py`
  Dagster-ресурс для SQLAlchemy engine/sessionmaker.
- `resources/dodo_api.py`
  Общий клиент Dodo API с retry.
- `resources/rabbitmq.py`
  Ресурс для публикации сообщений в RabbitMQ.
- `tokens/`, `subscribes/`, `franchisee/`
  Изолированные пакеты с `jobs.py`, `ops.py`, `schedulers.py`, `definitions.py`.

## Поведение jobs

### `tokens_job`

- Источник данных: таблица `special.tokens`.
- Успешный refresh обновляет `access`, `refresh`, `last_update` и возвращает `account.active=true`.
- Ошибка refresh:
  - создает или обновляет запись в `special.service_alerts`;
  - через 24 часа без успешного обновления отключает аккаунт: `account.active=false`;
  - через 48 часов удаляет токен и связанные подписки;
  - отправляет Telegram-уведомление через RabbitMQ только один раз на период сбоя.

### `subscribes_job`

- Источник Dodo API: `https://api.dodois.{domain}/marketplace/subscriptions`.
- Для premium-подписок догружает карточки заведений через `units/stores`.
- Если `tariffAlias == free`, такая точка больше не считается premium и удаляется из локальных `special.subscribes`.
- Если UUID больше не пришел из `marketplace/subscriptions`, локальная подписка тоже удаляется.
- Если после удаления точка больше никем не используется, она удаляется из `special.units`.
- Уведомления об ошибках токена эта job не шлет: lifecycle токена полностью остается в `tokens_job`.
- В RabbitMQ отправляется только список точек, у которых изменилась premium-подписка.

### `franchisee_job`

- Запрашивает `franchisee/units`.
- Добавляет только новые UUID в локальный справочник.
- Существующие записи не перетирает.

## Устойчивость

- Ошибка Dodo API по одному токену не роняет весь Dagster run.
- Ошибка записи в БД по одному токену не роняет весь Dagster run.
- После ошибок выполняется `rollback`, и job продолжает обработку следующих токенов.
- На уровне `ops.py` тоже есть защитный `try/except`, поэтому step не должен падать из-за единичной runtime-ошибки.

## Расписания по умолчанию

- `tokens_job`: `0 */6 * * *`
- `subscribes_job`: `0 4 * * *`
- `franchisee_job`: `0 5 * * *`

Все расписания можно переопределить через `SCHEDULE_<NAME>_*`.

## Конфигурация

Основные переменные окружения:

- `DBASE_*` или `DATABASE_URL` для PostgreSQL
- `DODO_API_*` для retry/timeout клиента Dodo API
- `DODOVKUS_TOKEN_*` для дешифрования токенов перед Dodo API и сохранения
  обновленных токенов через `dodovkus/auth`
- `DODO_APP_<APPLICATION_NAME>_SECRET` и `DODO_APP_<APPLICATION_NAME>_VERIFIER`
  для refresh-token flow Dodo OAuth; в Kubernetes они должны приходить из
  `servises-secret`
- `RABBIT_*` для публикации в RabbitMQ
- `SCHEDULE_<JOB>_*` для расписаний Dagster

Пример лежит в `env.example`.

## Локальный запуск

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
dagster dev -m defs
```

Запуск конкретной job:

```bash
dagster job execute -m defs -j tokens_job
dagster job execute -m defs -j subscribes_job
dagster job execute -m defs -j franchisee_job
```

Если запускаешь job вручную из shell, не стоит делать `source .env` как bash-скрипт: в cron-строках есть пробелы, и shell будет ругаться. Для локальных запусков лучше экспортировать нужные переменные отдельно или использовать способ загрузки env, который понимает формат `.env`.

## Docker / Kubernetes

Сервис собирается в один Docker-образ и поднимает Dagster gRPC code location.
Production-образ работает под UID/GID `10001` и содержит доступный для записи
`DAGSTER_HOME=/opt/dagster/dagster_home` для служебных файлов выполнения Dagster.
Контейнерная команда:

```bash
dagster api grpc -m defs -h 0.0.0.0 -p 4000
```

В Kubernetes этот образ нужно подключить к основному Dagster instance как code location.
Чтобы schedules реально исполнялись автоматически, рядом должен работать Dagster daemon.
