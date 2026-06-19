# Лекция 10: Логирование и трассировка

## О чем лекция
Лекция посвящена системам логирования, их стоимости и оптимизации, структурированным логам, уровням логирования, сэмплированию и распределенной трассировке для отладки микросервисных систем.

## Ключевые вопросы
- Что такое логи и почему они дорогие?
- Как правильно структурировать логи для эффективной обработки?
- Какие уровни логирования существуют и когда их использовать?
- Как не захлебнуться в логах при сбоях?
- Что такое distributed tracing и как он помогает в отладке?
- Как отслеживать зависшие запросы в микросервисах?

---

## Основная часть

### Что такое лог?

**Определение**: Лог — это **информация о событии**, которое когда-то произошло.

**Структура**:
```
Timestamp + Событие + Контекст
```

**Пример**:
```
2024-01-15T14:23:45.123Z [INFO] User 12345 successfully logged in from IP 192.168.1.100
```

**Компоненты**:
- **Timestamp** — когда произошло событие
- **Level** — уровень важности (INFO, ERROR, WARNING)
- **Message** — что произошло
- **Context** — дополнительные данные (user_id, IP, и т.д.)

---

### Структурированные логи

**Проблема**: Неструктурированные логи сложно парсить и анализировать.

**Неструктурированный лог** (плохо):
```
User john_doe logged in from 192.168.1.100 at 14:23:45
```

**Структурированный лог** (хорошо):
```json
{
  "timestamp": "2024-01-15T14:23:45.123Z",
  "level": "INFO",
  "event": "user_login",
  "user_id": "12345",
  "username": "john_doe",
  "ip_address": "192.168.1.100",
  "request_id": "abc-123-def"
}
```

#### Форматы структурированных логов

| Формат | Описание | Преимущества | Недостатки |
|--------|----------|--------------|------------|
| **JSON** | Один JSON-объект на строку | Читаемый, гибкий | Больше места, медленнее |
| **TSV** | Tab-separated values | Компактный, быстрый парсинг | Менее гибкий |
| **Protobuf** | Бинарный формат | Самый компактный, быстрый | Не читаемый человеком |

**Best practice**: Используйте **JSON** для большинства случаев (баланс читаемости и производительности).

**Пример JSON логирования**:
```python
import json
import logging

logger = logging.getLogger()

def log_event(event_type, **context):
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": "INFO",
        "event": event_type,
        **context
    }
    logger.info(json.dumps(log_entry))

# Использование
log_event("user_login", user_id=12345, ip="192.168.1.100")
```

---

### Хранение и обработка логов

#### Архитектура логирования

```
[Приложение] → Пишет логи в файл/stdout
      ↓
[Sidecar Agent] → Собирает логи (Filebeat, Fluentd)
      ↓
[Обогащение] → Добавление метаданных (Logstash)
      ↓
[Хранилище + Поиск] → Elasticsearch, Loki, CloudWatch
      ↓
[Визуализация] → Kibana, Grafana
```

**Компоненты**:

1. **Сбор логов**: Filebeat, Fluentd, Vector
2. **Обработка и обогащение**: Logstash, Vector
3. **Хранение**: Elasticsearch, Loki, S3 + Athena
4. **Поиск и визуализация**: Kibana, Grafana

**Популярные стеки**:
- **ELK**: Elasticsearch + Logstash + Kibana
- **PLG**: Promtail + Loki + Grafana
- **Cloud**: CloudWatch Logs, Stackdriver

---

### Логи — это дорого!

**Проблема**: Обработка логов может потреблять **до 70% ресурсов системы**.

#### Где тратятся ресурсы?

| Этап | Стоимость | Описание |
|------|-----------|----------|
| **Сериализация** | CPU | Превращение объектов в JSON/Protobuf |
| **Десериализация** | CPU | Парсинг логов в агрегаторе |
| **Сбор (Sidecar)** | CPU, Memory | Агент читает файлы и отправляет логи |
| **Обогащение** | CPU, Memory | Добавление контекста (hostname, pod, etc.) |
| **Передача** | Network | Отправка логов в хранилище |
| **Хранение** | Disk, $$ | Elasticsearch, S3 — дорого при больших объемах |
| **Индексация** | CPU, Disk | Создание индексов для поиска |

**Пример**:
```
1000 RPS × 1 KB лог × 86400 секунд = 86.4 GB логов в день
86.4 GB × 30 дней = 2.6 TB в месяц
2.6 TB в Elasticsearch ≈ $500-1000/месяц 💸
```

**Оптимизация**:
- Логировать только важное
- Использовать сэмплирование
- Сжимать логи (gzip)
- Использовать более дешевое хранилище для старых логов (S3)

---

### Логи как API

**Идея**: Логи не только для отладки, но и как **источник данных для бизнеса**.

#### Use cases

**1. Аналитика**:
```python
# Логи событий пользователя
log_event("product_viewed", user_id=123, product_id=456)
log_event("add_to_cart", user_id=123, product_id=456)
log_event("purchase", user_id=123, product_id=456, price=99.99)

# Аналитика строится на этих логах
```

**2. Служба поддержки**:
```python
# Лог действий пользователя
log_event("button_clicked", user_id=123, button="submit_form")
log_event("form_submitted", user_id=123, form_data={...})

# Оператор поддержки видит последовательность действий
```

**3. Аудит**:
```python
log_event("admin_action", user_id=789, action="delete_user", target_id=123)
# Для compliance и расследований
```

**4. Мониторинг и алертинг**:
```python
log_event("payment_failed", user_id=123, error_code="insufficient_funds")
# Алерт: если > 10 payment_failed за минуту → инцидент
```

#### Требования к "логам как API"

✅ **Стабильная схема**: Не ломать формат логов  
✅ **Обратная совместимость**: Добавлять поля, не удалять старые  
✅ **Документация**: Описать все события и их поля  
✅ **Версионирование**: `"schema_version": "2.0"`  

**Пример схемы события**:
```json
{
  "schema_version": "2.0",
  "event": "user_login",
  "required_fields": ["user_id", "timestamp"],
  "optional_fields": ["ip_address", "user_agent", "country"]
}
```

---

### Уровни логирования

#### ERROR — Требуется вмешательство человека

**Когда использовать**:
- ❌ Критическая ошибка, которая влияет на бизнес
- ❌ Невозможно выполнить операцию
- ❌ Нарушен SLA

**Примеры**:
```python
logger.error("Payment gateway timeout", extra={
    "user_id": 123,
    "order_id": 456,
    "gateway": "stripe",
    "timeout_ms": 30000
})

logger.error("Database connection failed", extra={
    "database": "postgres_primary",
    "error": "connection refused"
})
```

**На ERROR можно навесить алерты** → инженер получит уведомление.

#### WARNING — Предупреждение, не требует вмешательства

**Когда использовать**:
- ⚠️ Что-то странное, но не критично
- ⚠️ Деградация сервиса (использует fallback)
- ⚠️ Приближение к лимитам

**Примеры**:
```python
logger.warning("High memory usage", extra={
    "memory_percent": 85,
    "threshold": 80
})

logger.warning("Cache miss rate high", extra={
    "miss_rate": 0.45,
    "threshold": 0.3
})

logger.warning("Using fallback configuration", extra={
    "reason": "config_service_unavailable"
})
```

**WARNING НЕ должен вызывать алерты**, но должен мониториться.

#### INFO, DEBUG, TRACE

| Уровень | Назначение |
|---------|------------|
| **INFO** | Нормальные события (user logged in, order created) |
| **DEBUG** | Детали для отладки (обычно выключен в production) |
| **TRACE** | Очень детальная трассировка (редко используется) |

---

### Логи во время сбоев

**Проблема**: При сбоях приложения пишут **гораздо больше логов**.

**Сценарий**:
```
Нормальная работа: 1000 логов/сек
База данных упала: 50000 логов/сек (ERROR в каждом запросе)
```

**Последствия**:
- Система обработки логов **захлебнется**
- Логи начнут теряться
- Хранилище переполнится
- Еще больше усугубит проблему

**Пример**:
```python
# ❌ Плохо: логируем каждую ошибку
def process_request():
    try:
        db.query()
    except DBError as e:
        logger.error(f"DB error: {e}")  # 50k логов/сек!
        raise
```

#### Решения

**1. Rate Limiting логов**:
```python
from functools import lru_cache
from time import time

@lru_cache(maxsize=1024)
def should_log(key, window=60):
    # Максимум 1 лог в минуту для каждого key
    last_logged = getattr(should_log, f'last_{key}', 0)
    now = time()
    if now - last_logged > window:
        setattr(should_log, f'last_{key}', now)
        return True
    return False

def process_request():
    try:
        db.query()
    except DBError as e:
        if should_log("db_error"):
            logger.error(f"DB error (rate limited): {e}")
        raise
```

**2. Backpressure**:
- Когда очередь логов переполнена → drop менее важные логи
- Приоритет: ERROR > WARNING > INFO > DEBUG

**3. Dedicated error aggregation**:
- Использовать Sentry, Rollbar для агрегации ошибок
- Не логировать каждый stack trace в логи

---

### Сэмплирование логов

**Идея**: Логировать не **все** события, а только **выборочно**.

#### Стратегии сэмплирования

**1. Процентное сэмплирование**:
```python
import random

def log_with_sampling(message, sample_rate=0.1, **kwargs):
    """Логируем только 10% событий"""
    if random.random() < sample_rate:
        logger.info(message, extra=kwargs)

# Использование
log_with_sampling("User viewed page", sample_rate=0.01, user_id=123)
```

**2. Адаптивное сэмплирование**:
```python
def adaptive_sample_rate(event_type):
    """Меньше логов для частых событий"""
    frequency = event_frequency[event_type]
    if frequency > 1000:  # Очень частое событие
        return 0.001  # 0.1%
    elif frequency > 100:
        return 0.01   # 1%
    else:
        return 0.1    # 10%
```

**3. Сэмплирование по трейсу**:
```python
# Если запрос уже выбран для трассировки → логируем все
if trace_context.is_sampled():
    logger.info("Detailed log because trace is sampled")
```

**4. Логируем все ошибки, сэмплируем успехи**:
```python
def smart_log(success, message, **kwargs):
    if not success:
        logger.error(message, extra=kwargs)  # Все ошибки
    else:
        log_with_sampling(message, sample_rate=0.01, **kwargs)  # 1% успехов
```

**Важно**: Сэмплирование нужно **продумывать заранее**, на уровне библиотеки логирования.

---

### Трассировка (Distributed Tracing)

**Проблема**: В микросервисной архитектуре один запрос проходит через **множество сервисов**. Как отследить его путь?

#### Сквозные идентификаторы (Trace ID)

**Принцип**: Прокидывать уникальный **Trace ID** через все сервисы и логи.

```
Client → API Gateway → Auth Service → User Service → DB
           |              |              |
        [trace: abc-123-def]
```

**Все логи содержат `trace_id`**:
```json
// API Gateway
{"trace_id": "abc-123", "service": "gateway", "event": "request_received"}

// Auth Service
{"trace_id": "abc-123", "service": "auth", "event": "token_validated"}

// User Service
{"trace_id": "abc-123", "service": "users", "event": "user_fetched"}
```

**Поиск по Trace ID** → видим весь путь запроса!

#### Структура трейса

**OpenTelemetry / OpenTracing стандарт**:
```
Trace
  └─ Span (API Gateway) [100ms]
       └─ Span (Auth) [20ms]
       └─ Span (User Service) [50ms]
            └─ Span (DB Query) [40ms]
```

**Пример Span**:
```json
{
  "trace_id": "abc-123-def",
  "span_id": "span-456",
  "parent_span_id": "span-789",
  "service": "user-service",
  "operation": "get_user",
  "start_time": "2024-01-15T14:23:45.000Z",
  "end_time": "2024-01-15T14:23:45.050Z",
  "duration_ms": 50,
  "tags": {
    "user_id": 12345,
    "db.type": "postgres"
  }
}
```

---

### Инструменты трассировки

#### Jaeger

**Назначение**: Визуализация распределенных трейсов.

**Возможности**:
- Видим **waterfall диаграмму** запроса
- Показывает, где сколько времени потратилось
- Dependency graph между сервисами

**Пример UI**:
```
Request: GET /api/users/123 [Total: 250ms]
├─ API Gateway [10ms]
├─ Auth Service [30ms]
│  └─ Redis (check token) [25ms]
└─ User Service [210ms]
   ├─ DB Query (user) [150ms] ⚠️ Slow!
   └─ DB Query (preferences) [50ms]
```

**Вывод**: Проблема в `DB Query (user)` — нужна оптимизация.

#### Zipkin

**Назначение**: Альтернатива Jaeger, похожий функционал.

**Особенность**: Акцент на определении **начала и конца** каждого span.

**Как работает**:
```python
# В начале операции
span = tracer.start_span("get_user")
logger.info("Operation started", extra={"span_id": span.id})

try:
    user = db.get_user(123)
    span.set_tag("user_id", user.id)
finally:
    # В конце операции (обязательно!)
    span.finish()
    logger.info("Operation finished", extra={"span_id": span.id})
```

**Поиск зависших запросов**:
```sql
-- Запросы, у которых есть start, но нет finish
SELECT trace_id, span_id, operation, start_time
FROM spans
WHERE end_time IS NULL
  AND start_time < NOW() - INTERVAL '5 minutes'
```

**Алерт на зависшие запросы**:
```yaml
alert: HangingRequests
expr: |
  count(spans{end_time="null"} and start_time < time() - 5m) > 10
for: 5m
labels:
  severity: critical
annotations:
  summary: "{{ $value }} requests are hanging for >5 minutes"
```

---

### Интеграция трассировки и логов

**Best Practice**: Связать логи с трейсами.

```python
import logging
from opentelemetry import trace

def log_with_trace(message, level="INFO", **context):
    """Логирует с автоматическим добавлением trace_id"""
    span = trace.get_current_span()
    trace_id = span.get_span_context().trace_id if span else None
    
    log_entry = {
        "timestamp": datetime.utcnow().isoformat(),
        "level": level,
        "message": message,
        "trace_id": trace_id,
        **context
    }
    
    logging.info(json.dumps(log_entry))

# Использование
with tracer.start_span("process_order"):
    log_with_trace("Order received", order_id=123)
    # ...
    log_with_trace("Order processed successfully")
```

**В Kibana/Grafana** можно:
1. Найти trace в Jaeger
2. Скопировать `trace_id`
3. Найти все логи с этим `trace_id` в Kibana
4. Видеть полный контекст!

---

## Выводы и инсайты

- **Логи дорогие**: Могут потреблять до 70% ресурсов — логируйте только важное

- **Структурированные логи обязательны**: JSON на строку — стандарт для современных систем

- **ERROR ≠ WARNING**: ERROR требует вмешательства (алерты), WARNING — нет (мониторинг)

- **Логи как API**: Если на логах строится бизнес-логика, нужна схема, версионирование и обратная совместимость

- **Сбои → шторм логов**: Система логирования должна выдерживать всплески (rate limiting, backpressure)

- **Сэмплирование критично**: При высокой нагрузке — логируйте выборочно (0.1-10% событий)

- **Trace ID сквозь все сервисы**: Обязательно в микросервисах — единственный способ отследить запрос

- **Jaeger/Zipkin для waterfall**: Визуализация помогает найти узкие места быстрее логов

- **Определение зависших запросов**: Логируем начало операции, ищем те, у которых нет конца через N минут

- **Логи + Трейсы = Мощь**: trace_id в логах позволяет соединить детальность логов с визуализацией трейсов

- **Агрегация ошибок отдельно**: Sentry/Rollbar для ошибок, логи для событий — не смешивайте

- **Retention policy**: Детальные логи храним 7-30 дней, агрегированные — дольше

- **Начните с простого**: Структурированные JSON логи + Elasticsearch/Loki + базовая трассировка покрывают 90% потребностей

- **Мониторьте стоимость**: Если логирование стоит больше продукта — пересмотрите стратегию