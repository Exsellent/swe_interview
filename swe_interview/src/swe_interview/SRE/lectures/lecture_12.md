## О чем лекция
Лекция посвящена методам обнаружения сбоев в системах, классификации подходов к детектированию, метрикам RED и 4 Golden Signals, а также практическим сложностям определения того, что считать сбоем.

## Ключевые вопросы
- Как определить, что в системе произошел сбой?
- Когда сбой можно считать законченным?
- Какие существуют подходы к детектированию проблем?
- Почему недостаточно просто считать ошибки?
- Как учитывать сезонность и тренды при детектировании аномалий?

---

## Основная часть

### Философия определения сбоя

**Ключевой вопрос**: Что мы считаем сбоем?

**Это сложно**, потому что:
- ⚠️ Система замедлилась в 2 раза — сбой или нет?
- ⚠️ Долгая работа приводит к оттоку пользователей, но жалоб пока нет
- ⚠️ Баг в UI для товара с некорректными данными — сбой для этого товара или всей системы?
- ⚠️ Данные пишутся в Kafka, но никто не читает — технически все работает, но бизнес-функция сломана

**Примеры неочевидных сбоев**:

```
Сценарий 1: Медленная работа
- Время ответа выросло с 100ms до 500ms
- Технически все работает (HTTP 200)
- Но пользователи уходят → потеря выручки
Это сбой? 🤔

Сценарий 2: Kafka producer работает, consumer упал
- Метрики producer: все отлично, publish rate стабилен
- Но данные не обрабатываются → бизнес не работает
Это сбой? ✅ Да!

Сценарий 3: UI кнопка пропала на iOS
- Android, Web — работают
- iOS — кнопка не отображается → нет запросов
- Метрика Success не упала (просто меньше запросов)
Это сбой? ✅ Да!
```

**Вывод**: Определение сбоя требует **целостного взгляда на услугу**, а не только на технические метрики.

---

## Классы систем детектирования

### Класс 1: Прямое детектирование сбоев

**Принцип**: Активная проверка работоспособности системы.

#### 1.1. Обращения пользователей в колл-центр

**Как работает**: Пользователи сами сообщают о проблемах.

**Пример из Тинькофф Банка**:
```
13:45 - Первое обращение: "Не могу войти в приложение"
13:47 - Второе обращение: "Не открывается приложение"
13:48 - Третье обращение: "Приложение не работает"

→ Триггер: >3 обращения за 5 минут по одной проблеме
→ Алерт техподдержке/инженерам
```

**Преимущества**:
- ✅ Реальное мнение пользователей
- ✅ Обнаруживает проблемы, которые не видны в метриках

**Недостатки**:
- ⚠️ Реактивный подход (пользователи уже пострадали)
- ⚠️ Задержка (пока пользователь напишет в поддержку)

**Автоматизация**:
```python
# Мониторинг колл-центра
def monitor_support_tickets():
    recent_tickets = get_tickets_last_5_minutes()
    
    # Группируем по проблеме
    issues = group_by_issue(recent_tickets)
    
    for issue, count in issues.items():
        if count >= 3:
            alert(f"Spike in user reports: {issue} ({count} tickets)")
```

#### 1.2. Автотесты (Проберы / Prober / Synthetic Monitoring)

**Что это**: Автоматические тесты, которые **постоянно выполняются на проде**.

**Отличие от интеграционных тестов**:
| Интеграционные тесты | Проберы |
|----------------------|---------|
| Запускаются перед релизом | Запускаются постоянно (каждую минуту) |
| На стейджинге | На проде |
| Проверяют новый код | Проверяют работающую систему |

**Что проверяют проберы**:
- ✅ Критические user flows (login, payment, checkout)
- ✅ Интеграции с внешними системами
- ✅ Доступность сервисов
- ✅ Latency критических операций

**Best practices**:
- Используйте **специальные тестовые аккаунты** (не влияют на реальные данные)
- Помечайте тестовые транзакции (флаг `is_test=true`)
- Запускайте с разных географических локаций
- Тестируйте не только happy path, но и error handling

#### 1.3. Самодиагностика (Self-diagnostics)

**Принцип**: Софт сам диагностирует свое состояние и сообщает о проблемах.

**Health check endpoint**:
```python
@app.route('/health')
def health_check():
    """Детальная самодиагностика"""
    checks = {
        "database": check_database(),
        "redis": check_redis(),
        "kafka": check_kafka(),
        "disk_space": check_disk_space(),
        "memory": check_memory()
    }
    
    all_healthy = all(check["status"] == "ok" for check in checks.values())
    
    return jsonify({
        "status": "healthy" if all_healthy else "degraded",
        "checks": checks,
        "timestamp": datetime.utcnow().isoformat()
    }), 200 if all_healthy else 503

def check_database():
    """Проверка подключения к БД"""
    try:
        db.execute("SELECT 1")
        return {"status": "ok", "latency_ms": 5}
    except Exception as e:
        return {"status": "error", "error": str(e)}

def check_kafka():
    """Проверка Kafka consumer lag"""
    try:
        lag = get_consumer_lag()
        if lag > 10000:
            return {"status": "warning", "lag": lag}
        return {"status": "ok", "lag": lag}
    except Exception as e:
        return {"status": "error", "error": str(e)}

def check_disk_space():
    """Проверка свободного места"""
    free_percent = psutil.disk_usage('/').percent
    if free_percent < 10:
        return {"status": "critical", "free_percent": free_percent}
    elif free_percent < 20:
        return {"status": "warning", "free_percent": free_percent}
    return {"status": "ok", "free_percent": free_percent}
```

**Мониторинг health check**:
```yaml
# Prometheus scrape config
- job_name: 'health-checks'
  scrape_interval: 30s
  metrics_path: '/health'
  static_configs:
    - targets: ['service1:8080', 'service2:8080']
```

**Алерты на основе самодиагностики**:
```promql
# Алерт если health check не OK
health_check_status{status!="ok"} > 0
```

---

### Класс 2: Косвенное детектирование сбоев

**Принцип**: Анализ поведения системы и поиск аномалий.

#### 2.1. Поиск аномалий в статистике пользователей

**Что анализируем**:
- Резкое падение нагрузки
- Резкий рост нагрузки
- Изменение паттернов поведения

**Подход с ML**:
```python
# Аномалия: резкое падение запросов
def detect_request_drop(current_rps, historical_data):
    """
    Обучаем модель на исторических данных,
    проверяем текущее значение
    """
    # 1. Берем последние 7 дней (с учетом дня недели)
    same_weekday_data = get_historical_data(days=7, same_weekday=True)
    
    # 2. Предсказываем "нормальное" значение
    expected_rps = predict_normal_rps(same_weekday_data, current_time)
    
    # 3. Вычисляем отклонение
    deviation = (current_rps - expected_rps) / expected_rps
    
    # 4. Алерт если падение > 30%
    if deviation < -0.3:
        alert(f"Request rate drop: {current_rps} (expected {expected_rps})")
```

**Пример аномалий**:
```
Нормальная нагрузка: 10,000 RPS
Текущая нагрузка:    2,000 RPS
→ Падение на 80% → Алерт! 🚨

Возможные причины:
- Упал один из региональных серверов
- DDoS защита заблокировала легитимный трафик
- Проблема у CDN провайдера
```

#### 2.2. Изучение сезонности и трендов

**Временные ряды имеют паттерны**:
- **Сезонность**: день недели, время суток, праздники
- **Тренды**: постепенный рост/снижение
- **Циклы**: еженедельные, месячные

**Модели для анализа**:
```python
from prophet import Prophet  # Facebook Prophet

def predict_normal_behavior(historical_metrics):
    """Прогнозируем "нормальное" поведение"""
    
    # Готовим данные (timestamp + value)
    df = pd.DataFrame({
        'ds': timestamps,
        'y': values
    })
    
    # Обучаем модель с учетом сезонности
    model = Prophet(
        yearly_seasonality=True,
        weekly_seasonality=True,
        daily_seasonality=True
    )
    model.add_country_holidays(country_name='RU')
    model.fit(df)
    
    # Прогноз на следующий час
    future = model.make_future_dataframe(periods=60, freq='min')
    forecast = model.predict(future)
    
    return forecast

def detect_anomaly(current_value, forecast, threshold=0.95):
    """Проверяем, входит ли текущее значение в доверительный интервал"""
    lower_bound = forecast['yhat_lower'].iloc[-1]
    upper_bound = forecast['yhat_upper'].iloc[-1]
    
    if current_value < lower_bound or current_value > upper_bound:
        return True  # Аномалия!
    return False
```

**Визуализация**:
```
Requests Per Second
    │
15k │           ╱╲              ╱╲         (прогноз)
    │          ╱  ╲            ╱  ╲
10k │    ╱╲  ╱    ╲      ╱╲  ╱    ╲  ╱╲
    │   ╱  ╲╱      ╲    ╱  ╲╱      ╲╱  ╲
 5k │  ╱            ╲  ╱                 ╲
    │ ╱              ╲╱
  0 └──────────────────────────────────────
    Mon  Tue  Wed  Thu  Fri  Sat  Sun

❌ Аномалия: в среду RPS = 2k (ожидали 12k)
```

---

## RED метрики

**Ключевые метрики для мониторинга сервисов**:

### R — Request Rate (Запросы)

**Что измеряем**: Количество запросов в единицу времени.

```promql
# Запросов в секунду
rate(http_requests_total[5m])
```

**Зачем**:
- Понять нагрузку на систему
- Детектировать аномалии (резкий рост/падение)

### E — Error Rate (Ошибки)

**Что измеряем**: Процент ошибочных запросов.

```promql
# % ошибок
sum(rate(http_requests_total{status=~"5.."}[5m])) / 
sum(rate(http_requests_total[5m])) * 100
```

**Зачем**:
- Обнаружить деградацию сервиса
- SLA контроль (< 1% ошибок)

### D — Duration (Длительность)

**Что измеряем**: Latency запросов (медиана, p95, p99).

```promql
# p95 latency
histogram_quantile(0.95, rate(http_request_duration_bucket[5m]))
```

**Зачем**:
- Обнаружить замедление
- SLA контроль (p95 < 200ms)

---

## 4 Golden Signals (4 Золотых Сигнала)

**Расширение RED метрик от Google SRE**:

### 1. Traffic (Трафик)
= Request Rate из RED

### 2. Errors (Ошибки)
= Error Rate из RED

### 3. Latency (Задержка)
= Duration из RED

### 4. Saturation (Насыщенность)

**Что это**: Насколько система близка к пределу возможностей.

**Примеры метрик saturation**:
```promql
# CPU usage
avg(rate(cpu_seconds_total[5m])) * 100

# Memory usage
memory_used_bytes / memory_total_bytes * 100

# Database connections
db_connections_active / db_connections_max * 100

# Queue depth
kafka_consumer_lag
```

**Когда алертить**:
```
CPU > 80%         → Warning
CPU > 90%         → Critical
Memory > 85%      → Warning
DB connections > 90% → Critical
```

---

## Практический опыт: Нюансы детектирования

### 1. Считайте Success, а не только Errors

**Проблема с Errors**:
```python
# ❌ Плохо: считаем только ошибки
errors = count_errors()

# Проблема: что если ошибки не логируются?
# Проблема: что если весь сервис упал? errors = 0
```

**Решение — считать Success**:
```python
# ✅ Хорошо: считаем успешные операции
success = count_successful_requests()

# Если success упал → точно проблема
# Даже если errors не растут (потому что не логируются)
```

**Метрики**:
```promql
# Success rate
sum(rate(http_requests_total{status="200"}[5m]))

# Если success упал → инцидент
success_rate < 0.95 * normal_rate → Alert!
```

**Преимущества**:
- ✅ Уменьшаем False Negatives (пропущенные сбои)
- ✅ Уменьшаем False Positives (ложные тревоги)
- ✅ Четко понимаем: запрос **точно прошел успешно**

### 2. Считайте метрики в интервале начала запроса

**Проблема**:
```
Запрос начался:   14:00:58
Запрос завершился: 14:01:03 (длился 5 секунд)

Собираем метрики раз в минуту.

В какой интервал записать success/error?
- В интервал 14:00-14:01? (когда начался)
- В интервал 14:01-14:02? (когда завершился)
```

**Правильный подход**:
```python
# ✅ Записываем в интервал НАЧАЛА запроса
def record_request_metric(start_time, end_time, status):
    # Определяем минутный бакет по времени НАЧАЛА
    bucket = floor(start_time, resolution='1min')
    
    metrics.record(
        name="http_requests",
        value=1,
        timestamp=bucket,  # Время начала!
        labels={"status": status}
    )
```

**Почему это важно**:
- Если запросы длятся > 1 минуту, метрики размазываются по бакетам
- Сложно понять реальную нагрузку в конкретный момент

### 3. Saturation сложно определить

**Проблема**: Как понять, что система насыщена?

**Решение**: Нагрузочное тестирование.

```python
# Нагрузочный тест: ищем точку насыщения
def load_test():
    rps = 100
    while True:
        latency_p95 = send_requests(rps, duration=60)
        
        # Если latency начала расти → нашли saturation point
        if latency_p95 > baseline_latency * 1.5:
            print(f"Saturation at {rps} RPS")
            break
        
        rps += 100

# Пример результата:
# Baseline latency: 50ms
# При 5000 RPS: latency = 55ms ✓
# При 8000 RPS: latency = 75ms ⚠️ Начало деградации
# При 10000 RPS: latency = 150ms 🚨 Saturation!

# Устанавливаем алерт: если RPS > 8000 → Warning
```

### 4. Определение успешности операции — сложная задача

**Бизнес-определение успеха**:
```
Операция "Покупка товара" успешна, если:
1. ✅ Оплата прошла
2. ✅ Заказ создан в БД
3. ✅ Инвентарь обновлен
4. ✅ Email подтверждение отправлено
5. ✅ Событие записано в аналитику

Если хотя бы одно НЕ выполнилось → операция НЕ успешна
(даже если HTTP вернул 200!)
```

**Как гарантировать регистрацию success**:
```python
@transaction
def process_order(order_id):
    try:
        # 1. Оплата
        payment = charge_payment(order.id)
        
        # 2. Создание заказа
        order = create_order(payment.id)
        
        # 3. Обновление инвентаря
        update_inventory(order.items)
        
        # 4. Отправка email (асинхронно, но проверяем добавление в очередь)
        queue_email_confirmation(order.id)
        
        # 5. Запись в аналитику
        track_event("order_completed", order.id)
        
        # ✅ ТОЛЬКО ЕСЛИ ВСЁ ПРОШЛО → записываем success
        metrics.record("order.success", 1, order_id=order.id)
        
        # Commit транзакции
        db.commit()
        
        return {"status": "success", "order_id": order.id}
        
    except Exception as e:
        # ❌ Откат и запись failure
        db.rollback()
        metrics.record("order.failure", 1, error=str(e))
        # Не возвращаем 200! Возвращаем 500
        raise
```

---

## Реальные проблемы детектирования

### Проблема 1: Success резко вырос

**Сценарий**:
```
Обычная нагрузка: 10,000 success/min
Вдруг:           50,000 success/min
```

**Причины (не обязательно плохо)**:
- 🎉 Вирусная новость упомянула ваш продукт
- 📺 Реклама по ТВ
- 🤖 Легитимный скрейпер (Google bot)
- 🚀 Успешная маркетинговая кампания

**Как различить**:
```python
# Проверяем корреляцию с другими метриками
if success_rate_up and error_rate_normal and latency_normal:
    # Скорее всего organic growth → OK
    pass
else:
    # Что-то не так
    alert("Suspicious spike in success rate")
```

### Проблема 2: Success не изменился, но ошибки есть

**Сценарий**:
```
Success: стабильно 10,000/min
Errors:  вдруг 2,000/min

Total requests = 12,000/min (было 10,000)
```

**Что произошло**:
- Рост числа запросов
- Но часть из них ошибочные
- Success не упал (даже вырос!)

**Детектирование**:
```promql
# Нужно смотреть на error rate, а не на success
error_rate = errors / total_requests

# Алерт
error_rate > 0.05 → Alert!  # > 5% ошибок
```

### Проблема 3: Success упал, но система работает

**Сценарий**:
```
Success упал с 10,000/min до 8,000/min
```

**Причины (не баг)**:
- 📱 Кнопка пропала на некоторых устройствах (iOS bug)
- 🌐 Проблема у CDN в одном регионе
- 🏢 Крупный B2B клиент временно приостановил интеграцию

**Как проверить**:
```python
# Сегментация по устройствам
success_by_platform = {
    "iOS": get_success_count(platform="iOS"),
    "Android": get_success_count(platform="Android"),
    "Web": get_success_count(platform="Web")
}

# iOS упал на 90% → проблема в iOS приложении
if success_by_platform["iOS"] < baseline_ios * 0.5:
    alert("iOS success rate dropped significantly")
```

### Проблема 4: Нагрузка просела, но все работает

**Сценарий**:
```
Success упал с 10,000/min до 7,000/min
Но: error_rate стабилен, latency в норме
```

**Причина**:
```
Крупный B2B клиент (30% трафика) столкнулся с проблемой
у себя и перестал отправлять запросы
```

**Детектирование**:
```python
# Анализ по клиентам
success_by_client = group_by_client(success_metrics)

largest_clients = top_10_clients(success_by_client)

for client in largest_clients:
    current = success_by_client[client]
    baseline = get_baseline(client)
    
    if current < baseline * 0.5:
        notify_account_manager(
            f"Major client {client} traffic dropped 50%"
        )
```

---

## Рекомендации ЦБ (Центральный Банк)

**Контекст**: Регулятор требует отчетности о сбоях.

### Требование: Прогноз сезонности

**Что нужно**:
1. Построить прогноз: какой была бы нагрузка, **если бы все шло нормально**
2. Сравнить фактическую нагрузку с прогнозом
3. Если **успешных операций меньше прогноза** → это сбой → отчитаться

**Реализация**:
```python
from prophet import Prophet

def build_seasonality_model(historical_data):
    """Обучаем модель на исторических данных"""
    # Берем последние 90 дней
    df = get_historical_success_count(days=90)
    
    model = Prophet(
        yearly_seasonality=False,  # Нет года данных
        weekly_seasonality=True,   # Учитываем день недели
        daily_seasonality=True     # Учитываем время суток
    )
    
    # Добавляем праздники
    model.add_country_holidays(country_name='RU')
    
    # Добавляем внешние факторы (если есть)
    df['marketing_spend'] = get_marketing_spend()
    model.add_regressor('marketing_spend')
    
    model.fit(df)
    return model

def detect_incident_for_regulator():
    """Детектирование по требованиям ЦБ"""
    
    # 1. Прогноз "нормального" количества операций
    forecast = model.predict(current_period)
    expected_success = forecast['yhat'].iloc[-1]
    expected_lower_bound = forecast['yhat_lower'].iloc[-1]
    
    # 2. Фактическое количество
    actual_success = get_current_success_count()
    
    # 3. Сравнение
    if actual_success < expected_lower_bound:
        # Фактических операций меньше прогноза → СБОЙ
        deviation_percent = (expected_success - actual_success) / expected_success * 100
        
        report_incident_to_regulator({
            "type": "service_degradation",
            "expected": expected_success,
            "actual": actual_success,
            "deviation": f"{deviation_percent:.1f}%",
            "timestamp": datetime.utcnow().isoformat()
        })
        
        return True  # Инцидент
    
    return False  # Все ОК
```

**Визуализация**:
```
Successful Operations

30k │              ╱‾‾‾╲              ╱‾‾‾╲
    │            ╱╲    ╱╲            ╱     ╲    (прогноз + доверительный интервал)
20k │          ╱╲  ╲  ╱  ╲          ╱       ╲
    │    ╱‾╲ ╱  ╲  ╲╱    ╲    ╱‾╲ ╱
10k │   ╱   ╲╱    ╲       ╲  ╱   ╲╱
    │  ╱              ✕✕✕✕✕✕✕✕             (факт)
  0 └──────────────────────────────────────
    Mon  Tue  Wed  Thu  Fri  Sat  Sun

✕✕✕ - фактическое значение ниже нижней границы
→ СБОЙ для ЦБ → Нужен отчет
```

### Сложность: Правильный прогноз

**Проблемы**:
- 📊 Нужны качественные исторические данные (минимум 3 месяца)
- 🎯 Нужно учитывать множество факторов (праздники, маркетинг, внешние события)
- ⚖️ Баланс между чувствительностью и ложными тревогами
- 🔄 Модель нужно переобучать регулярно (каждую неделю)

**Best practices**:
```python
# 1. Несколько моделей для перепроверки
models = [
    ProphetModel(),
    ARIMAModel(),
    LSTMModel()
]

predictions = [model.predict(current_time) for model in models]

# Консенсус: если 2+ модели говорят "аномалия" → инцидент
if sum(p.is_anomaly for p in predictions) >= 2:
    report_incident()

# 2. Адаптивные пороги
threshold = adaptive_threshold(
    historical_false_positive_rate,
    target_false_positive_rate=0.01  # < 1% ложных тревог
)

# 3. Контекстуальные проверки
if is_anomaly and not is_known_event(current_time):
    # Не праздник, не запланированное обслуживание → реальный сбой
    report_incident()
```

---

## Выводы и инсайты

- **Определение сбоя — философский вопрос**: Нужен **целостный взгляд на услугу**, а не только технические метрики

- **Прямое детектирование > Косвенное**: Проберы и health checks обнаруживают проблемы быстрее ML анализа

- **Колл-центр — ценный источник**: Пользователи часто сообщают о проблемах раньше automated monitoring

- **Тестируйте на проде**: Проберы (synthetic monitoring) — единственный способ узнать, работает ли система **прямо сейчас**

- **Success > Errors**: Считайте успешные операции, а не ошибки — меньше False Negatives

- **Kafka producer работает ≠ система работает**: Проверяйте end-to-end flow, а не только части

- **Считайте метрики в момент начала запроса**: Иначе долгие запросы размоются по временным бакетам

- **Saturation требует нагрузочного тестирования**: Без тестов невозможно знать предел системы

- **Сегментируйте метрики**: По платформам, регионам, клиентам — для точной локализации проблем

- **Сезонность критична**: Без учета трендов и сезонности — море ложных алертов

- **ML для аномалий, но с умом**: Prophet, ARIMA работают, но нужна адаптация к специфике бизнеса

- **Регуляторные требования серьезны**: ЦБ требует прогнозов и отчетности — автоматизируйте заранее

- **4 Golden Signals покрывают 90%**: Traffic, Errors, Latency, Saturation — начните с них

- **Определение "успеха" нетривиально**: Для каждой операции продумайте, что означает "успешно выполнена"

- **Множественные модели для важных метрик**: Консенсус нескольких моделей → меньше ошибок в детектировании









