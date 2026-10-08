# Метрики Kubernetes и Kafka из Prometheus для Zabbix

Текущая версия: **1.3.2**. Предыдущие выпуски: **v1.3.1**, **v1.3.0**, **v1.2.0**,
**v1.1.0** и **v1.0.0**. Изменения описаны в [CHANGELOG.md](CHANGELOG.md).
Версия хранится в переменной `__version__` модуля `prometheus_common.py` и выводится
командой `--version` обоих скриптов.

Скрипты получают данные через Prometheus HTTP API и возвращают числа либо JSON
для Low-level discovery (LLD). Подключаться непосредственно к Kubernetes API или
Kafka из Zabbix не требуется.

Kubernetes и Kafka могут находиться в одном Prometheus или в разных. У каждого
скрипта свой конфигурационный файл, адрес сервера, учётная запись и каталог кэша.
Поддерживаются HTTP Basic Auth и HTTPS с проверкой сертификата.

## Состав проекта

| Файл | Назначение |
|---|---|
| `k8s_prometheus.py` | Существующие проверки Kubernetes: ноды, состояния контейнеров, PVC, ingress |
| `kafka_prometheus.py` | Проверки Kafka: lag, группы, топики, репликация и брокеры |
| `prometheus_common.py` | HTTP-клиент, авторизация, TLS, конфигурация и файловый кэш |
| `CHANGELOG.md` | История версий и несовместимых изменений |
| `examples/k8s.json` | Пример подключения без авторизации |
| `examples/kafka.json` | Пример отдельного Prometheus с Basic Auth |
| `examples/zabbix_agentd_prometheus.conf` | Все UserParameter для обоих скриптов |
| `tests/test_prometheus.py` | Автоматические проверки с локальными HTTP/HTTPS-серверами |

Схема работы:

```text
Zabbix server/proxy → Zabbix agent → k8s_prometheus.py   → Prometheus Kubernetes
                                 → kafka_prometheus.py → Prometheus Kafka
                                          │
                                 prometheus_common.py
                                 HTTP + TLS + Auth + кэш
```

Скрипты выполняются на машине с агентом. Адрес Prometheus должен быть доступен
именно с этой машины и от пользователя агента.

## Требования

- Python 3.8 или новее; сторонние Python-пакеты не нужны.
- Zabbix agent или agent2 с разрешёнными UserParameter.
- Prometheus с доступным `/api/v1/query`.
- Для Kafka — метрики `danielqsj/kafka_exporter` с labels `topic`, `partition`,
  `consumergroup` для соответствующих показателей.
- Для Kubernetes — метрики, используемые текущими запросами скрипта;
  ingress-запрос оставлен прежним: `nginx_ingress_controller_request`.

Фактические названия и labels проверьте в своём Prometheus. Другие exporter и
версии могут отдавать другой набор серий. Состав исходных Kafka-метрик описан
в [документации kafka_exporter](https://github.com/danielqsj/kafka_exporter#metrics).

## Установка

Команды ниже — пример установки на Linux из каталога проекта. В репозитории они
автоматически не выполняются. Учётная запись и группа `zabbix` должны существовать.

```bash
sudo install -d -o root -g root -m 0755 /etc/zabbix/scripts
sudo install -o root -g root -m 0755 k8s_prometheus.py kafka_prometheus.py /etc/zabbix/scripts/
sudo install -o root -g root -m 0644 prometheus_common.py /etc/zabbix/scripts/

sudo install -d -o root -g zabbix -m 0750 /etc/zabbix/prometheus
sudo install -o root -g zabbix -m 0640 examples/k8s.json /etc/zabbix/prometheus/k8s.json
sudo install -o root -g zabbix -m 0640 examples/kafka.json /etc/zabbix/prometheus/kafka.json

sudo install -d -o zabbix -g zabbix -m 0700 /var/cache/k8s-prometheus
sudo install -d -o zabbix -g zabbix -m 0700 /var/cache/kafka-prometheus
```

Все три Python-файла должны находиться рядом. Измените адреса и selector в
установленных JSON-файлах перед запуском. Примеры не содержат настоящих паролей.

## Настройка подключения

### Kubernetes без авторизации

`/etc/zabbix/prometheus/k8s.json`:

```json
{
  "url": "http://prometheus-k8s.example.org:9090",
  "timeout": 5,
  "cache_ttl": 60,
  "cache_dir": "/var/cache/k8s-prometheus",
  "selector": {
    "cluster": "production"
  }
}
```

### Kubernetes: фильтр по labels

В Kubernetes-конфиг можно добавить `selector`, чтобы получать метрики только
одного кластера или другого нужного набора серий:

```json
{
  "url": "http://prometheus-k8s.example.org:9090",
  "cache_dir": "/var/cache/k8s-prometheus",
  "selector": {"cluster": "production"}
}
```

`cluster` здесь пример. Выберите label, который реально есть у нужных
`kube_*`-метрик **и у ingress-метрики**, если используете ingress-команды.
Фильтр добавляется к исходной метрике до `sum`, вычитания реплик и расчёта
`increase`. Например, `cluster pending` выполнит:

```promql
sum(kube_pod_status_phase{cluster="production",phase="Pending"})
```

Пустой `selector` для Kubernetes разрешён и сохраняет прежние запросы.
`prometheus.health` всегда проверяет только API через `vector(1)` и не зависит
от `selector`. Если обязательное условие команды противоречит `selector`,
например `phase="Running"` для `cluster pending`, команда завершится ошибкой.
Отсутствующая серия в прежних числовых K8s-проверках по-прежнему даёт `0`,
поэтому перед включением триггеров проверьте наличие выбранных серий.

### Kafka с логином и паролем

`/etc/zabbix/prometheus/kafka.json`:

```json
{
  "url": "https://prometheus-kafka.example.org",
  "timeout": 5,
  "cache_ttl": 60,
  "cache_dir": "/var/cache/kafka-prometheus",
  "username": "zabbix",
  "password_file": "kafka.password",
  "selector": {
    "job": "kafka-exporter",
    "namespace": "kafka",
    "service": "kafka-exporter",
    "cluster": "production"
  }
}
```

Это учётные данные HTTP-доступа к Prometheus или его reverse proxy. Они не имеют
отношения к SASL-паролю Kafka: подключение exporter к Kafka настраивается отдельно.

Создать файл пароля без его записи в командную строку или историю shell можно так:

```bash
sudo python3 - <<'PY'
import getpass
import grp
import os

path = "/etc/zabbix/prometheus/kafka.password"
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o640)
with os.fdopen(fd, "w", encoding="utf-8") as stream:
    os.fchown(stream.fileno(), 0, grp.getgrnam("zabbix").gr_gid)
    os.fchmod(stream.fileno(), 0o640)
    stream.write(getpass.getpass("Пароль Prometheus: "))
PY
```

Команда предназначена для первоначального создания: существующий файл она не
перезаписывает. При смене пароля используйте защищённый редактор или систему
управления секретами и сохраните владельца `root:zabbix` и права `0640`.

Относительный `password_file` считается от каталога JSON-конфигурации. Конечные
переводы строк в файле пароля удаляются; остальные пробелы сохраняются. Файл
читается при каждом запуске процесса.

Вместо `password_file` можно использовать поле `password` непосредственно в JSON.
В этом случае JSON сам содержит секрет и должен быть защищён теми же правами.
Одновременно задавать непустые `password` и `password_file` нельзя. Пустой пароль
с именем пользователя считается ошибкой конфигурации.

Для Basic Auth используйте HTTPS: HTTP не защищает передаваемые учётные данные.
Вставлять пароль в URL, UserParameter или аргументы команды не нужно.

Если авторизация не требуется, уберите `username`, `password` и `password_file`.
Для K8s доступны точно такие же настройки авторизации.

### HTTPS и собственный CA

По умолчанию проверяются сертификат и имя сервера через системное хранилище CA.
Для внутреннего удостоверяющего центра добавьте в JSON:

```json
"ca_file": "/etc/zabbix/prometheus/company-ca.pem"
```

Это фрагмент объекта JSON, а не отдельный конфигурационный файл. Сам сертификат
должен быть в PEM-формате. Относительный путь также считается от каталога JSON.
Отключение проверки TLS не предусмотрено. Bearer/OAuth и клиентские сертификаты
в этой версии не реализованы.

HTTP-перенаправления не выполняются, чтобы не переслать заголовок авторизации
другому серверу. Если proxy возвращает redirect, укажите конечный HTTPS-адрес.
Допускается URL с префиксом пути, например `https://monitoring.example.org/prometheus`:
к нему добавляется `/api/v1/query`. Не добавляйте этот API-путь вручную.

### Один или несколько Prometheus

Если всё хранится в одном Prometheus, задайте одинаковый `url` в обоих файлах.
Если источники разные — задайте разные адреса и при необходимости разные пароли.

В Kafka-конфиге `selector` — объект с любым числом пар «имя label: точное
значение». Например:

```json
"selector": {
  "job": "kafka-exporter",
  "namespace": "kafka",
  "service": "kafka-exporter",
  "cluster": "production"
}
```

При запросе `up` это даёт
`up{cluster="production",job="kafka-exporter",namespace="kafka",service="kafka-exporter"}`. Поле
`job` необязательно: можно указать `service`, `cluster` или другой label,
который реально есть у ваших серий. Имя label должно быть допустимым именем
Prometheus, значение — строкой. Условия объединяются через логическое И;
регулярные выражения здесь не поддерживаются.

`instance` в Kubernetes часто содержит IP пода и меняется после его перезапуска.
Используйте устойчивые labels, которые присутствуют **и у Kafka-метрик, и у `up`**.
`namespace`, `service` и `cluster` в примере — возможные варианты, но их наличие
и значения нужно проверить в вашем Prometheus. Несколько exporter могут иметь
одинаковые значения этих labels; комбинация
условий должна выбирать один exporter одного Kafka-кластера.

Для Kafka пустой `selector` разрешён только для `prometheus.health`, который
проверяет API Prometheus без Kafka-фильтра. Остальные Kafka-команды требуют
непустой `selector`. Для Kubernetes фильтр необязателен; если он пуст и в
Prometheus есть несколько кластеров, агрегаты могут объединять их.

Скрипт Kafka отвергает повторяющиеся серии с одинаковыми предметными labels.
Например, две серии lag с одинаковыми `consumergroup`, `topic`, `partition`
считаются ошибкой. Он не выбирает случайную серию и не удваивает значение.
Это проверка дублей, а не автоматическое определение границ кластера: корректность
selector нужно проверить по реальным labels.

Для второго Kafka-кластера создайте второй JSON и отдельные UserParameter-ключи,
например `kafka.prod.*` и `kafka.test.*`, с фиксированными путями к соответствующим
конфигам. Динамический путь к конфигу в item key не требуется.

### Все параметры и приоритет

Приоритет: **значения по умолчанию → JSON → переменные окружения**.
`--config PATH` выбирает файл вместо `PROM_CONFIG` и должен стоять перед командой.
Сам файл автоматически из `/etc` не загружается.

| JSON-поле | Переменная окружения | По умолчанию |
|---|---|---|
| `url` | `PROM_URL` | `http://127.0.0.1:9090` |
| `timeout` | `PROM_TIMEOUT` | `10` секунд; строго больше нуля |
| `cache_ttl` | `PROM_CACHE_TTL` | `60` секунд; `0` отключает кэш |
| `cache_dir` | `PROM_CACHE_DIR` | `/tmp/k8s-prometheus-cache` или `/tmp/kafka-prometheus-cache` |
| `username` | `PROM_USERNAME` | Пустая строка: без авторизации |
| `password_file` | `PROM_PASSWORD_FILE` | Не задан |
| `password` | Нет | Не задан |
| `ca_file` | `PROM_CA_FILE` | Системное хранилище CA |
| `selector` | Нет | `{}`; для Kubernetes необязателен, для Kafka обязателен при сборе метрик |
| Выбор файла | `PROM_CONFIG` | Не задан |

`PROM_PASSWORD` не используется. Неизвестные поля JSON приводят к ошибке, чтобы
опечатки не меняли поведение незаметно. Относительные пути из JSON считаются от
его каталога; пути из окружения — от рабочего каталога процесса. Для переменных
окружения у агента используйте абсолютные пути.

Старый запуск K8s с переменными окружения продолжает работать:

```bash
PROM_URL=http://prometheus-k8s.example.org:9090 python3 k8s_prometheus.py cluster pending
```

Окружение интерактивного shell не передаётся автоматически systemd-сервису агента.
Для двух разных источников удобнее фиксированные `--config` в UserParameter.
Глобальный `PROM_URL` в сервисе переопределит URL обоих JSON-файлов.

## Версия

Оба скрипта используют `__version__ = "1.3.2"` из `prometheus_common.py`.
Отдельного файла версии нет. После установки проверьте:

```bash
python3 kafka_prometheus.py --version
python3 k8s_prometheus.py --version
```

Обе команды должны вывести `1.3.2` и завершиться с кодом `0`. Они не обращаются
к Prometheus и не требуют файла конфигурации. Порядок выпусков и изменения API перечислены в
[CHANGELOG.md](CHANGELOG.md).

## Команды Kafka

Общий формат:

```bash
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json COMMAND [ARGS]
```

| Команда | Результат |
|---|---|
| `prometheus.health` | `1` при успешном запросе `vector(1)` без кэша; иначе ошибка |
| `exporter.up` | Значение `up` для выбранного target: `0` или `1` |
| `brokers` | Количество брокеров, сообщённое exporter |
| `topic.discovery` | LLD топиков с `{#TOPIC}` |
| `group.discovery` | LLD групп с `{#CONSUMERGROUP}` |
| `group_topic.discovery` | LLD пар группа–топик с обеими макропеременными |
| `group.members GROUP` | Количество участников группы |
| `topic.partitions TOPIC` | Количество партиций топика |
| `topic.under_replicated TOPIC` | Число недореплицированных партиций топика |
| `group_topic.lag.sum GROUP TOPIC` | Сумма lag по возвращённым партициям |
| `group_topic.lag.max GROUP TOPIC` | Максимальный lag среди возвращённых партиций |

Примеры:

```bash
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json prometheus.health
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json exporter.up
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json brokers
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json topic.discovery
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json group.discovery
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json group_topic.discovery
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json group.members orders-workers
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json topic.partitions orders
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json topic.under_replicated orders
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json group_topic.lag.sum orders-workers orders
python3 kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json group_topic.lag.max orders-workers orders
```

При lag `12`, `7`, `0` на трёх партициях сумма равна `19`, максимум — `12`.
Это отставание по offsets, а не время в секундах.

Пример результата `group_topic.discovery`:

```json
{"data": [{"{#CONSUMERGROUP}": "orders-workers", "{#TOPIC}": "orders"}]}
```

### Используемые исходные серии

| Задача | Исходная метрика |
|---|---|
| Доступность target | `up` |
| Брокеры | `kafka_brokers` |
| Discovery топиков и число партиций | `kafka_topic_partitions` |
| Discovery групп и число участников | `kafka_consumergroup_members` |
| Недореплицированные партиции | `kafka_topic_partition_under_replicated_partition` |
| Discovery группа–топик, сумма и максимум lag | `kafka_consumergroup_lag` |

К имени каждой метрики добавляется selector, например:

```promql
kafka_consumergroup_lag{job="kafka-exporter",namespace="kafka"}
```

Весь набор серий сохраняется в кэш. Выбор группы/топика и агрегация выполняются
в Python; имена объектов из аргументов CLI не вставляются в PromQL. Discovery,
сумма и максимум lag используют один и тот же запрос и кэш.

`up=1` подтверждает успешный scrape, но не гарантирует полноту Kafka-метрик.
Сумма и максимум строятся по фактически возвращённым партициям: скрипт не
доказывает, что exporter увидел все ожидаемые партиции. Отсутствие отдельных серий
нужно расследовать по настройкам и журналам exporter. Топики без известных групп
не появятся в discovery группа–топик.

## Команды Kubernetes

Существующие команды и порядок аргументов сохранены:

```bash
python3 k8s_prometheus.py --config /etc/zabbix/prometheus/k8s.json prometheus.health
python3 k8s_prometheus.py --config /etc/zabbix/prometheus/k8s.json node.discovery
python3 k8s_prometheus.py --config /etc/zabbix/prometheus/k8s.json node.condition Ready worker-01
python3 k8s_prometheus.py --config /etc/zabbix/prometheus/k8s.json cluster pending
python3 k8s_prometheus.py --config /etc/zabbix/prometheus/k8s.json ingress.service_status.discovery
python3 k8s_prometheus.py --config /etc/zabbix/prometheus/k8s.json ingress.status.service 500 my-service
```

Cluster-метрики: `oomkilled`, `crashloop`, `pending`, `failed`,
`deployment_not_ready`, `pvc_not_bound`.
Состояния ноды: `Ready`, `MemoryPressure`, `DiskPressure`, `PIDPressure`.
Для `Ready` значение `1` означает готовность; для Pressure значение `1`
означает наличие проблемы. Ingress возвращает `increase(...[1m])` по паре
service/status; результат может быть дробным.

Изменение относительно старой версии: HTTP/TLS/JSON-ошибки, некорректные числа
и неоднозначный результат скалярной проверки больше не превращаются в `0`.
Успешный пустой ответ для прежних числовых K8s-проверок по-прежнему даёт `0`.
Поэтому дополнительно контролируйте наличие ожидаемых метрик в источнике:
`prometheus.health` проверяет API, но не наличие `kube_*` или ingress-серий.

## Подключение к Zabbix

### UserParameter

Полный готовый набор находится в `examples/zabbix_agentd_prometheus.conf`.
Пример установки для обычного агента:

```bash
sudo install -d -o root -g root -m 0755 /etc/zabbix/zabbix_agentd.d
sudo install -o root -g root -m 0644 examples/zabbix_agentd_prometheus.conf /etc/zabbix/zabbix_agentd.d/prometheus.conf
```

Убедитесь, что основной конфиг агента включает этот каталог:

```ini
Include=/etc/zabbix/zabbix_agentd.d/*.conf
```

Для agent2 используйте каталог из его `Include` и соответствующий основной конфиг.
Не оставляйте одновременно старые и новые определения одних и тех же ключей.

Показательные строки из примера:

```ini
UserParameter=kafka.brokers,/usr/bin/python3 /etc/zabbix/scripts/kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json brokers
UserParameter=kafka.group_topic.discovery,/usr/bin/python3 /etc/zabbix/scripts/kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json group_topic.discovery
UserParameter=kafka.group_topic.lag.sum[*],/usr/bin/python3 /etc/zabbix/scripts/kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json group_topic.lag.sum "$1" "$2"
```

Для `timeout: 5` задайте таймаут выполнения проверки с запасом, например 10 секунд
там, где он настраивается в вашей версии Zabbix. Общий процесс включает запуск
Python, TLS и файловые операции. HTTP timeout ограничивает ожидание сетевых
операций и не является строгим пределом времени всей команды.

UserParameter выполняется через shell, а аргументы подставляет агент. Оставьте
`UnsafeUserParameters=0`. Если имя группы содержит запрещённые Zabbix символы,
не отключайте защиту для этих команд: исключите такую группу фильтром LLD либо
адаптируйте способ передачи идентификаторов. Скрипт умеет обрабатывать строковые
имена, но это не отменяет ограничения транспорта UserParameter.
Поведение описано в [документации Zabbix UserParameter](https://www.zabbix.com/documentation/current/en/manual/config/items/userparameters).

После изменения конфигурации перезапустите используемый агент:

```bash
sudo systemctl restart zabbix-agent
```

Для agent2 имя службы обычно `zabbix-agent2`; используйте службу своей установки.

### Элементы данных и LLD

Для числовых Kafka-items используйте тип информации **Numeric (unsigned)**.
Стартовый интервал опроса — `60s`, discovery — `10m`. Для ingress K8s нужен
**Numeric (float)**. Тип сбора выбирайте `Zabbix agent` или `Zabbix agent (active)`
в соответствии с настройкой хоста.

Создайте обычные Kafka-items:

```text
kafka.prometheus.health
kafka.exporter.up
kafka.brokers
```

Создайте три правила LLD и прототипы:

| Правило LLD | Ключи прототипов items |
|---|---|
| `kafka.topic.discovery` | `kafka.topic.partitions["{#TOPIC}"]`, `kafka.topic.under_replicated["{#TOPIC}"]` |
| `kafka.group.discovery` | `kafka.group.members["{#CONSUMERGROUP}"]` |
| `kafka.group_topic.discovery` | `kafka.group_topic.lag.sum["{#CONSUMERGROUP}","{#TOPIC}"]`, `kafka.group_topic.lag.max["{#CONSUMERGROUP}","{#TOPIC}"]` |

JSON содержит готовые LLD-макросы. Дополнительный JSONPath для них не требуется
при использовании поддерживаемого Zabbix формата `{"data": [...]}`.

Фильтры включения/исключения топиков и групп удобно задать в правилах LLD.
Они управляют создаваемыми items, но не уменьшают объём запроса к Prometheus.
Если нужен меньший объём исходных данных, настраивайте сбор в exporter.

Не удаляйте потерянные ресурсы сразу после одного пустого discovery. Выберите
период хранения, например сутки, с учётом эксплуатации: исчезновение группы
или временно пустой ответ не должны немедленно удалять историю.

### Примеры триггеров

Ниже выражения для современного синтаксиса Zabbix. `Kafka via Prometheus` —
пример имени шаблона; замените его своим. Пороговые значения — начальные примеры,
которые нужно подобрать под нагрузку и допустимое отставание.

Макросы шаблона:

```text
{$KAFKA.BROKERS.EXPECTED} = 3
{$KAFKA.LAG.SUM.WARN} = 10000
{$KAFKA.LAG.MAX.WARN} = 5000
```

Суммарный lag превышал порог на всех полученных измерениях за последние 5 минут:

```text
min(/Kafka via Prometheus/kafka.group_topic.lag.sum["{#CONSUMERGROUP}","{#TOPIC}"],5m)>{$KAFKA.LAG.SUM.WARN}
```

Отстаёт отдельная партиция:

```text
min(/Kafka via Prometheus/kafka.group_topic.lag.max["{#CONSUMERGROUP}","{#TOPIC}"],5m)>{$KAFKA.LAG.MAX.WARN}
```

Сохраняются проблемы репликации:

```text
min(/Kafka via Prometheus/kafka.topic.under_replicated["{#TOPIC}"],3m)>0
```

Число брокеров меньше ожидаемого:

```text
max(/Kafka via Prometheus/kafka.brokers,3m)<{$KAFKA.BROKERS.EXPECTED}
```

Нет участников группы:

```text
max(/Kafka via Prometheus/kafka.group.members["{#CONSUMERGROUP}"],5m)=0
```

Последний триггер включайте только для групп, которые обязаны работать постоянно.
Для периодических задач отсутствие участников может быть штатным. Разные пороги
lag задавайте через отдельные шаблоны, переопределение макросов или контекстные
макросы, согласовав выражения с вашими правилами именования.

Нет успешного scrape exporter:

```text
max(/Kafka via Prometheus/kafka.exporter.up,3m)=0
```

Нет подтверждения доступности API:

```text
nodata(/Kafka via Prometheus/kafka.prometheus.health,3m)=1
```

Исчезла серия `up` или данные конкретной пары:

```text
nodata(/Kafka via Prometheus/kafka.exporter.up,3m)=1
nodata(/Kafka via Prometheus/kafka.group_topic.lag.sum["{#CONSUMERGROUP}","{#TOPIC}"],5m)=1
```

`min`/`max` оценивают существующие точки и сами по себе не доказывают непрерывность
сбора. Контроль `nodata` и unsupported items нужен отдельно. Аналогичную проверку
`nodata` добавьте для `k8s.prometheus.health`. Зависимости триггеров помогут
подавить вторичные уведомления о Kafka при общей недоступности источника.

## Ошибки и отсутствие данных

| Ситуация | Поведение |
|---|---|
| Успешное числовое значение | Число в stdout, код завершения `0` |
| Успешное discovery без объектов | `{"data": []}`, код `0` |
| Отсутствует запрошенная числовая Kafka-серия | Сообщение `ERROR` в stderr, пустой stdout, код `1` |
| Lag отрицательный, дробный, `NaN` или бесконечный | Ошибка; в `0` не преобразуется |
| Повторяющиеся серии Kafka | Ошибка: нужно уточнить selector |
| HTTP 401/403/5xx, таймаут, ошибка TLS или JSON | Ошибка без ложного числового результата |
| Prometheus вернул предупреждения API | Ошибка: ответ может быть неполным |
| Успешная пустая числовая K8s-проверка | `0` для совместимости |
| Ошибка чтения/записи кэша | Предупреждение в stderr; выполняется запрос/возвращается полученное значение |

Zabbix может объединять stdout и stderr команды. Поэтому `ERROR` не является
числом и числовой item не должен получать ложный ноль. Предупреждение о кэше
тоже может сделать результат непригодным для числового item: исправьте права
каталога или отключите кэш (`cache_ttl: 0`). Не добавляйте `2>/dev/null` или
преобразование всех ошибок в ноль в UserParameter.

`prometheus.health` возвращает `1` либо ошибку, а не `0`. Последнее сохранённое
значение может остаться `1`, поэтому для этой проверки нужен `nodata`, а не
условие `last(...)=0`.

В обычных сообщениях клиента не выводятся пароль, Authorization или тело ответа API.
При включённом `--debug-log` тело ответа записывается в файл с маскированием
известных клиенту пароля и Basic Auth token.
HTTP-код сохраняется для диагностики. Отсутствие метрик и ошибку авторизации
следует проверять отдельно.

## Кэширование и нагрузка

- Кэш файловый: его используют независимые процессы UserParameter.
- Ключ зависит от URL, учётных данных, настройки CA и полного текста PromQL.
  Разные источники и разные пароли не разделяют один ответ даже в общем каталоге.
- Пароль не записывается в содержимое кэша; имя файла — SHA-256 от контекста запроса.
  Каталог кэша всё равно должен быть закрыт от посторонних пользователей.
- Запись выполняется атомарно через временный файл с правами `0600` и `os.replace`.
- `cache_ttl: 0` отключает чтение и запись кэша.
- Просроченный ответ не возвращается при сбое Prometheus.
- Пока TTL не истёк, обычные items могут возвращать сохранённые значения даже после
  отказа источника. `prometheus.health` всегда выполняет настоящий запрос.
- При одновременном старте нескольких процессов на пустом или истёкшем кэше
  возможны несколько одинаковых HTTP-запросов. Межпроцессная блокировка не реализована.
- Новые URL, пароли или запросы создают новые файлы. Автоматической очистки старых
  ключей нет; при необходимости используйте правила обслуживания каталога кэша.

Итоговая задержка обнаружения зависит от scrape interval Prometheus, TTL и
интервала опроса Zabbix. Для более быстрой реакции уменьшите их согласованно.
Для production задайте `/var/cache/...`: каталог `/tmp` может очищаться системой.

## Отладочный журнал

Оба скрипта поддерживают `--debug-log PATH`. Ключ задаётся перед командой;
порядок относительно `--config` произвольный. Без этого ключа журнал не создаётся.

Пример для `exporter.up`, с отключением кэша на один запуск:

```bash
sudo -u zabbix env PROM_CACHE_TTL=0 /usr/bin/python3 /etc/zabbix/scripts/kafka_prometheus.py \
  --config /etc/zabbix/prometheus/kafka.json \
  --debug-log /var/cache/kafka-prometheus/debug.jsonl exporter.up
```

Пример для Kubernetes:

```bash
sudo -u zabbix /usr/bin/python3 /etc/zabbix/scripts/k8s_prometheus.py \
  --debug-log /var/cache/k8s-prometheus/debug.jsonl \
  --config /etc/zabbix/prometheus/k8s.json cluster pending
```

Родительский каталог журнала должен существовать и быть доступен пользователю
агента. Новый файл создаётся с правами `0600`, существующий дополняется.
Если запись невозможна, команда завершится с понятной ошибкой.

Формат — JSON Lines: одна JSON-запись на строку. Каждая запись содержит UTC-время,
PID и тип события `event`. Содержимое журнала:

| Событие | Что записывается |
|---|---|
| `start`, `client` | Версия, URL источника, selector, timeout и TTL |
| `query` | Полный URL с параметрами и исходный PromQL |
| `cache_hit` | Возвращённый результат из кэша; HTTP-запрос не выполнялся |
| `cache_miss`, `cache_disabled` | Причина перехода к HTTP-запросу |
| `http_request` | Метод, полный URL, PromQL, тело запроса и признак Basic Auth |
| `http_response` | HTTP-код и тело ответа, включая ответы с ошибкой HTTP |
| `http_error`, `request_error` | Тип/описание ошибки сети, TLS, JSON или redirect |
| `missing_series` | Запрос и ключ серии, которую не удалось найти |

Запросы используют **GET**: PromQL передаётся в параметре `query` URL, тела
запроса нет (`"body": null`). В `http_response.body` содержится строка исходного
ответа сервера, в том числе HTML вместо JSON. Заголовок Authorization не пишется;
известные клиенту пароль и Basic Auth token заменяются на `[REDACTED]`, если
встретились в ответе. Остальные данные ответа, включая labels, записываются.

Чтобы просмотреть JSON Lines в более удобном виде:

```bash
sudo -u zabbix python3 - <<'PYCODE'
import json
with open("/var/cache/kafka-prometheus/debug.jsonl", encoding="utf-8") as stream:
    for line in stream:
        print(json.dumps(json.loads(line), ensure_ascii=False, indent=2))
PYCODE
```

### Как разбирать отсутствие `exporter.up`

Команда запрашивает встроенную метрику Prometheus `up` с selector из Kafka-конфига:

```promql
up{job="kafka-exporter",namespace="kafka"}
```

Теперь сообщение об отсутствии серии содержит этот запрос и подсказку проверить
labels у `up`. Если ответ HTTP содержит `"result": []`, Prometheus выполнил запрос,
но подходящих серий нет. Сравните все условия `selector` с labels реально
существующей серии `up` в Prometheus. Labels самих Kafka-метрик
и labels `up` могут отличаться; особенно это касается labels внутри exporter.

Если видите `cache_hit` с пустым результатом, повторите запуск с `PROM_CACHE_TTL=0`,
как в примере выше. Debug-ключ сам по себе не отключает кэш.

Журнал не попадает в stdout и не меняет числовой формат ответа Zabbix. Для временной
диагностики через агент можно добавить `--debug-log PATH` перед командой в нужной
строке UserParameter. После отладки уберите ключ: ответы могут быть большими,
автоматической ротации журнала нет.

## Проверка после установки

Сначала выполните команды от имени пользователя агента:

```bash
sudo -u zabbix /usr/bin/python3 /etc/zabbix/scripts/kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json prometheus.health
sudo -u zabbix /usr/bin/python3 /etc/zabbix/scripts/kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json exporter.up
sudo -u zabbix /usr/bin/python3 /etc/zabbix/scripts/kafka_prometheus.py --config /etc/zabbix/prometheus/kafka.json topic.discovery
sudo -u zabbix /usr/bin/python3 /etc/zabbix/scripts/k8s_prometheus.py --config /etc/zabbix/prometheus/k8s.json node.discovery
```

Затем проверьте зарегистрированные UserParameter с основным конфигом агента:

```bash
sudo -u zabbix zabbix_agentd -c /etc/zabbix/zabbix_agentd.conf -t kafka.brokers
sudo -u zabbix zabbix_agentd -c /etc/zabbix/zabbix_agentd.conf -t 'kafka.group_topic.lag.sum[orders-workers,orders]'
```

Для agent2 используйте `zabbix_agent2` и его конфигурационный файл. Проверка через
реальный запущенный агент с разрешённого server/proxy:

```bash
zabbix_get -s AGENT_IP -k kafka.prometheus.health
zabbix_get -s AGENT_IP -k 'kafka.group_topic.lag.sum[orders-workers,orders]'
```

Если у агента настроен TLS, добавьте соответствующие параметры `zabbix_get`.
Проверьте Latest data и журнал агента. Скрипты не создают автоматически шаблоны,
items и триггеры в Zabbix: их нужно настроить по примерам выше.

### Типичные проблемы

| Симптом | Что проверить |
|---|---|
| HTTP 401 | Имя пользователя, пароль, endpoint и настройки Basic Auth proxy |
| HTTP 403 | Права этой учётной записи на API запросов |
| Ошибка TLS | CA, цепочку сертификатов, имя хоста в URL, срок действия сертификата |
| Redirect | Конечный URL, схему HTTPS и префикс пути reverse proxy |
| Нет Kafka-серии | Реальные labels, selector, настройки exporter и наличие группы/топика |
| Дубли серий | Несколько targets/реплик exporter или кластеров под одним selector |
| `exporter.up` пустой, но Kafka-метрики есть | Selector использует label, отсутствующий у `up` |
| Отрицательный lag | Состояние offsets и поведение конкретной версии exporter |
| Работает от root, не работает от zabbix | Права на JSON, пароль, CA, каталоги и кэш; окружение сервиса |
| Таймаут агента | Таймаут выполнения item, задержки Prometheus, доступность сети |
| `ModuleNotFoundError: prometheus_common` | Общий модуль должен лежать рядом со скриптами |

## Обновление существующей установки K8s

1. Сохраните используемую конфигурацию и старый скрипт штатными средствами резервного копирования.
2. Установите `prometheus_common.py` рядом с обновлённым `k8s_prometheus.py`.
3. Существующие команды и переменные `PROM_URL`, `PROM_TIMEOUT`, `PROM_CACHE_TTL`,
   `PROM_CACHE_DIR` можно оставить. При переходе на JSON уберите конфликтующие
   глобальные переменные окружения сервиса.
4. Добавьте `k8s.prometheus.health` и контроль отсутствия данных.
5. Проверьте триггеры: ошибки теперь видны как ошибки, а не как нулевые значения.
6. Отдельно установите Kafka-скрипт, его конфиг и UserParameter.

Старые файлы кэша не используются: формат ключа изменён. Удалять их для запуска
новой версии не требуется. Все изменённые и новые текстовые файлы проекта
сохраняются в UTF-8 с Unix-переводами строк, без BOM.

## Автоматические тесты

Из корня проекта:

```bash
python3 -m unittest discover -s tests -v
```

Тесты не обращаются к внешним Prometheus, Kafka или Zabbix. Они запускают локальные
HTTP-серверы, вызывают скрипты отдельными процессами и создают временные файлы
внутри проекта. Проверяются авторизация, HTTP-ошибки, таймаут, запрет redirect,
кэш между процессами, изоляция источников, discovery, агрегация, отсутствие и дубли
серий, прежние команды K8s. Для теста реального HTTPS нужен `openssl`; без него
только этот тест будет пропущен. TLS-тест проверяет доверенный CA, недоверенный
сертификат и несовпадение имени хоста.

Тесты с эмулятором не заменяют проверку labels вашего exporter и интеграции
с вашей версией Zabbix/Glaber. Экспортируемого шаблона Zabbix в проекте пока нет.
