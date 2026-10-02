#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Общий клиент Prometheus для отдельных процессов Zabbix UserParameter."""

import base64
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import ssl
import sys
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


# Single release source / Единый источник версии выпуска.
__version__ = "1.2.0"


class MonitoringError(Exception):
    """Ошибка сбора метрик. / Metric collection error.
    """


class NoRedirect(urllib.request.HTTPRedirectHandler):
    """Запрещает HTTP redirects с учётными данными. / Blocks credential-bearing HTTP redirects.
    """
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        """Останавливает перенаправление запроса. / Rejects a redirected request.

        Args / Аргументы:
            req (Request): исходный запрос / original request
            fp (IO): ответ / response
            code (int): HTTP-код / status
            msg (str): текст / message
            headers (Mapping): заголовки / headers
            newurl (str): новый адрес / destination

        Returns / Возвращает:
            None: всегда прерывает redirect / always stops the redirect

        Raises / Исключения:
            MonitoringError: сервер вернул redirect / server redirected
        """
        # Не передавать Authorization другому серверу. / Never forward credentials to another server.
        raise MonitoringError("Prometheus вернул redirect; укажите конечный URL")


def number(value):
    """Преобразует конечное числовое значение. / Parses a finite number.

    Args / Аргументы:
        value (str | int | float): значение метрики / metric value

    Returns / Возвращает:
        float: число / finite value

    Raises / Исключения:
        MonitoringError: тип или число некорректны / invalid type or non-finite value
    """
    if isinstance(value, bool):
        raise MonitoringError("Ожидалось число, а не логическое значение")
    try:
        result = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise MonitoringError("Метрика содержит нечисловое значение") from exc
    if not math.isfinite(result):
        raise MonitoringError("Метрика содержит NaN или Infinity")
    return result


def print_value(value):
    """Печатает число без лишнего десятичного нуля. / Prints a compact number.

    Args / Аргументы:
        value (str | int | float): значение / value

    Returns / Возвращает:
        None: число в stdout / writes number to stdout

    Raises / Исключения:
        MonitoringError: некорректное число / invalid number
    """
    value = number(value)
    print(int(value) if value.is_integer() else value)


class DebugLog:
    """Пишет диагностику в JSON Lines. / Writes diagnostics as JSON Lines."""

    def __init__(self, path=None):
        """Задаёт файл журнала. / Sets the optional log file.

        Args / Аргументы:
            path (str | None): путь файла / file path; None отключает журнал / disables logging.
        Returns / Возвращает:
            None: журнал настроен / log configured.
        Raises / Исключения:
            MonitoringError: файл недоступен / file is not writable.
        """
        self.path = path
        self.secrets = []
        if path:
            self.write("start", version=__version__)

    def write(self, event, **fields):
        """Дописывает событие, скрывая известные секреты. / Appends an event with known secrets masked.

        Args / Аргументы:
            event (str): тип события / event type.
            fields (dict): диагностические поля / diagnostic fields.
        Returns / Возвращает:
            None: запись в файл, если включено / file output when enabled.
        Raises / Исключения:
            MonitoringError: файл недоступен / file is not writable.
        """
        if not self.path:
            return
        record = {"time": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                  "pid": os.getpid(), "event": event, **fields}
        # Маскируем до сериализации, включая строковые тела ответов. / Mask before serialization, including response bodies.
        def redact(value):
            """Маскирует секреты в значениях. / Masks secrets in values.

            Args / Аргументы:
                value (object): JSON-совместимое значение / JSON-compatible value.
            Returns / Возвращает:
                object: значение со скрытыми секретами / value with secrets masked.
            """
            if isinstance(value, str):
                for secret in sorted(self.secrets, key=len, reverse=True):
                    if secret:
                        value = value.replace(secret, "[REDACTED]")
                return value
            if isinstance(value, dict):
                return {key: redact(item) for key, item in value.items()}
            if isinstance(value, list):
                return [redact(item) for item in value]
            return value
        line = json.dumps(redact(record), ensure_ascii=False) + "\n"
        try:
            descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
                stream.write(line)
        except OSError as exc:
            raise MonitoringError("Не удалось записать debug-журнал; проверьте путь и права") from exc


def extract_config(argv):
    """Разбирает общие CLI-ключи перед командой. / Parses shared options before the command.

    Args / Аргументы:
        argv (list[str]): аргументы без имени скрипта / arguments excluding script name.
    Returns / Возвращает:
        tuple: путь конфига, путь журнала, оставшиеся аргументы / config path, log path, remaining arguments.
    Raises / Исключения:
        MonitoringError: отсутствует значение ключа / missing option value.
    """
    args = list(argv)
    options = {"--config": os.environ.get("PROM_CONFIG"), "--debug-log": None}
    while args and args[0] in options:
        option = args.pop(0)
        if not args or args[0].startswith("--"):
            raise MonitoringError(f"После {option} нужен путь к файлу")
        options[option] = args.pop(0)
    return options["--config"], options["--debug-log"], args


def load_settings(service, path=None):
    """Собирает и проверяет настройки клиента. / Loads and validates client settings.

    Args / Аргументы:
        service (str): имя интеграции / integration name
        path (str | None): путь JSON / JSON path

    Returns / Возвращает:
        dict: настройки подключения / connection settings

    Raises / Исключения:
        MonitoringError: некорректный файл, URL, секрет или selector / invalid file, URL, secret, or selector
    """
    settings = {
        "url": "http://127.0.0.1:9090", "timeout": 10, "cache_ttl": 60,
        "cache_dir": f"/tmp/{service}-prometheus-cache",
        "username": "", "password": "", "password_file": "", "ca_file": "",
        "selector": {}, "label_name": "", "label_value": "",
    }
    allowed = set(settings)
    if path:
        try:
            with open(path, encoding="utf-8") as stream:
                supplied = json.load(stream)
        except (OSError, ValueError) as exc:
            raise MonitoringError("Не удалось прочитать JSON-конфигурацию") from exc
        if not isinstance(supplied, dict) or set(supplied) - allowed:
            raise MonitoringError("Конфигурация должна быть объектом с известными параметрами")
        settings.update(supplied)
        for key in ("cache_dir", "password_file", "ca_file"):
            value = supplied.get(key)
            if isinstance(value, str) and value and not os.path.isabs(value):
                settings[key] = str(Path(path).resolve().parent / value)
    # Приоритет: defaults < JSON < окружение. / Precedence: defaults < JSON < environment.
    for key in allowed - {"selector", "password"}:
        env_key = "PROM_" + key.upper()
        if env_key in os.environ:
            settings[key] = os.environ[env_key]
    for key in ("url", "cache_dir", "username", "password", "password_file", "ca_file",
                "label_name", "label_value"):
        if not isinstance(settings[key], str):
            raise MonitoringError(f"Параметр {key} должен быть строкой")
    for key in ("timeout", "cache_ttl"):
        settings[key] = number(settings[key])
    if settings["timeout"] <= 0 or settings["cache_ttl"] < 0:
        raise MonitoringError("timeout должен быть > 0, cache_ttl должен быть >= 0")
    if not settings["cache_dir"]:
        raise MonitoringError("Не задан cache_dir")
    try:
        url = urllib.parse.urlsplit(settings["url"])
        _ = url.port  # Проверка порта. / Validate port syntax and range.
    except ValueError as exc:
        raise MonitoringError("Некорректный URL Prometheus") from exc
    if (url.scheme not in ("http", "https") or not url.hostname or
            url.username is not None or url.password is not None or url.query or url.fragment):
        raise MonitoringError("Нужен HTTP(S) URL без пароля, query и fragment")
    settings["url"] = settings["url"].rstrip("/")
    if settings["password"] and settings["password_file"]:
        raise MonitoringError("Укажите только password или password_file")
    if settings["password_file"]:
        try:
            settings["password"] = Path(settings["password_file"]).read_text(encoding="utf-8").rstrip("\r\n")
        except (OSError, UnicodeError) as exc:
            raise MonitoringError("Не удалось прочитать файл пароля") from exc
    if bool(settings["username"]) != bool(settings["password"]):
        raise MonitoringError("Для Basic Auth нужны и username, и непустой пароль")
    if ":" in settings["username"]:
        raise MonitoringError("Имя пользователя Basic Auth не должно содержать ':'")
    selector = settings["selector"]
    if not isinstance(selector, dict) or any(
        not isinstance(key, str) or not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", key)
        or key == "__name__" or not isinstance(value, str)
        for key, value in selector.items()
    ):
        raise MonitoringError("selector должен содержать имена labels и строковые значения")
    label_name, label_value = settings["label_name"], settings["label_value"]
    if bool(label_name) != bool(label_value):
        raise MonitoringError("label_name и label_value должны быть заданы вместе и не быть пустыми")
    if label_name:
        if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", label_name) or label_name == "__name__":
            raise MonitoringError("label_name должен быть допустимым именем label Prometheus")
        if label_name in selector:
            raise MonitoringError("label_name уже указан в selector; оставьте его в одном месте")
        # Сохраняем прежний selector и добавляем выбранный label. / Merge the chosen label with legacy selectors.
        settings["selector"] = {**selector, label_name: label_value}
    return settings


class PrometheusClient:
    """Клиент instant query с Basic Auth, TLS и кэшем. / Instant-query client with Basic Auth, TLS, and cache.
    """
    def __init__(self, service, config_path=None, debug=None):
        """Создаёт клиент для выбранной интеграции. / Initializes a client for an integration.

        Args / Аргументы:
            service (str): имя интеграции / integration name
            config_path (str | None): путь JSON / JSON path
            debug (DebugLog | None): журнал запросов / request log

        Returns / Возвращает:
            None: заполненный клиент / initialized client

        Raises / Исключения:
            MonitoringError: неверная конфигурация или CA / invalid settings or CA
        """
        self.settings = load_settings(service, config_path)
        self.selector = self.settings["selector"]
        self.debug = debug or DebugLog()
        password = self.settings["password"]
        if password:
            credentials = (self.settings["username"] + ":" + password).encode("utf-8")
            self.debug.secrets = [password, json.dumps(password, ensure_ascii=False)[1:-1],
                                  json.dumps(password, ensure_ascii=True)[1:-1],
                                  base64.b64encode(credentials).decode("ascii")]
        self.debug.write("client", url=self.settings["url"], selector=self.selector,
                         timeout=self.settings["timeout"], cache_ttl=self.settings["cache_ttl"])
        try:
            context = ssl.create_default_context(cafile=self.settings["ca_file"] or None)
        except (OSError, ssl.SSLError) as exc:
            raise MonitoringError("Не удалось загрузить CA для HTTPS") from exc
        self.opener = urllib.request.build_opener(
            NoRedirect(), urllib.request.HTTPSHandler(context=context)
        )

    def metric_query(self, metric):
        """Добавляет selector к имени метрики. / Appends the configured label selector.

        Args / Аргументы:
            metric (str): имя метрики / metric name

        Returns / Возвращает:
            str: PromQL selector / PromQL selector

        """
        labels = ",".join(
            f"{key}={json.dumps(value, ensure_ascii=False)}"
            for key, value in sorted(self.selector.items())
        )
        return metric + ("{" + labels + "}" if labels else "")

    def cache_path(self, query):
        # Разделяем источники и учётные записи. / Scope cache entries to source and credentials.
        # Секретов в содержимом нет. / Cache payloads never contain credentials.
        """Строит ключ кэша с учётом источника и учётной записи. / Builds a source-scoped cache path.

        Args / Аргументы:
            query (str): PromQL-запрос / PromQL query

        Returns / Возвращает:
            Path: путь файла кэша / cache file path

        """
        identity = [self.settings[key] for key in ("url", "username", "password", "ca_file")]
        identity.append(query)
        digest = hashlib.sha256(json.dumps(identity, ensure_ascii=False).encode("utf-8")).hexdigest()
        return Path(self.settings["cache_dir"]) / (digest + ".json")

    @staticmethod
    def validate_result(result):
        """Проверяет структуру instant vector. / Validates an instant vector.

        Args / Аргументы:
            result (object): поле data.result / data.result field

        Returns / Возвращает:
            list[dict]: проверенные серии / validated series

        Raises / Исключения:
            MonitoringError: неверная структура серии / malformed series
        """
        if not isinstance(result, list):
            raise MonitoringError("Ожидался список серий Prometheus")
        for item in result:
            if (not isinstance(item, dict) or not isinstance(item.get("metric"), dict)
                    or not isinstance(item.get("value"), list) or len(item["value"]) != 2):
                raise MonitoringError("Некорректная серия Prometheus")
            if any(not isinstance(k, str) or not isinstance(v, str) for k, v in item["metric"].items()):
                raise MonitoringError("Некорректные labels Prometheus")
        return result

    def read_cache(self, query):
        """Читает только свежий корректный ответ. / Reads only fresh, valid cached data.

        Args / Аргументы:
            query (str): PromQL-запрос / PromQL query

        Returns / Возвращает:
            list[dict] | None: серии либо отсутствие кэша / series or cache miss

        Raises / Исключения:
            None: ошибки файла считаются промахом кэша / file errors are treated as cache misses
        """
        try:
            with self.cache_path(query).open(encoding="utf-8") as stream:
                payload = json.load(stream)
            age = time.time() - float(payload["timestamp"])
            if not 0 <= age < self.settings["cache_ttl"]:
                return None
            return self.validate_result(payload["result"])
        except FileNotFoundError:
            return None
        except (OSError, ValueError, TypeError, KeyError, MonitoringError):
            print("WARNING: кэш недоступен или повреждён; запрашиваем Prometheus", file=sys.stderr)
            return None

    def write_cache(self, query, result):
        """Атомарно сохраняет ответ Prometheus. / Atomically writes a Prometheus result.

        Args / Аргументы:
            query (str): PromQL-запрос / query
            result (list[dict]): серии / series

        Returns / Возвращает:
            None: результат записан или выдано предупреждение / cached or warning emitted

        Raises / Исключения:
            None: ошибки записи не прерывают сбор / write failures do not stop collection
        """
        temporary = None
        try:
            path = self.cache_path(query)
            path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", dir=path.parent,
                                             prefix=".prom-", delete=False) as stream:
                temporary = stream.name
                json.dump({"timestamp": time.time(), "result": result}, stream, ensure_ascii=False)
            # Замена целого файла атомарна. / Replace the complete file atomically.
            os.replace(temporary, path)
            temporary = None
        except OSError:
            print("WARNING: не удалось записать кэш Prometheus", file=sys.stderr)
        finally:
            if temporary:
                try:
                    os.unlink(temporary)
                except OSError:
                    print("WARNING: не удалось удалить временный файл кэша", file=sys.stderr)

    def query(self, query, use_cache=True):
        # Пробелы в labels значимы. / Whitespace inside label values is significant.
        """Получает instant vector из кэша или HTTP API. / Fetches an instant vector from cache or HTTP API.

        Args / Аргументы:
            query (str): PromQL-запрос / query
            use_cache (bool): разрешить кэш / allow cache

        Returns / Возвращает:
            list[dict]: серии Prometheus / Prometheus series

        Raises / Исключения:
            MonitoringError: HTTP, TLS, JSON, warning или неверный ответ / HTTP, TLS, JSON, warning, or invalid response
        """
        query = query.strip()
        url = self.settings["url"] + "/api/v1/query?" + urllib.parse.urlencode({"query": query})
        self.debug.write("query", url=url, query=query)
        cache_enabled = use_cache and self.settings["cache_ttl"] > 0
        if cache_enabled:
            cached = self.read_cache(query)
            if cached is not None:
                self.debug.write("cache_hit", query=query, result=cached)
                return cached
        self.debug.write("cache_miss" if cache_enabled else "cache_disabled", query=query)
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        if self.settings["username"]:
            credentials = (self.settings["username"] + ":" + self.settings["password"]).encode("utf-8")
            request.add_header("Authorization", "Basic " + base64.b64encode(credentials).decode("ascii"))
        # GET передаёт PromQL в URL; тело отсутствует. / GET carries PromQL in the URL, without a body.
        self.debug.write("http_request", method="GET", url=url, query=query, body=None,
                         basic_auth=bool(self.settings["username"]))
        try:
            with self.opener.open(request, timeout=self.settings["timeout"]) as response:
                raw = response.read()
                self.debug.write("http_response", url=url, status=response.status,
                                 body=raw.decode("utf-8", errors="replace"))
                data = json.loads(raw)
        except urllib.error.HTTPError as exc:
            if self.debug.path:
                try:
                    raw_error = exc.read().decode("utf-8", errors="replace")
                except (OSError, http.client.HTTPException) as read_error:
                    raw_error = f"<response read failed: {type(read_error).__name__}>"
                self.debug.write("http_response", url=url, status=exc.code, body=raw_error)
            exc.close()
            raise MonitoringError(f"Prometheus вернул HTTP {exc.code}") from exc
        except (OSError, urllib.error.URLError, http.client.HTTPException, ValueError) as exc:
            self.debug.write("http_error", url=url, error_type=type(exc).__name__, message=str(exc))
            raise MonitoringError("Не удалось получить ответ Prometheus: сеть, TLS или JSON") from exc
        except MonitoringError as exc:
            self.debug.write("request_error", url=url, message=str(exc))
            raise
        if not isinstance(data, dict) or data.get("status") != "success":
            raise MonitoringError("Prometheus сообщил об ошибке запроса")
        body = data.get("data")
        if not isinstance(body, dict) or body.get("resultType") != "vector":
            raise MonitoringError("Ожидался instant vector от Prometheus")
        if data.get("warnings"):
            raise MonitoringError("Prometheus вернул предупреждения; результат может быть неполным")
        result = self.validate_result(body.get("result"))
        if cache_enabled:
            self.write_cache(query, result)
        return result

    def health(self):
        """Проверяет API без кэша запросом vector(1). / Probes the API without cache.

        Returns / Возвращает:
            int: 1 при успехе / 1 on success

        Raises / Исключения:
            MonitoringError: источник недоступен или ответ неверен / unavailable source or invalid response
        """
        result = self.query("vector(1)", use_cache=False)
        if len(result) != 1 or number(result[0]["value"][1]) != 1:
            raise MonitoringError("Некорректный ответ на проверку Prometheus")
        return 1
