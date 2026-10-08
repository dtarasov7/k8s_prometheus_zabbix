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
__version__ = "1.3.2"


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
    parsed_value = number(value)
    if parsed_value.is_integer():
        output = int(parsed_value)
    else:
        output = parsed_value
    print(output)


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
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        record = {"time": timestamp, "pid": os.getpid(), "event": event}
        record.update(fields)
        safe_record = self._redact(record)
        json_text = json.dumps(safe_record, ensure_ascii=False)
        line = json_text + "\n"
        try:
            descriptor = os.open(self.path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
            with os.fdopen(descriptor, "a", encoding="utf-8") as stream:
                stream.write(line)
        except OSError as exc:
            raise MonitoringError("Не удалось записать debug-журнал; проверьте путь и права") from exc

    def _redact(self, value):
        """Маскирует известные секреты в журнале. / Masks known secrets in the log.

        Args / Аргументы:
            value (object): JSON-совместимое значение / JSON-compatible value

        Returns / Возвращает:
            object: значение со скрытыми секретами / redacted value
        """
        if isinstance(value, str):
            for secret in sorted(self.secrets, key=len, reverse=True):
                if secret:
                    value = value.replace(secret, "[REDACTED]")
            return value
        if isinstance(value, dict):
            result = {}
            for key, item in value.items():
                result[key] = self._redact(item)
            return result
        if isinstance(value, list):
            result = []
            for item in value:
                result.append(self._redact(item))
            return result
        return value


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
        "url": "http://127.0.0.1:9090",
        "timeout": 10,
        "cache_ttl": 60,
        "cache_dir": f"/tmp/{service}-prometheus-cache",
        "username": "",
        "password": "",
        "password_file": "",
        "ca_file": "",
        "selector": {},
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
        config_dir = Path(path).resolve().parent
        for key in ("cache_dir", "password_file", "ca_file"):
            value = supplied.get(key)
            if isinstance(value, str) and value and not os.path.isabs(value):
                settings[key] = str(config_dir / value)
    # Приоритет: defaults < JSON < окружение. / Precedence: defaults < JSON < environment.
    for key in allowed - {"selector", "password"}:
        env_key = "PROM_" + key.upper()
        if env_key in os.environ:
            settings[key] = os.environ[env_key]
    for key in ("url", "cache_dir", "username", "password", "password_file", "ca_file"):
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
            password_path = Path(settings["password_file"])
            password_text = password_path.read_text(encoding="utf-8")
            settings["password"] = password_text.rstrip("\r\n")
        except (OSError, UnicodeError) as exc:
            raise MonitoringError("Не удалось прочитать файл пароля") from exc
    if bool(settings["username"]) != bool(settings["password"]):
        raise MonitoringError("Для Basic Auth нужны и username, и непустой пароль")
    if ":" in settings["username"]:
        raise MonitoringError("Имя пользователя Basic Auth не должно содержать ':'")
    selector = settings["selector"]
    if not isinstance(selector, dict):
        raise MonitoringError("selector должен содержать имена labels и строковые значения")
    for label, value in selector.items():
        valid_name = isinstance(label, str) and re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]*", label)
        if not valid_name or label == "__name__" or not isinstance(value, str):
            raise MonitoringError("selector должен содержать имена labels и строковые значения")
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
            unicode_password = json.dumps(password, ensure_ascii=False)[1:-1]
            escaped_password = json.dumps(password, ensure_ascii=True)[1:-1]
            encoded_credentials = base64.b64encode(credentials).decode("ascii")
            self.debug.secrets = [password, unicode_password, escaped_password, encoded_credentials]
        self.debug.write("client", url=self.settings["url"], selector=self.selector,
                         timeout=self.settings["timeout"], cache_ttl=self.settings["cache_ttl"])
        try:
            context = ssl.create_default_context(cafile=self.settings["ca_file"] or None)
        except (OSError, ssl.SSLError) as exc:
            raise MonitoringError("Не удалось загрузить CA для HTTPS") from exc
        redirect_handler = NoRedirect()
        https_handler = urllib.request.HTTPSHandler(context=context)
        self.opener = urllib.request.build_opener(redirect_handler, https_handler)

    def metric_query(self, metric, equals=None, not_equals=None):
        """Собирает selector метрики с дополнительными условиями. / Builds a metric selector with extra matchers.

        Args / Аргументы:
            metric (str): имя метрики / metric name
            equals (dict[str, str] | None): обязательные значения / required values
            not_equals (dict[str, str] | None): исключаемые значения / excluded values

        Returns / Возвращает:
            str: PromQL selector / PromQL selector

        Raises / Исключения:
            MonitoringError: label конфликтует с настройкой / label conflicts with configuration
        """
        labels = dict(self.selector)
        for name, value in (equals or {}).items():
            if name in labels and labels[name] != value:
                raise MonitoringError(f"selector: label {name} конфликтует с обязательным значением {value!r}")
            labels[name] = value
        for name in (not_equals or {}):
            if name in labels:
                raise MonitoringError(f"selector: label {name} конфликтует с исключающим условием")
        matchers = []
        for key, value in sorted(labels.items()):
            quoted_value = json.dumps(value, ensure_ascii=False)
            matchers.append(f"{key}={quoted_value}")
        for key, value in sorted((not_equals or {}).items()):
            quoted_value = json.dumps(value, ensure_ascii=False)
            matchers.append(f"{key}!={quoted_value}")
        if not matchers:
            return metric
        return metric + "{" + ",".join(matchers) + "}"

    def cache_path(self, query):
        """Строит ключ кэша с учётом источника и учётной записи. / Builds a source-scoped cache path.

        Args / Аргументы:
            query (str): PromQL-запрос / PromQL query

        Returns / Возвращает:
            Path: путь файла кэша / cache file path

        """
        # Разделяем источники и учётные записи. / Scope cache entries to source and credentials.
        # Секретов в содержимом нет. / Cache payloads never contain credentials.
        identity = [
            self.settings["url"],
            self.settings["username"],
            self.settings["password"],
            self.settings["ca_file"],
            query,
        ]
        serialized_identity = json.dumps(identity, ensure_ascii=False)
        identity_bytes = serialized_identity.encode("utf-8")
        digest = hashlib.sha256(identity_bytes).hexdigest()
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
            if not isinstance(item, dict):
                raise MonitoringError("Некорректная серия Prometheus")
            metric = item.get("metric")
            sample = item.get("value")
            if not isinstance(metric, dict) or not isinstance(sample, list) or len(sample) != 2:
                raise MonitoringError("Некорректная серия Prometheus")
            for label, value in metric.items():
                if not isinstance(label, str) or not isinstance(value, str):
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
            cache_path = self.cache_path(query)
            with cache_path.open(encoding="utf-8") as stream:
                payload = json.load(stream)
            cached_at = float(payload["timestamp"])
            age = time.time() - cached_at
            if not 0 <= age < self.settings["cache_ttl"]:
                return None
            result = payload["result"]
            return self.validate_result(result)
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

    def _fetch(self, query, url):
        """Получает JSON-ответ API по HTTP. / Fetches the JSON API response over HTTP.

        Args / Аргументы:
            query (str): PromQL-запрос / PromQL query
            url (str): полный адрес API / full API URL

        Returns / Возвращает:
            dict: разобранный ответ / parsed response

        Raises / Исключения:
            MonitoringError: ошибка HTTP, TLS или JSON / HTTP, TLS, or JSON error
        """
        request = urllib.request.Request(url, headers={"Accept": "application/json"})
        if self.settings["username"]:
            credentials_text = self.settings["username"] + ":" + self.settings["password"]
            credentials = credentials_text.encode("utf-8")
            encoded_credentials = base64.b64encode(credentials).decode("ascii")
            request.add_header("Authorization", "Basic " + encoded_credentials)
        # GET передаёт PromQL в URL; тело отсутствует. / GET carries PromQL in the URL, without a body.
        self.debug.write("http_request", method="GET", url=url, query=query, body=None,
                         basic_auth=bool(self.settings["username"]))
        try:
            with self.opener.open(request, timeout=self.settings["timeout"]) as response:
                raw = response.read()
                response_body = raw.decode("utf-8", errors="replace")
                self.debug.write("http_response", url=url, status=response.status, body=response_body)
                data = json.loads(raw)
        except urllib.error.HTTPError as exc:
            if self.debug.path:
                try:
                    error_bytes = exc.read()
                    raw_error = error_bytes.decode("utf-8", errors="replace")
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
        return data

    def query(self, query, use_cache=True):
        """Получает instant vector из кэша или HTTP API. / Fetches an instant vector from cache or HTTP API.

        Args / Аргументы:
            query (str): PromQL-запрос / query
            use_cache (bool): разрешить кэш / allow cache

        Returns / Возвращает:
            list[dict]: серии Prometheus / Prometheus series

        Raises / Исключения:
            MonitoringError: HTTP, TLS, JSON, warning или неверный ответ / HTTP, TLS, JSON, warning, or invalid response
        """
        # Пробелы в labels значимы. / Whitespace inside label values is significant.
        query = query.strip()
        parameters = urllib.parse.urlencode({"query": query})
        url = self.settings["url"] + "/api/v1/query?" + parameters
        self.debug.write("query", url=url, query=query)
        cache_enabled = use_cache and self.settings["cache_ttl"] > 0
        if cache_enabled:
            cached = self.read_cache(query)
            if cached is not None:
                self.debug.write("cache_hit", query=query, result=cached)
                return cached
        cache_event = "cache_miss" if cache_enabled else "cache_disabled"
        self.debug.write(cache_event, query=query)
        data = self._fetch(query, url)
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
        if len(result) != 1:
            raise MonitoringError("Некорректный ответ на проверку Prometheus")
        raw_value = result[0]["value"][1]
        value = number(raw_value)
        if value != 1:
            raise MonitoringError("Некорректный ответ на проверку Prometheus")
        return 1
