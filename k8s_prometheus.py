#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""Метрики Kubernetes из Prometheus для Zabbix. / Kubernetes metrics for Zabbix."""

import json
import sys

from prometheus_common import (
    DebugLog, MonitoringError, PrometheusClient, __version__, extract_config, number, print_value,
)


_client = None


# Простые агрегаты: метрика и обязательные labels. / Simple aggregates: metric and required labels.
CLUSTER_QUERIES = {
    "oomkilled": ("kube_pod_container_status_last_terminated_reason", {"reason": "OOMKilled"}),
    "crashloop": ("kube_pod_container_status_waiting_reason", {"reason": "CrashLoopBackOff"}),
    "pending": ("kube_pod_status_phase", {"phase": "Pending"}),
    "failed": ("kube_pod_status_phase", {"phase": "Failed"}),
    "deployment_not_ready": None,
    "pvc_not_bound": None,
}


NODE_CONDITIONS = {
    "Ready": "Ready",
    "MemoryPressure": "MemoryPressure",
    "DiskPressure": "DiskPressure",
    "PIDPressure": "PIDPressure",
}


def k8s_metric(metric, equals=None, not_equals=None):
    """Добавляет K8s selector к исходной метрике. / Applies the K8s selector to a source metric.

    Args / Аргументы:
        metric (str): имя метрики / metric name
        equals (dict[str, str] | None): обязательные labels / required labels
        not_equals (dict[str, str] | None): исключаемые labels / excluded labels

    Returns / Возвращает:
        str: PromQL selector метрики / metric selector

    Raises / Исключения:
        MonitoringError: конфликт labels / conflicting labels
    """
    global _client
    if _client is None:
        _client = PrometheusClient("k8s")
    return _client.metric_query(metric, equals=equals, not_equals=not_equals)


def ingress_query():
    """Строит общий запрос для ingress discovery и значений. / Builds the shared ingress query.

    Returns / Возвращает:
        str: агрегат по service/status / service/status aggregate

    Raises / Исключения:
        MonitoringError: конфликт labels / conflicting labels
    """
    metric_selector = k8s_metric("nginx_ingress_controller_request")
    return f"sum by (service, status) (increase({metric_selector}[1m]))"


def prometheus_query(query):
    """Выполняет запрос через общий клиент. / Queries the shared Prometheus client.

    Args / Аргументы:
        query (str): PromQL-запрос / PromQL query

    Returns / Возвращает:
        list[dict]: серии / series

    Raises / Исключения:
        MonitoringError: ошибка источника или ответа / source or response error
    """
    global _client
    if _client is None:
        _client = PrometheusClient("k8s")
    return _client.query(query)


def get_single_value(query):
    """Читает одно значение; пустой ответ даёт ноль. / Reads one value; empty success yields zero.

    Args / Аргументы:
        query (str): PromQL-запрос / PromQL query

    Returns / Возвращает:
        float: значение метрики / metric value

    Raises / Исключения:
        MonitoringError: неоднозначная или неверная серия / ambiguous or invalid series
    """
    result = prometheus_query(query)
    if not result:
        return 0.0
    if len(result) != 1:
        raise MonitoringError("Ожидалась одна серия Kubernetes; уточните источник")
    raw_value = result[0]["value"][1]
    return number(raw_value)


def node_discovery():
    """Выводит LLD нод. / Prints node discovery JSON.

    Returns / Возвращает:
        None: JSON в stdout / JSON written to stdout

    Raises / Исключения:
        MonitoringError: ошибка запроса / query error
    """
    query = k8s_metric("kube_node_info")
    result = prometheus_query(query)

    nodes = []
    seen = set()

    for item in result:
        labels = item.get("metric", {})
        node = labels.get("node")

        if node and node not in seen:
            seen.add(node)
            nodes.append({"{#NODE}": node})

    discovery = {"data": nodes}
    print(json.dumps(discovery, ensure_ascii=False))


def ingress_service_status_discovery():
    """Выводит LLD пар service/status. / Prints service/status discovery JSON.

    Returns / Возвращает:
        None: JSON в stdout / JSON written to stdout

    Raises / Исключения:
        MonitoringError: ошибка запроса / query error
    """
    query = ingress_query()

    result = prometheus_query(query)

    items = []
    seen = set()

    for item in result:
        metric = item.get("metric", {})
        service = metric.get("service")
        status = metric.get("status")

        if not service or not status:
            continue

        key = (service, status)

        if key in seen:
            continue

        seen.add(key)

        items.append({
            "{#SERVICE}": service,
            "{#STATUS}": status,
        })

    discovery = {"data": items}
    print(json.dumps(discovery, ensure_ascii=False))


def cluster_metric(name):
    """Выводит агрегат Kubernetes-кластера. / Prints a Kubernetes cluster aggregate.

    Args / Аргументы:
        name (str): ключ метрики / metric key

    Returns / Возвращает:
        None: число в stdout / number written to stdout

    Raises / Исключения:
        MonitoringError: ошибка запроса или значения / query or value error
    """
    if name == "deployment_not_ready":
        specified = k8s_metric("kube_deployment_spec_replicas")
        ready = k8s_metric("kube_deployment_status_ready_replicas")
        query = f"sum(clamp_min({specified} - {ready}, 0))"
    elif name == "pvc_not_bound":
        excluded_labels = {"phase": "Bound"}
        pvc = k8s_metric("kube_persistentvolumeclaim_status_phase", not_equals=excluded_labels)
        query = f"sum({pvc})"
    else:
        metric, labels = CLUSTER_QUERIES[name]
        metric_selector = k8s_metric(metric, equals=labels)
        query = f"sum({metric_selector})"
    value = get_single_value(query)
    print_value(value)


def node_condition(condition, node):
    """Выводит состояние ноды по condition. / Prints a node condition value.

    Args / Аргументы:
        condition (str): состояние / condition
        node (str): имя ноды / node name

    Returns / Возвращает:
        None: число в stdout / number written to stdout

    Raises / Исключения:
        MonitoringError: ошибка запроса или значения / query or value error
    """
    # Имя ноды экранируется в общем builder. / The shared builder escapes the node label.
    required_labels = {"node": node, "condition": condition, "status": "true"}
    query = k8s_metric("kube_node_status_condition", equals=required_labels)
    value = get_single_value(query)
    print_value(value)


def ingress_status_service(status, service):
    """Выводит число запросов пары status/service. / Prints request count for a status/service pair.

    Args / Аргументы:
        status (str): HTTP-статус / HTTP status
        service (str): сервис / service name

    Returns / Возвращает:
        None: число в stdout / number written to stdout

    Raises / Исключения:
        MonitoringError: ошибка запроса или значения / query or value error
    """
    query = ingress_query()

    result = prometheus_query(query)

    for item in result:
        metric = item.get("metric", {})

        if metric.get("service") != service:
            continue

        if metric.get("status") != status:
            continue

        value = item["value"][1]
        print_value(value)
        return

    print(0)


def usage():
    """Выводит справку CLI. / Prints CLI usage.

    Returns / Возвращает:
        None: справка в stdout / help written to stdout

    """
    print("""
Usage:

  k8s_prometheus.py [--config PATH] [--debug-log PATH] <command> [args]
  k8s_prometheus.py --version
  k8s_prometheus.py prometheus.health
  k8s_prometheus.py node.discovery
  k8s_prometheus.py node.condition <condition> <node>

  k8s_prometheus.py cluster <metric>

  k8s_prometheus.py ingress.service_status.discovery
  k8s_prometheus.py ingress.status.service <status> <service>

Cluster metrics:
  oomkilled
  crashloop
  pending
  failed
  deployment_not_ready
  pvc_not_bound

Node conditions:
  Ready
  MemoryPressure
  DiskPressure
  PIDPressure

Environment variables:
  PROM_CONFIG
  PROM_USERNAME
  PROM_PASSWORD_FILE
  PROM_CA_FILE
  PROM_URL
  PROM_TIMEOUT
  PROM_CACHE_TTL
  PROM_CACHE_DIR
""".strip())


def main():
    """Разбирает команду Kubernetes и выводит результат. / Dispatches a Kubernetes command and prints its result.

    Args / Аргументы:
        None: использует sys.argv / uses sys.argv

    Returns / Возвращает:
        None: значение в stdout или завершение с кодом 1 / stdout value or exit code 1

    Raises / Исключения:
        MonitoringError: преобразуется в stderr и код 1 / converted to stderr and exit code 1
    """
    global _client
    try:
        config_path, debug_path, args = extract_config(sys.argv[1:])
        sys.argv = [sys.argv[0]] + args
        if args in (["--help"], ["-h"]):
            usage()
            return
        if args in (["--version"], ["-V"]):
            print(__version__)
            return
        if len(sys.argv) < 2:
            usage()
            sys.exit(1)

        debug_log = DebugLog(debug_path)
        _client = PrometheusClient("k8s", config_path, debug=debug_log)
        command = sys.argv[1]

        if command == "prometheus.health":
            if len(sys.argv) != 2:
                raise MonitoringError("prometheus.health не принимает аргументов")
            health_value = _client.health()
            print_value(health_value)
            return

        if command == "node.discovery":
            if len(sys.argv) != 2:
                raise MonitoringError("node.discovery не принимает аргументов")
            node_discovery()
            return

        if command == "node.condition":
            if len(sys.argv) != 4:
                usage()
                sys.exit(1)

            condition = sys.argv[2]
            node = sys.argv[3]

            if condition not in NODE_CONDITIONS:
                raise MonitoringError("Неизвестное состояние ноды")

            node_condition(condition, node)
            return

        if command == "cluster":
            if len(sys.argv) != 3:
                usage()
                sys.exit(1)

            metric = sys.argv[2]

            if metric not in CLUSTER_QUERIES:
                raise MonitoringError("Неизвестная метрика кластера")

            cluster_metric(metric)
            return

        if command == "ingress.service_status.discovery":
            if len(sys.argv) != 2:
                raise MonitoringError("ingress.service_status.discovery не принимает аргументов")
            ingress_service_status_discovery()
            return

        if command == "ingress.status.service":
            if len(sys.argv) != 4:
                usage()
                sys.exit(1)

            status = sys.argv[2]
            service = sys.argv[3]

            ingress_status_service(status, service)
            return

        usage()
        sys.exit(1)

    except MonitoringError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
