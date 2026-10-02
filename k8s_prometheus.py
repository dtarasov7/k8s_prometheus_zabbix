#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Единый скрипт для zabbix-agent / glabber.

Назначение:
  - получать метрики из Prometheus;
  - отдавать значения в формате, понятном zabbix-agent / glabber;
  - делать LLD discovery для нод и ingress service/status;
  - кэшировать запросы к Prometheus (кроме prometheus.health).

Важно:
  zabbix-agent запускает UserParameter каждый раз как отдельный процесс.
  Поэтому кэш хранится не в памяти, а в файлах на диске.

"""

"""
   Конфиг агента:

UserParameter=k8s.node.discovery,/etc/zabbix/scripts/k8s_prometheus.py node.discovery
UserParameter=k8s.node.condition[*],/etc/zabbix/scripts/k8s_prometheus.py node.condition "$1" "$2"

UserParameter=k8s.cluster.metric[*],/etc/zabbix/scripts/k8s_prometheus.py cluster "$1"

UserParameter=k8s.ingress.service_status.discovery,/etc/zabbix/scripts/k8s_prometheus.py ingress.service_status.discovery
UserParameter=k8s.ingress.status.service[*],/etc/zabbix/scripts/k8s_prometheus.py ingress.status.service "$1" "$2"

Для `/var/cache` лучше так:

mkdir -p /var/cache/k8s-prometheus
chown zabbix:zabbix /var/cache/k8s-prometheus

и в systemd/env или в скрипте указать:

PROM_CACHE_DIR=/var/cache/k8s-prometheus
"""


import json
import sys

from prometheus_common import (
    MonitoringError, PrometheusClient, __version__, extract_config, number, print_value,
)


_client = None


CLUSTER_QUERIES = {
    "oomkilled": (
        'sum(kube_pod_container_status_last_terminated_reason{reason="OOMKilled"})'
    ),
    "crashloop": (
        'sum(kube_pod_container_status_waiting_reason{reason="CrashLoopBackOff"})'
    ),
    "pending": (
        'sum(kube_pod_status_phase{phase="Pending"})'
    ),
    "failed": (
        'sum(kube_pod_status_phase{phase="Failed"})'
    ),
    "deployment_not_ready": (
        "sum("
        "  clamp_min("
        "    kube_deployment_spec_replicas"
        "    -"
        "    kube_deployment_status_ready_replicas,"
        "    0"
        "  )"
        ")"
    ),
    "pvc_not_bound": (
        'sum(kube_persistentvolumeclaim_status_phase{phase!="Bound"})'
    ),
}


NODE_CONDITIONS = {
    "Ready": "Ready",
    "MemoryPressure": "MemoryPressure",
    "DiskPressure": "DiskPressure",
    "PIDPressure": "PIDPressure",
}


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
    return number(result[0]["value"][1])


def node_discovery():
    """Выводит LLD нод. / Prints node discovery JSON.

    Returns / Возвращает:
        None: JSON в stdout / JSON written to stdout

    Raises / Исключения:
        MonitoringError: ошибка запроса / query error
    """
    result = prometheus_query("kube_node_info")

    nodes = []
    seen = set()

    for item in result:
        node = item.get("metric", {}).get("node")

        if node and node not in seen:
            seen.add(node)
            nodes.append({"{#NODE}": node})

    print(json.dumps({"data": nodes}, ensure_ascii=False))


def ingress_service_status_discovery():
    """Выводит LLD пар service/status. / Prints service/status discovery JSON.

    Returns / Возвращает:
        None: JSON в stdout / JSON written to stdout

    Raises / Исключения:
        MonitoringError: ошибка запроса / query error
    """
    query = """
        sum by (service, status) (
          increase(nginx_ingress_controller_request[1m])
        )
    """

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

    print(json.dumps({"data": items}, ensure_ascii=False))


def cluster_metric(name):
    """Выводит агрегат Kubernetes-кластера. / Prints a Kubernetes cluster aggregate.

    Args / Аргументы:
        name (str): ключ метрики / metric key

    Returns / Возвращает:
        None: число в stdout / number written to stdout

    Raises / Исключения:
        MonitoringError: ошибка запроса или значения / query or value error
    """
    query = CLUSTER_QUERIES[name]
    print_value(get_single_value(query))


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
    # Экранируем имя ноды как PromQL-строку. / Escape the node as a PromQL string.
    query = (
        'kube_node_status_condition'
        f'{{node={json.dumps(node)},condition={json.dumps(condition)},status="true"}}'
    )

    print_value(get_single_value(query))


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
    query = """
        sum by (service, status) (
          increase(nginx_ingress_controller_request[1m])
        )
    """

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

  k8s_prometheus.py [--config PATH] <command> [args]
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
        config_path, args = extract_config(sys.argv[1:])
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

        _client = PrometheusClient("k8s", config_path)
        if _client.selector:
            raise MonitoringError("selector поддерживается только kafka_prometheus.py")
        command = sys.argv[1]

        if command == "prometheus.health":
            if len(sys.argv) != 2:
                raise MonitoringError("prometheus.health не принимает аргументов")
            print_value(_client.health())
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
