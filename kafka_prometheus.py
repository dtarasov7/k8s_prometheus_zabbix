#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Метрики danielqsj/kafka_exporter из Prometheus для Zabbix."""

import argparse
import json
import sys

from prometheus_common import (
    DebugLog, MonitoringError, PrometheusClient, __version__, extract_config, number, print_value,
)


class KafkaMetrics:
    """Извлекает показатели Kafka из серий одного exporter. / Reads Kafka values from one exporter.
    """
    def __init__(self, client):
        """Проверяет selector и сохраняет клиент. / Validates the selector and stores the client.

        Args / Аргументы:
            client (PrometheusClient): источник метрик / metrics source

        Returns / Возвращает:
            None: объект готов / instance initialized

        Raises / Исключения:
            MonitoringError: selector не задан / missing selector
        """
        self.client = client
        if not client.selector:
            raise MonitoringError("Для Kafka задайте selector, выбирающий один exporter одного кластера")

    def rows(self, metric, labels):
        """Индексирует серии по предметным labels без дублей. / Indexes series by domain labels without duplicates.

        Args / Аргументы:
            metric (str): имя метрики / metric name
            labels (tuple[str]): ключевые labels / key labels

        Returns / Возвращает:
        dict[tuple, str]: значения по ключам / values by keys

        Raises / Исключения:
            MonitoringError: отсутствуют labels или есть дубли / missing labels or duplicates
        """
        result = self.client.query(self.client.metric_query(metric))
        rows = {}
        for item in result:
            metadata = item["metric"]
            if any(not metadata.get(label) for label in labels):
                raise MonitoringError("В метрике Kafka отсутствуют обязательные labels")
            key = tuple(metadata[label] for label in labels)
            # Дубли искажают агрегаты. / Duplicates would distort aggregation.
            if key in rows:
                raise MonitoringError("Дубли серий Kafka: selector должен выбирать один exporter одного кластера")
            rows[key] = item["value"][1]
        return rows

    @staticmethod
    def nonnegative(value):
        """Проверяет неотрицательное целое значение. / Validates a nonnegative integer value.

        Args / Аргументы:
            value (str | int | float): значение серии / sample value

        Returns / Возвращает:
            float: проверенное значение / validated value

        Raises / Исключения:
            MonitoringError: число неверно или отрицательно / invalid or negative number
        """
        value = number(value)
        if value < 0 or not value.is_integer():
            raise MonitoringError("Ожидалось неотрицательное целое значение Kafka; проверьте offsets")
        return value

    def require(self, rows, key, metric):
        """Требует наличия выбранной серии. / Requires a selected series to exist.

        Args / Аргументы:
            rows (dict): серии по ключам / indexed series
            key (tuple): нужный ключ / requested key
            metric (str): имя метрики для диагностики / metric name for diagnostics

        Returns / Возвращает:
            str: значение серии / sample value

        Raises / Исключения:
            MonitoringError: серия отсутствует / missing series
        """
        if key not in rows:
            query = self.client.metric_query(metric)
            self.client.debug.write("missing_series", query=query, key=list(key))
            hint = " Проверьте selector: его labels должны присутствовать у up." if metric == "up" else ""
            raise MonitoringError(f"Серия отсутствует: запрос {query}, ключ {key}.{hint}")
        return rows[key]

    def discovery(self, kind):
        """Формирует LLD для топиков или групп. / Builds topic or group LLD data.

        Args / Аргументы:
            kind (str): topic, group или group_topic / discovery kind

        Returns / Возвращает:
            dict: объект с полем data / object with data field

        Raises / Исключения:
            MonitoringError: неверные или повторные серии / malformed or duplicate series
        """
        if kind == "topic":
            labels, macros, metric = ("topic",), ("{#TOPIC}",), "kafka_topic_partitions"
        elif kind == "group":
            labels, macros, metric = ("consumergroup",), ("{#CONSUMERGROUP}",), "kafka_consumergroup_members"
        else:
            labels = ("consumergroup", "topic", "partition")
            macros, metric = ("{#CONSUMERGROUP}", "{#TOPIC}"), "kafka_consumergroup_lag"
        rows = self.rows(metric, labels)
        keys = sorted({key[:len(macros)] for key in rows})
        # Пустой успешный ответ даёт пустой LLD. / Empty successful data yields empty LLD.
        return {"data": [dict(zip(macros, key)) for key in keys]}

    def brokers(self):
        """Возвращает количество брокеров. / Returns broker count.

        Returns / Возвращает:
            float: число брокеров / broker count

        Raises / Исключения:
            MonitoringError: серия отсутствует или неверна / missing or invalid series
        """
        rows = self.rows("kafka_brokers", ())
        return self.nonnegative(self.require(rows, (), "kafka_brokers"))

    def exporter_up(self):
        """Читает доступность выбранного exporter. / Reads availability of the selected exporter.

        Returns / Возвращает:
            float: 0 или 1 / 0 or 1

        Raises / Исключения:
            MonitoringError: серия отсутствует или неверна / missing or invalid series
        """
        rows = self.rows("up", ())
        value = self.nonnegative(self.require(rows, (), "up"))
        if value not in (0, 1):
            raise MonitoringError("Метрика up должна быть 0 или 1")
        return value

    def group_members(self, group):
        """Возвращает число участников группы. / Returns consumer-group member count.

        Args / Аргументы:
            group (str): имя группы / group name

        Returns / Возвращает:
            float: число участников / member count

        Raises / Исключения:
            MonitoringError: серия отсутствует или неверна / missing or invalid series
        """
        rows = self.rows("kafka_consumergroup_members", ("consumergroup",))
        return self.nonnegative(self.require(rows, (group,), "kafka_consumergroup_members"))

    def topic_partitions(self, topic):
        """Возвращает число партиций топика. / Returns topic partition count.

        Args / Аргументы:
            topic (str): имя топика / topic name

        Returns / Возвращает:
            float: число партиций / partition count

        Raises / Исключения:
            MonitoringError: серия отсутствует или неверна / missing or invalid series
        """
        rows = self.rows("kafka_topic_partitions", ("topic",))
        return self.nonnegative(self.require(rows, (topic,), "kafka_topic_partitions"))

    def under_replicated(self, topic):
        """Считает недореплицированные партиции топика. / Counts under-replicated topic partitions.

        Args / Аргументы:
            topic (str): имя топика / topic name

        Returns / Возвращает:
            float: число партиций / partition count

        Raises / Исключения:
            MonitoringError: данных нет или индикатор неверен / missing data or invalid flag
        """
        rows = self.rows("kafka_topic_partition_under_replicated_partition", ("topic", "partition"))
        values = [self.nonnegative(value) for key, value in rows.items() if key[0] == topic]
        if not values:
            raise MonitoringError("Нет данных о репликации топика")
        if any(value not in (0, 1) for value in values):
            raise MonitoringError("Индикатор недорепликации должен быть 0 или 1")
        return sum(values)

    def lag(self, group, topic, operation):
        """Считает суммарный или максимальный lag. / Computes total or maximum lag.

        Args / Аргументы:
            group (str): группа / consumer group
            topic (str): топик / topic
            operation (str): sum или max / aggregation

        Returns / Возвращает:
            float: lag в offsets / lag in offsets

        Raises / Исключения:
            MonitoringError: нет серий или значение неверно / missing series or invalid value
        """
        rows = self.rows("kafka_consumergroup_lag", ("consumergroup", "topic", "partition"))
        values = [self.nonnegative(value) for key, value in rows.items() if key[:2] == (group, topic)]
        if not values:
            raise MonitoringError("Нет данных lag для группы и топика")
        return sum(values) if operation == "sum" else max(values)


def parser():
    """Создаёт парсер Kafka-команд. / Builds the Kafka command parser.

    Returns / Возвращает:
        ArgumentParser: настроенный парсер / configured parser

    """
    result = argparse.ArgumentParser(
        description="Kafka exporter → Prometheus → Zabbix. --config PATH и --debug-log PATH указываются перед командой.",
        epilog="Настройки и примеры UserParameter описаны в README.md.",
    )
    result.add_argument("--version", action="version", version=__version__)
    commands = result.add_subparsers(dest="command", required=True)
    for name in ("prometheus.health", "exporter.up", "brokers", "topic.discovery",
                 "group.discovery", "group_topic.discovery"):
        commands.add_parser(name)
    commands.add_parser("group.members").add_argument("group")
    for name in ("topic.partitions", "topic.under_replicated"):
        commands.add_parser(name).add_argument("topic")
    for name in ("group_topic.lag.sum", "group_topic.lag.max"):
        command = commands.add_parser(name)
        command.add_argument("group")
        command.add_argument("topic")
    return result


def main(argv=None):
    """Выполняет Kafka-команду и возвращает код завершения. / Runs a Kafka command and returns its exit code.

    Args / Аргументы:
        argv (list[str] | None): аргументы или sys.argv / arguments or sys.argv

    Returns / Возвращает:
        int: 0 при успехе, 1 при ошибке сбора / 0 on success, 1 on collection error

    Raises / Исключения:
        None: ошибки сбора пишутся в stderr / collection errors are printed to stderr
    """
    try:
        config_path, debug_path, args = extract_config(sys.argv[1:] if argv is None else argv)
        args = parser().parse_args(args)
        client = PrometheusClient("kafka", config_path, debug=DebugLog(debug_path))
        if args.command == "prometheus.health":
            print_value(client.health())
            return 0
        kafka = KafkaMetrics(client)
        if args.command.endswith(".discovery"):
            print(json.dumps(kafka.discovery(args.command.split(".")[0]), ensure_ascii=False))
            return 0
        if args.command == "exporter.up":
            value = kafka.exporter_up()
        elif args.command == "brokers":
            value = kafka.brokers()
        elif args.command == "group.members":
            value = kafka.group_members(args.group)
        elif args.command == "topic.partitions":
            value = kafka.topic_partitions(args.topic)
        elif args.command == "topic.under_replicated":
            value = kafka.under_replicated(args.topic)
        else:
            value = kafka.lag(args.group, args.topic, args.command.rsplit(".", 1)[1])
        print_value(value)
        return 0
    except MonitoringError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
