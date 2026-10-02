"""Контракт CLI, реальные HTTP-запросы и файловый кэш без внешних сервисов."""

import base64
from concurrent.futures import ThreadPoolExecutor
from contextlib import redirect_stderr
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
from pathlib import Path
import shutil
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from unittest.mock import patch
import urllib.parse

from prometheus_common import MonitoringError, PrometheusClient, __version__, load_settings


ROOT = Path(__file__).resolve().parents[1]


def sample(value, **labels):
    return {"metric": labels, "value": [time.time(), str(value)]}


def vector(rows):
    return {"status": "success", "data": {"resultType": "vector", "result": rows}}


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        return

    def do_GET(self):
        query = urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query).get("query", [""])[0]
        self.server.calls.append((query, self.headers.get("Authorization")))
        if self.server.auth and self.headers.get("Authorization") != self.server.auth:
            self.send_response(401)
            self.end_headers()
            return
        if self.server.redirect:
            self.send_response(302)
            self.send_header("Location", self.server.redirect)
            self.end_headers()
            return
        self.send_response(self.server.status)
        if getattr(self.server, "truncate", False):
            self.send_header("Content-Length", "100000")
        self.end_headers()
        response = self.server.responses.get(query, self.server.default)
        if callable(response):
            response = response()
        body = response if isinstance(response, bytes) else json.dumps(response).encode()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            # Клиент намеренно завершает соединение в тесте таймаута.
            return


class IntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix=".test-prometheus-", dir=ROOT)
        self.addCleanup(self.tmp.cleanup)
        self.directory = Path(self.tmp.name)
        clean_env = {k: v for k, v in os.environ.items() if not k.startswith("PROM_")}
        self.env_patch = patch.dict(os.environ, clean_env, clear=True)
        self.env_patch.start()
        self.addCleanup(self.env_patch.stop)
        self.server = self.start_server()
        self.url = f"http://127.0.0.1:{self.server.server_port}"
        self.config = self.write_config()

    def start_server(self, context=None):
        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        server.calls, server.responses = [], {}
        server.auth, server.redirect = None, None
        server.status, server.default = 200, vector([sample(1)])
        if context:
            server.socket = context.wrap_socket(server.socket, server_side=True)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        return server

    def write_config(self, name="config.json", **overrides):
        settings = {"url": self.url, "cache_dir": str(self.directory / "cache"),
                    "cache_ttl": 60, "timeout": 2,
                    "selector": {"job": "kafka-exporter"}}
        settings.update(overrides)
        path = self.directory / name
        path.write_text(json.dumps(settings), encoding="utf-8")
        return path

    def client(self, path=None):
        return PrometheusClient("kafka", str(path or self.config))

    def respond(self, metric, rows):
        self.server.responses[self.client().metric_query(metric)] = vector(rows)

    def cli(self, *args, script="kafka_prometheus.py", config=None):
        return subprocess.run(
            [sys.executable, str(ROOT / script), "--config", str(config or self.config), *args],
            text=True, capture_output=True, cwd=ROOT, timeout=10,
        )

    def assert_failure(self, result, text=None):
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("ERROR:", result.stderr)
        if text:
            self.assertIn(text, result.stderr)

    def test_version_matches_code_without_prometheus(self):
        expected = __version__
        self.assertEqual(expected, "1.3.1")
        self.server.status = 503
        for script in ("kafka_prometheus.py", "k8s_prometheus.py"):
            result = self.cli("--version", script=script)
            self.assertEqual((result.returncode, result.stdout, result.stderr),
                             (0, expected + "\n", ""))
        self.assertEqual(self.server.calls, [])

    def test_debug_empty_up_records_request_response_and_selector(self):
        log = self.directory / "debug.jsonl"
        self.respond("up", [])
        query = self.client().metric_query("up")
        result = self.cli("--debug-log", str(log), "exporter.up")
        self.assert_failure(result, query)
        self.assertIn("selector", result.stderr)
        records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
        request = next(row for row in records if row["event"] == "http_request")
        self.assertEqual(request["method"], "GET")
        self.assertIsNone(request["body"])
        self.assertEqual(request["query"], query)
        self.assertEqual(urllib.parse.parse_qs(urllib.parse.urlsplit(request["url"]).query), {"query": [query]})
        response = next(row for row in records if row["event"] == "http_response")
        self.assertEqual(response["status"], 200)
        self.assertEqual(json.loads(response["body"]), vector([]))
        self.assertEqual(records[-1]["event"], "missing_series")
        self.assertEqual(log.stat().st_mode & 0o777, 0o600)
        # Второй процесс использует кэш и дописывает файл. / Next process appends a cache hit.
        result = self.cli("--debug-log", str(log), "exporter.up")
        self.assert_failure(result, query)
        records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(sum(row["event"] == "http_request" for row in records), 1)
        self.assertEqual(next(row for row in records if row["event"] == "cache_hit")["result"], [])

    def test_debug_k8s_does_not_change_numeric_output(self):
        config = self.write_config(selector={})
        log = self.directory / "k8s-debug.jsonl"
        result = self.cli("--debug-log", str(log), "cluster", "pending", script="k8s_prometheus.py", config=config)
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "1\n", ""))
        records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
        self.assertTrue(any(row["event"] == "http_response" for row in records))

    def test_debug_http_error_masks_credentials_in_response(self):
        log = self.directory / "auth-debug.jsonl"
        password = 'private"пароль'
        token = base64.b64encode(("reader:" + password).encode()).decode()
        config = self.write_config(username="reader", password=password)
        self.server.status = 403
        self.server.default = {"error": password, "echo": "Basic " + token}
        result = self.cli("--debug-log", str(log), "brokers", config=config)
        self.assert_failure(result, "403")
        content = log.read_text(encoding="utf-8")
        self.assertNotIn(password, content)
        self.assertNotIn(token, content)
        records = [json.loads(line) for line in content.splitlines()]
        response = next(row for row in records if row["event"] == "http_response")
        self.assertEqual(response["status"], 403)
        self.assertEqual(json.loads(response["body"]), {"error": "[REDACTED]", "echo": "Basic [REDACTED]"})

    def test_debug_invalid_json_records_raw_response(self):
        log = self.directory / "bad-json.jsonl"
        self.server.default = b"<html>proxy error</html>"
        self.assert_failure(self.cli("--debug-log", str(log), "brokers"))
        records = [json.loads(line) for line in log.read_text(encoding="utf-8").splitlines()]
        self.assertEqual(next(row for row in records if row["event"] == "http_response")["body"], "<html>proxy error</html>")
        self.assertEqual(records[-1]["event"], "http_error")

    def test_debug_unwritable_path_fails_before_request(self):
        log = self.directory / "missing-parent" / "debug.jsonl"
        self.assert_failure(self.cli("--debug-log", str(log), "brokers"), "debug")
        self.assertEqual(self.server.calls, [])

    def test_debug_option_requires_path(self):
        self.assert_failure(self.cli("--debug-log"), "--debug-log")

    def test_basic_auth_from_password_file(self):
        secret = 'secret:пароль $"'
        (self.directory / "password").write_text(secret + "\n", encoding="utf-8")
        self.server.auth = "Basic " + base64.b64encode(("reader:" + secret).encode()).decode()
        config = self.write_config(username="reader", password_file="password")
        result = self.cli("brokers", config=config)
        self.assertEqual((result.returncode, result.stdout, result.stderr), (0, "1\n", ""))
        self.assertEqual(self.server.calls[0][1], self.server.auth)
        for path in (self.directory / "cache").glob("*.json"):
            self.assertNotIn(secret, path.read_text())
            self.assertEqual(path.stat().st_mode & 0o777, 0o600)

    def test_http_errors_are_not_zero_for_both_scripts(self):
        for script in ("kafka_prometheus.py", "k8s_prometheus.py"):
            config = self.write_config(selector={})
            for status in (401, 403, 500):
                self.server.status = status
                self.assert_failure(self.cli("prometheus.health", script=script, config=config), str(status))

    def test_redirect_does_not_forward_credentials(self):
        target = self.start_server()
        self.server.redirect = f"http://127.0.0.1:{target.server_port}/api/v1/query"
        config = self.write_config(username="reader", password="secret")
        self.assert_failure(self.cli("brokers", config=config), "redirect")
        self.assertEqual(target.calls, [])

    def test_cache_shared_between_cli_processes(self):
        self.respond("kafka_consumergroup_lag", [sample(12, consumergroup="workers", topic="orders", partition="0"),
                                               sample(7, consumergroup="workers", topic="orders", partition="1")])
        for command, value in (("group_topic.lag.sum", "19\n"), ("group_topic.lag.max", "12\n")):
            result = self.cli(command, "workers", "orders")
            self.assertEqual((result.returncode, result.stdout), (0, value), result.stderr)
        result = self.cli("group_topic.discovery")
        self.assertEqual(json.loads(result.stdout), {"data": [{"{#CONSUMERGROUP}": "workers", "{#TOPIC}": "orders"}]})
        self.assertEqual(len(self.server.calls), 1)

    def test_cache_source_and_credentials_are_isolated(self):
        first = self.client()
        first.query("metric")
        other_server = self.start_server()
        other_server.default = vector([sample(42)])
        other = self.client(self.write_config(name="other.json", url=f"http://127.0.0.1:{other_server.server_port}"))
        self.assertEqual(other.query("metric")[0]["value"][1], "42")
        self.server.auth = "Basic " + base64.b64encode(b"reader:new-password").decode()
        wrong = self.client(self.write_config(name="wrong.json", username="reader", password="wrong"))
        with self.assertRaises(MonitoringError):
            wrong.query("metric")
        self.assertNotEqual(first.cache_path("metric"), wrong.cache_path("metric"))

    def test_ttl_and_corrupt_cache_refresh(self):
        client = self.client()
        client.query("metric")
        path = client.cache_path("metric")
        path.write_text(json.dumps({"timestamp": 0, "result": []}))
        client.query("metric")
        path.write_text("{bad")
        with redirect_stderr(io.StringIO()) as errors:
            client.query("metric")
        self.assertIn("WARNING", errors.getvalue())
        self.assertEqual(len(self.server.calls), 3)

    def test_no_stale_cache_on_failure(self):
        client = self.client()
        client.query("metric")
        client.cache_path("metric").write_text(json.dumps({"timestamp": 0, "result": [sample(0)]}))
        self.server.status = 503
        with self.assertRaises(MonitoringError):
            client.query("metric")

    def test_zero_ttl_disables_cache(self):
        client = self.client(self.write_config(cache_ttl=0))
        client.query("metric")
        client.query("metric")
        self.assertEqual(len(self.server.calls), 2)
        self.assertFalse((self.directory / "cache").exists())

    def test_health_bypasses_cache(self):
        self.assertEqual(self.cli("prometheus.health").stdout, "1\n")
        self.server.status = 401
        self.assert_failure(self.cli("prometheus.health"), "401")
        self.assertEqual(len(self.server.calls), 2)

    def test_whitespace_inside_label_is_preserved(self):
        client = self.client(self.write_config(selector={"job": 'a  b"\\c'}))
        query = client.metric_query("metric")
        client.query(query)
        self.assertIn('a  b', self.server.calls[0][0])
        self.assertNotEqual(client.cache_path('metric{job="a  b"}'), client.cache_path('metric{job="a b"}'))

    def test_parallel_cache_writes_produce_valid_json(self):
        client = self.client()
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(lambda i: client.write_cache("metric", [sample(i)]), range(32)))
        result = client.read_cache("metric")
        self.assertEqual(len(result), 1)
        self.assertEqual(list((self.directory / "cache").glob(".prom-*")), [])

    def test_unwritable_cache_warns_but_returns_data(self):
        self.directory.joinpath("not-a-dir").write_text("file")
        client = self.client(self.write_config(cache_dir=str(self.directory / "not-a-dir")))
        with redirect_stderr(io.StringIO()) as errors:
            self.assertEqual(client.query("metric")[0]["value"][1], "1")
        self.assertIn("WARNING", errors.getvalue())

    def test_missing_nan_negative_duplicate_lag_fail(self):
        for rows in ([], [sample("NaN", consumergroup="g", topic="t", partition="0")],
                     [sample(-1, consumergroup="g", topic="t", partition="0")],
                     [sample(1, consumergroup="g", topic="t", partition="0", instance="a"),
                      sample(2, consumergroup="g", topic="t", partition="0", instance="b")]):
            config = self.write_config(cache_ttl=0)
            self.respond("kafka_consumergroup_lag", rows)
            self.assert_failure(self.cli("group_topic.lag.sum", "g", "t", config=config))

    def test_topics_groups_and_replication(self):
        self.respond("kafka_topic_partitions", [sample(2, topic="orders"), sample(1, topic="audit")])
        self.respond("kafka_consumergroup_members", [sample(0, consumergroup="workers")])
        self.respond("kafka_topic_partition_under_replicated_partition", [sample(1, topic="orders", partition="0"),
                                                                        sample(0, topic="orders", partition="1")])
        self.assertEqual(self.cli("topic.partitions", "orders").stdout, "2\n")
        self.assertEqual(self.cli("topic.under_replicated", "orders").stdout, "1\n")
        self.assertEqual(self.cli("group.members", "workers").stdout, "0\n")
        self.assertEqual(json.loads(self.cli("topic.discovery").stdout), {"data": [{"{#TOPIC}": "audit"}, {"{#TOPIC}": "orders"}]})
        self.assertEqual(json.loads(self.cli("group.discovery").stdout), {"data": [{"{#CONSUMERGROUP}": "workers"}]})
        self.assert_failure(self.cli("topic.partitions", "missing"))

    def test_up_zero_and_missing_are_distinct(self):
        self.write_config(cache_ttl=0)
        self.respond("up", [sample(0)])
        self.assertEqual(self.cli("exporter.up").stdout, "0\n")
        self.respond("up", [])
        self.assert_failure(self.cli("exporter.up"))

    def test_empty_discovery_is_json(self):
        self.respond("kafka_topic_partitions", [])
        self.assertEqual(json.loads(self.cli("topic.discovery").stdout), {"data": []})

    def test_selector_filters_metrics_and_up_without_instance(self):
        config = self.write_config(selector={"job": "kafka-exporter", "namespace": "kafka"})
        expected_up = 'up{job="kafka-exporter",namespace="kafka"}'
        expected_brokers = 'kafka_brokers{job="kafka-exporter",namespace="kafka"}'
        self.server.responses[expected_up] = vector([sample(1)])
        self.server.responses[expected_brokers] = vector([sample(3)])
        self.assertEqual(self.cli("exporter.up", config=config).stdout, "1\n")
        self.assertEqual(self.cli("brokers", config=config).stdout, "3\n")
        self.assertEqual([query for query, _ in self.server.calls], [expected_up, expected_brokers])
        self.assertNotIn("instance=", expected_up)
        # IP пода меняется, но выбор target остаётся тем же. / Pod IP changes, selection stays stable.
        updated = self.write_config(cache_ttl=0, selector={"job": "kafka-exporter", "namespace": "kafka"})
        self.server.responses[expected_up] = vector([sample(1, instance="10.0.0.1:9308")])
        self.assertEqual(self.cli("exporter.up", config=updated).stdout, "1\n")
        self.server.responses[expected_up] = vector([sample(1, instance="10.0.0.2:9308")])
        self.assertEqual(self.cli("exporter.up", config=updated).stdout, "1\n")
        self.assertEqual([query for query, _ in self.server.calls[-2:]], [expected_up, expected_up])

    def test_selector_accepts_any_label_without_job(self):
        config = self.write_config(selector={"namespace": "kafka"})
        query = 'up{namespace="kafka"}'
        self.server.responses[query] = vector([sample(1)])
        self.assertEqual(self.cli("exporter.up", config=config).stdout, "1\n")
        self.assertEqual(self.server.calls[0][0], query)

    def test_selector_accepts_multiple_stable_labels(self):
        selector = {"namespace": "kafka", "service": "exporter", "cluster": "production"}
        config = self.write_config(selector=selector)
        query = 'up{cluster="production",namespace="kafka",service="exporter"}'
        self.server.responses[query] = vector([sample(1)])
        self.assertEqual(self.cli("exporter.up", config=config).stdout, "1\n")
        self.assertEqual(self.server.calls[0][0], query)

    def test_removed_label_name_fields_are_rejected(self):
        for field in ("label_name", "label_value"):
            with self.subTest(field=field):
                self.assert_failure(self.cli("brokers", config=self.write_config(**{field: "old"})))

    def test_selector_required(self):
        config = self.write_config(selector={})
        self.assert_failure(self.cli("brokers", config=config), "selector")

    def test_invalid_api_responses_fail(self):
        self.write_config(cache_ttl=0)
        for response in (b"bad json", {"status": "error", "error": "secret must not leak"},
                         {"status": "success", "data": {"resultType": "scalar", "result": [0, "1"]}},
                         vector([{"metric": {}}]),
                         dict(vector([sample(1)]), warnings=["partial response"])):
            self.server.default = response
            result = self.cli("brokers")
            self.assert_failure(result)
            self.assertNotIn("secret must not leak", result.stderr)

    def test_truncated_http_response_fails_cleanly(self):
        self.server.truncate = True
        result = self.cli("brokers")
        self.assert_failure(result, "сеть")
        self.assertNotIn("Traceback", result.stderr)

    def test_config_validation(self):
        for overrides in ({"timeout": True}, {"timeout": 0}, {"cache_ttl": -1}, {"username": "reader"},
                          {"username": "reader", "password": "secret", "password_file": "file"},
                          {"url": "http://user:password@example.org"}, {"unexpected": 1},
                          {"selector": {"bad-label": "x"}}, {"ca_file": "missing.crt"}):
            with self.subTest(overrides=list(overrides)):
                self.assert_failure(self.cli("brokers", config=self.write_config(**overrides)))

    def test_environment_overrides_json(self):
        with patch.dict(os.environ, {"PROM_TIMEOUT": "3", "PROM_CACHE_TTL": "0", "PROM_URL": "http://other:9090"}):
            settings = load_settings("kafka", str(self.config))
        self.assertEqual((settings["timeout"], settings["cache_ttl"], settings["url"]), (3, 0, "http://other:9090"))

    def test_k8s_selector_filters_every_metric_family(self):
        config = self.write_config(selector={"cluster": "production"}, cache_ttl=0)
        expected = (
            (("node.discovery",), 'kube_node_info{cluster="production"}'),
            (("node.condition", "Ready", "worker-01"),
             'kube_node_status_condition{cluster="production",condition="Ready",node="worker-01",status="true"}'),
            (("cluster", "oomkilled"),
             'sum(kube_pod_container_status_last_terminated_reason{cluster="production",reason="OOMKilled"})'),
            (("cluster", "crashloop"),
             'sum(kube_pod_container_status_waiting_reason{cluster="production",reason="CrashLoopBackOff"})'),
            (("cluster", "pending"), 'sum(kube_pod_status_phase{cluster="production",phase="Pending"})'),
            (("cluster", "failed"), 'sum(kube_pod_status_phase{cluster="production",phase="Failed"})'),
            (("cluster", "deployment_not_ready"),
             'sum(clamp_min(kube_deployment_spec_replicas{cluster="production"} - '
             'kube_deployment_status_ready_replicas{cluster="production"}, 0))'),
            (("cluster", "pvc_not_bound"),
             'sum(kube_persistentvolumeclaim_status_phase{cluster="production",phase!="Bound"})'),
            (("ingress.service_status.discovery",),
             'sum by (service, status) (increase(nginx_ingress_controller_request{cluster="production"}[1m]))'),
            (("ingress.status.service", "500", "app"),
             'sum by (service, status) (increase(nginx_ingress_controller_request{cluster="production"}[1m]))'),
        )
        for args, query in expected:
            with self.subTest(args=args):
                result = self.cli(*args, script="k8s_prometheus.py", config=config)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(self.server.calls[-1][0], query)
        self.assertEqual(len(self.server.calls), len(expected))

    def test_k8s_selector_same_label_and_conflict(self):
        config = self.write_config(selector={"phase": "Pending"}, cache_ttl=0)
        result = self.cli("cluster", "pending", script="k8s_prometheus.py", config=config)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(self.server.calls[-1][0], 'sum(kube_pod_status_phase{phase="Pending"})')
        result = self.cli("cluster", "failed", script="k8s_prometheus.py", config=config)
        self.assert_failure(result, "phase")
        config = self.write_config(selector={"phase": "Bound"}, cache_ttl=0)
        self.assert_failure(self.cli("cluster", "pvc_not_bound", script="k8s_prometheus.py", config=config), "phase")
        config = self.write_config(selector={"node": "worker-02"}, cache_ttl=0)
        self.assert_failure(self.cli("node.condition", "Ready", "worker-01", script="k8s_prometheus.py", config=config), "node")
        self.assertEqual(len(self.server.calls), 1)

    def test_k8s_selector_does_not_affect_prometheus_health(self):
        config = self.write_config(selector={"cluster": "production"})
        result = self.cli("prometheus.health", script="k8s_prometheus.py", config=config)
        self.assertEqual((result.returncode, result.stdout), (0, "1\n"), result.stderr)
        self.assertEqual(self.server.calls[-1][0], "vector(1)")

    def test_k8s_legacy_commands_and_empty_value(self):
        config = self.write_config(selector={}, cache_ttl=0)
        self.server.responses["kube_node_info"] = vector([sample(1, node="node1"), sample(1, node="node1")])
        result = self.cli("node.discovery", script="k8s_prometheus.py", config=config)
        self.assertEqual(json.loads(result.stdout), {"data": [{"{#NODE}": "node1"}]})
        self.server.default = vector([sample(3)])
        self.assertEqual(self.cli("cluster", "pending", script="k8s_prometheus.py", config=config).stdout, "3\n")
        self.server.default = vector([])
        self.assertEqual(self.cli("cluster", "pending", script="k8s_prometheus.py", config=config).stdout, "0\n")
        self.server.default = vector([sample(1), sample(2)])
        self.assert_failure(self.cli("node.condition", "Ready", "node1", script="k8s_prometheus.py", config=config))

    def test_k8s_ingress_and_escaped_node(self):
        config = self.write_config(selector={}, cache_ttl=0)
        self.server.default = vector([sample(2.5, service="app", status="200")])
        self.assertEqual(self.cli("ingress.status.service", "200", "app", script="k8s_prometheus.py", config=config).stdout, "2.5\n")
        result = self.cli("ingress.service_status.discovery", script="k8s_prometheus.py", config=config)
        self.assertEqual(json.loads(result.stdout), {"data": [{"{#SERVICE}": "app", "{#STATUS}": "200"}]})
        self.cli("node.condition", "Ready", 'node"\\name', script="k8s_prometheus.py", config=config)
        self.assertIn('node=' + json.dumps('node"\\name'), self.server.calls[-1][0])

    def test_timeout_fails_without_zero(self):
        def slow():
            time.sleep(0.2)
            return vector([sample(1)])
        self.server.default = slow
        config = self.write_config(timeout=0.05)
        self.assert_failure(self.cli("brokers", config=config), "сеть")

    @unittest.skipUnless(shutil.which("openssl"), "Для интеграционного теста TLS нужен openssl")
    def test_https_custom_ca_and_hostname_verification(self):
        cert, key = self.directory / "ca.crt", self.directory / "ca.key"
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-keyout", str(key), "-out", str(cert), "-subj", "/CN=localhost",
                        "-addext", "subjectAltName=DNS:localhost"], check=True, capture_output=True)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cert, key)
        server = self.start_server(context)
        url = f"https://localhost:{server.server_port}"
        config = self.write_config(url=url, cache_ttl=0)
        self.assert_failure(self.cli("brokers", config=config), "TLS")
        config = self.write_config(url=url, ca_file=str(cert), cache_ttl=0)
        result = self.cli("brokers", config=config)
        self.assertEqual((result.returncode, result.stdout), (0, "1\n"), result.stderr)
        config = self.write_config(url=f"https://127.0.0.1:{server.server_port}", ca_file=str(cert), cache_ttl=0)
        self.assert_failure(self.cli("brokers", config=config), "TLS")


if __name__ == "__main__":
    unittest.main()
