#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import importlib.util
from contextlib import redirect_stdout
import io
import json
from pathlib import Path
from types import SimpleNamespace
import sys
import unittest
from unittest import mock
import urllib.error
import urllib.parse


MODULE_PATH = Path(__file__).with_name("emit_litefuse_otel_io.py")
SPEC = importlib.util.spec_from_file_location("emit_litefuse_otel_io", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def attribute_values(span):
    values = {}
    for attribute in span["attributes"]:
        value = attribute["value"]
        if "arrayValue" in value:
            values[attribute["key"]] = [
                next(iter(item.values()))
                for item in value["arrayValue"]["values"]
            ]
        else:
            values[attribute["key"]] = next(iter(value.values()))
    return values


class FakeResponse:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, _exc_type, _exc, _traceback):
        return False

    def read(self):
        return b"{}"


class PartialSuccessResponse(FakeResponse):
    def read(self):
        return b'{"partialSuccess":{"rejectedSpans":"1","errorMessage":"bad span"}}'


class JsonResponse(FakeResponse):
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode()


class LitefuseOtelExporterTest(unittest.TestCase):
    def verify_single_trace(self, args, public_key, secret_key, trace_id, count):
        result = {"trace_id": trace_id, "observation_count": count}
        MODULE.verify_traces(
            args, public_key, secret_key, [{"result": result, "subagent": False}]
        )
        return result["verified"]

    def verification_args(self, **overrides):
        return SimpleNamespace(**{
            "base_url": "https://litefuse.example",
            "verify_attempts": 3,
            "verify_sleep_seconds": 5,
            "verify_timeout_seconds": 120,
            "min_observations": 3,
            "min_step_observations": 1,
            **overrides,
        })

    def verification_fixture(self, *, subagent=False, count=3):
        events = [self.span_event(f"{index:032x}") for index in range(1, count + 1)]
        events[0]["body"]["name"] = "codex.subagent.review" if subagent else "codex.review"
        payload = {"batch": [{"type": "trace-create", "body": self.trace_body()}, *events]}
        result = {"trace_id": "subagent" if subagent else "main", "observation_count": count}
        target = MODULE.trace_verification_target(result, payload, subagent=subagent)
        observations = [
            {**event["body"], "id": MODULE.otel_id(event["body"]["id"], 8)}
            for event in events
        ]
        return target, observations, payload

    def trace_body(self):
        return {
            "id": "1" * 32,
            "name": "doris-ai-review",
            "sessionId": "run-123",
            "environment": "github-actions",
            "metadata": {"repository": "apache/doris", "codex_jsonl": True},
            "tags": ["doris-ai-review", "codex-jsonl"],
        }

    def span_event(self, span_id, parent_id=None, output_size=0):
        body = {
            "id": span_id,
            "traceId": "1" * 32,
            "name": "codex.command",
            "startTime": "2026-09-01T00:00:00.000Z",
            "endTime": "2026-09-01T00:00:01.000Z",
            "input": {"command": "git status"},
            "output": {"status": "completed", "text": "x" * output_size},
            "environment": "github-actions",
            "metadata": {"item_type": "command_execution"},
            "level": "DEFAULT",
        }
        if parent_id:
            body["parentObservationId"] = parent_id
        return {"type": "span-create", "body": body}

    def reject_payloads_above(self, server_limit, request_sizes):
        def fake_urlopen(request, timeout):
            request_sizes.append(len(request.data))
            if len(request.data) > server_limit:
                raise urllib.error.HTTPError(
                    request.full_url,
                    413,
                    "Payload Too Large",
                    {},
                    io.BytesIO(b"payload too large"),
                )
            return FakeResponse()

        return fake_urlopen

    def test_converts_legacy_events_to_otlp_hierarchy_and_attributes(self):
        root_id = "2" * 32
        child_id = "3" * 32
        root = self.span_event(root_id)
        child = self.span_event(child_id, root_id)

        payload = MODULE.otlp_payload(self.trace_body(), [root, child])
        spans = payload["resourceSpans"][0]["scopeSpans"][0]["spans"]

        self.assertEqual(len(spans), 2)
        self.assertEqual(spans[0]["traceId"], "1" * 32)
        self.assertEqual(len(spans[0]["spanId"]), 16)
        self.assertNotIn("parentSpanId", spans[0])
        self.assertEqual(spans[1]["parentSpanId"], spans[0]["spanId"])
        attributes = attribute_values(spans[0])
        self.assertEqual(attributes["langfuse.trace.name"], "doris-ai-review")
        self.assertEqual(attributes["session.id"], "run-123")
        self.assertEqual(attributes["langfuse.observation.type"], "span")
        self.assertTrue(attributes["langfuse.internal.is_app_root"])
        self.assertEqual(
            attributes["langfuse.trace.tags"],
            ["doris-ai-review", "codex-jsonl"],
        )

    def test_bounds_and_shrinks_failed_turn_status_message(self):
        args = SimpleNamespace(
            repository="apache/doris",
            workflow="Code Review",
            run_id="run-123",
            pr_number="67413",
            head_sha="a" * 40,
            base_sha="b" * 40,
            reasoning_effort="xhigh",
            max_json_chars=20_000,
            max_context_json_chars=0,
            trace_name="doris-ai-review",
            session_id="run-123",
            environment="github-actions",
            model="gpt-5.6-sol",
        )
        events = [
            {"type": "turn.failed", "error": {"message": "e" * 50_000}}
        ]
        _trace_id, payload, _observation_count = MODULE.build_ingestion_payload(
            args, "review task", "", events
        )
        turn_event = next(
            event
            for event in payload["batch"]
            if (event.get("body") or {}).get("name") == "codex.turn"
        )
        source_status = turn_event["body"]["statusMessage"]

        self.assertLess(len(source_status), 20_100)
        self.assertIn("[truncated to first 20000 chars]", source_status)
        trace_body = MODULE.trace_body_from_payload(payload)
        self.assertGreater(
            MODULE.json_payload_bytes(MODULE.otlp_payload(trace_body, [turn_event])),
            20_000,
        )

        chunks = MODULE.otlp_chunks(payload, max_payload_bytes=20_000)
        spans = [
            span
            for _legacy, otel, _size in chunks
            for resource in otel["resourceSpans"]
            for scope in resource["scopeSpans"]
            for span in scope["spans"]
        ]
        turn_span = next(span for span in spans if span["name"] == "codex.turn")
        attributes = attribute_values(turn_span)

        self.assertEqual(turn_span["status"], {"code": 2})
        self.assertIn(
            "[truncated to first", attributes["langfuse.observation.status_message"]
        )
        self.assertTrue(all(size <= 20_000 for _legacy, _otel, size in chunks))

    def test_subagent_root_observation_preserves_task_input(self):
        args = SimpleNamespace(
            max_input_chars=200_000,
            max_output_chars=200_000,
            max_json_chars=40_000,
            repository="apache/doris",
            workflow="Code Review",
            run_id="run-123",
            pr_number="67413",
            head_sha="a" * 40,
            base_sha="b" * 40,
            reasoning_effort="xhigh",
            session_id="run-123",
            subagent_trace_name="doris-ai-review-subagent",
            environment="github-actions",
            model="gpt-5.6-sol",
        )
        session_path = "/tmp/thread-123.jsonl"
        events = [
            {
                "type": "session_meta",
                "timestamp": "2026-09-01T00:00:00.000Z",
                "payload": {"id": "thread-123"},
            },
            {
                "type": "response_item",
                "timestamp": "2026-09-01T00:00:01.000Z",
                "payload": {
                    "type": "message",
                    "role": "user",
                    "content": [{"type": "input_text", "text": "review task"}],
                },
            },
            {
                "type": "response_item",
                "timestamp": "2026-09-01T00:00:02.000Z",
                "payload": {
                    "type": "message",
                    "role": "assistant",
                    "content": [{"type": "output_text", "text": "review result"}],
                },
            },
        ]

        result = MODULE.build_subagent_session_payload(args, session_path, events)
        chunks = MODULE.otlp_chunks(result["payload"], max_payload_bytes=20_000)
        spans = [
            span
            for _legacy, otel, _size in chunks
            for resource in otel["resourceSpans"]
            for scope in resource["scopeSpans"]
            for span in scope["spans"]
        ]
        root = next(span for span in spans if span["name"] == "codex.subagent.review")
        attributes = attribute_values(root)

        self.assertEqual(
            json.loads(attributes["langfuse.observation.input"]),
            {"prompt": "review task"},
        )
        self.assertEqual(
            attributes["langfuse.observation.metadata.session_file"], session_path
        )
        self.assertEqual(
            attributes["langfuse.observation.metadata.thread_id"], "thread-123"
        )

    def test_chunks_encoded_otlp_payloads_to_requested_size(self):
        trace_event = {"type": "trace-create", "body": self.trace_body()}
        span_events = [
            self.span_event(f"{index:032x}", output_size=2_000)
            for index in range(1, 9)
        ]

        chunks = MODULE.otlp_chunks(
            {"batch": [trace_event, *span_events]}, max_payload_bytes=6_000
        )

        self.assertGreater(len(chunks), 1)
        self.assertEqual(
            sum(MODULE.otlp_span_count(otel) for _legacy, otel, _size in chunks),
            len(span_events),
        )
        self.assertTrue(all(size <= 6_000 for _legacy, _otel, size in chunks))

    def test_truncates_one_oversized_span(self):
        trace_event = {"type": "trace-create", "body": self.trace_body()}
        span_event = self.span_event("2" * 32, output_size=50_000)

        chunks = MODULE.otlp_chunks(
            {"batch": [trace_event, span_event]}, max_payload_bytes=6_000
        )

        self.assertEqual(len(chunks), 1)
        _legacy, otel, size = chunks[0]
        self.assertLessEqual(size, 6_000)
        output = attribute_values(
            otel["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        )["langfuse.observation.output"]
        self.assertIn("truncated_json", output)

    def test_does_not_truncate_span_below_full_otlp_limit(self):
        trace_event = {"type": "trace-create", "body": self.trace_body()}
        span_event = self.span_event("2" * 32, output_size=3_500)
        original = MODULE.otlp_payload(self.trace_body(), [span_event])
        original_size = MODULE.json_payload_bytes(original)

        self.assertGreater(original_size, 3_000)
        self.assertLessEqual(original_size, 6_000)
        chunks = MODULE.otlp_chunks(
            {"batch": [trace_event, span_event]}, max_payload_bytes=6_000
        )

        self.assertEqual(len(chunks), 1)
        _legacy, otel, size = chunks[0]
        self.assertEqual(size, original_size)
        output = attribute_values(
            otel["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        )["langfuse.observation.output"]
        self.assertNotIn("truncated_json", output)
        self.assertEqual(len(json.loads(output)["text"]), 3_500)

    def test_preserves_near_limit_single_span_payload(self):
        trace_event = {"type": "trace-create", "body": self.trace_body()}
        span_event = self.span_event("2" * 32, output_size=4_509)
        original = MODULE.otlp_payload(self.trace_body(), [span_event])
        original_size = MODULE.json_payload_bytes(original)

        self.assertGreater(original_size, 6_000)
        self.assertLess(original_size, 6_100)
        chunks = MODULE.otlp_chunks(
            {"batch": [trace_event, span_event]}, max_payload_bytes=6_000
        )

        self.assertEqual(len(chunks), 1)
        _legacy, otel, size = chunks[0]
        self.assertGreater(size, 5_500)
        self.assertLessEqual(size, 6_000)
        output = attribute_values(
            otel["resourceSpans"][0]["scopeSpans"][0]["spans"][0]
        )["langfuse.observation.output"]
        self.assertGreater(len(json.loads(output)["truncated_json"]), 4_000)

    def test_prechunks_before_otlp_encoding(self):
        trace_event = {"type": "trace-create", "body": self.trace_body()}
        span_events = [
            self.span_event(f"{index:032x}", output_size=1_000)
            for index in range(1, 21)
        ]
        encoded_batch_sizes = []
        original_otlp_payload = MODULE.otlp_payload

        def recording_otlp_payload(trace_body, events):
            encoded_batch_sizes.append(len(events))
            return original_otlp_payload(trace_body, events)

        with mock.patch.object(
            MODULE, "otlp_payload", side_effect=recording_otlp_payload
        ):
            MODULE.otlp_chunks(
                {"batch": [trace_event, *span_events]}, max_payload_bytes=6_000
            )

        self.assertLess(max(encoded_batch_sizes), len(span_events))

    def test_posts_otlp_v4_headers(self):
        payload = MODULE.otlp_payload(
            self.trace_body(), [self.span_event("2" * 32)]
        )
        captured = {}

        def fake_urlopen(request, timeout):
            captured["request"] = request
            captured["timeout"] = timeout
            return FakeResponse()

        with mock.patch.object(MODULE.urllib.request, "urlopen", fake_urlopen):
            status = MODULE.post_payload_once(
                "https://litefuse.example/api/public/otel/v1/traces",
                "public",
                "secret",
                payload,
                30,
            )

        headers = {key.lower(): value for key, value in captured["request"].header_items()}
        self.assertEqual(headers["content-type"], "application/json")
        self.assertEqual(headers["x-langfuse-ingestion-version"], "4")
        self.assertEqual(headers["x-langfuse-sdk-name"], "doris-code-review")
        self.assertEqual(captured["timeout"], 30)
        self.assertEqual(status["success_count"], 1)

    def test_paginates_v2_observations_until_root_is_visible(self):
        first_page = [{"id": f"child-{index}"} for index in range(1_000)]
        responses = [
            JsonResponse({"data": first_page, "meta": {"cursor": "next"}}),
            JsonResponse({"data": [{"id": "root"}], "meta": {}}),
        ]
        requests = []

        def fake_urlopen(request, timeout):
            requests.append((request, timeout))
            return responses.pop(0)

        with mock.patch.object(MODULE.urllib.request, "urlopen", fake_urlopen):
            payload = MODULE.fetch_observations_v2(
                "https://litefuse.example", "public", "secret", "trace-id"
            )

        self.assertEqual(len(payload["data"]), 1_001)
        self.assertEqual(payload["data"][-1], {"id": "root"})
        first_query = urllib.parse.parse_qs(
            urllib.parse.urlparse(requests[0][0].full_url).query
        )
        second_query = urllib.parse.parse_qs(
            urllib.parse.urlparse(requests[1][0].full_url).query
        )
        self.assertEqual(first_query["limit"], ["1000"])
        self.assertNotIn("cursor", first_query)
        self.assertEqual(second_query["cursor"], ["next"])

    def test_rejects_incomplete_v2_observation_pagination(self):
        response = JsonResponse(
            {"data": [{"id": "newest"}], "meta": {"cursor": "still-more"}}
        )

        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=response
        ):
            with self.assertRaisesRegex(RuntimeError, "remained paginated"):
                MODULE.fetch_observations_v2(
                    "https://litefuse.example",
                    "public",
                    "secret",
                    "trace-id",
                    max_pages=1,
                )

    def test_rejects_otlp_partial_success(self):
        payload = MODULE.otlp_payload(
            self.trace_body(), [self.span_event("2" * 32)]
        )

        with mock.patch.object(
            MODULE.urllib.request, "urlopen", return_value=PartialSuccessResponse()
        ):
            with self.assertRaisesRegex(RuntimeError, "partially rejected 1 spans"):
                MODULE.post_payload_once(
                    "https://litefuse.example/api/public/otel/v1/traces",
                    "public",
                    "secret",
                    payload,
                    30,
                )

    def test_splits_multi_span_chunk_after_transport_error(self):
        trace_event = {"type": "trace-create", "body": self.trace_body()}
        payload = {
            "batch": [
                trace_event,
                self.span_event("2" * 32),
                self.span_event("3" * 32, "2" * 32),
            ]
        }

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[
                urllib.error.URLError("connection reset"),
                FakeResponse(),
                FakeResponse(),
            ],
        ):
            status = MODULE.post_payload(
                "https://litefuse.example/api/public/otel/v1/traces",
                "public",
                "secret",
                payload,
                10_000,
                30,
                3,
                0,
            )

        self.assertEqual(status["transport_retries"], 1)
        self.assertEqual(status["request_count"], 2)
        self.assertEqual(status["success_count"], 2)

    def test_splits_multi_span_chunk_after_http_413(self):
        trace_event = {"type": "trace-create", "body": self.trace_body()}
        payload = {
            "batch": [
                trace_event,
                self.span_event("2" * 32),
                self.span_event("3" * 32, "2" * 32),
            ]
        }
        payload_too_large = urllib.error.HTTPError(
            "https://litefuse.example/api/public/otel/v1/traces",
            413,
            "Payload Too Large",
            {},
            io.BytesIO(b"payload too large"),
        )

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[payload_too_large, FakeResponse(), FakeResponse()],
        ):
            status = MODULE.post_payload(
                "https://litefuse.example/api/public/otel/v1/traces",
                "public",
                "secret",
                payload,
                10_000,
                30,
                3,
                0,
            )

        self.assertEqual(status["payload_too_large_retries"], 1)
        self.assertEqual(status["request_count"], 2)
        self.assertEqual(status["success_count"], 2)

    def test_retries_single_span_413_without_half_size_ceiling(self):
        trace_body = {
            **self.trace_body(),
            "metadata": {"repository": "apache/doris", "fixed": "m" * 3_000},
        }
        trace_event = {"type": "trace-create", "body": trace_body}
        span_event = self.span_event("2" * 32, output_size=1_500)
        server_limit = (
            MODULE.json_payload_bytes(MODULE.otlp_payload(trace_body, [span_event])) - 1
        )
        request_sizes = []

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=self.reject_payloads_above(server_limit, request_sizes),
        ):
            status = MODULE.post_payload(
                "https://litefuse.example/api/public/otel/v1/traces",
                "public",
                "secret",
                {"batch": [trace_event, span_event]},
                10_000,
                30,
                3,
                0,
            )

        self.assertEqual(status["payload_too_large_retries"], 1)
        self.assertEqual(status["request_count"], 1)
        self.assertEqual(status["success_count"], 1)
        self.assertEqual(len(request_sizes), 2)
        self.assertLessEqual(request_sizes[1], server_limit)
        self.assertLess(request_sizes[1], request_sizes[0])
        self.assertGreater(request_sizes[1], request_sizes[0] // 2)

    def test_adapts_single_span_413_for_lower_server_limit(self):
        trace_body = {
            **self.trace_body(),
            "metadata": {"repository": "apache/doris", "fixed": "m" * 3_000},
        }
        trace_event = {"type": "trace-create", "body": trace_body}
        span_event = self.span_event("2" * 32, output_size=5_000)
        initial_size = MODULE.json_payload_bytes(
            MODULE.otlp_payload(trace_body, [span_event])
        )
        server_limit = initial_size - 1_000
        request_sizes = []

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=self.reject_payloads_above(server_limit, request_sizes),
        ):
            status = MODULE.post_payload(
                "https://litefuse.example/api/public/otel/v1/traces",
                "public",
                "secret",
                {"batch": [trace_event, span_event]},
                10_000,
                30,
                5,
                0,
            )

        self.assertEqual(initial_size, 9_486)
        self.assertEqual(len(request_sizes), 3)
        self.assertTrue(
            all(
                current > following
                for current, following in zip(request_sizes, request_sizes[1:])
            )
        )
        self.assertLessEqual(request_sizes[-1], server_limit)
        self.assertEqual(status["payload_too_large_retries"], 2)
        self.assertEqual(status["post_attempt_count"], len(request_sizes))
        self.assertEqual(status["success_count"], 1)

    def test_stops_single_span_413_after_retry_budget(self):
        trace_event = {"type": "trace-create", "body": self.trace_body()}
        span_event = self.span_event("2" * 32, output_size=5_000)
        request_sizes = []

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=self.reject_payloads_above(-1, request_sizes),
        ):
            with self.assertRaisesRegex(RuntimeError, "after 3 retries"):
                MODULE.post_payload(
                    "https://litefuse.example/api/public/otel/v1/traces",
                    "public",
                    "secret",
                    {"batch": [trace_event, span_event]},
                    10_000,
                    30,
                    3,
                    0,
                )

        self.assertEqual(len(request_sizes), 4)
        self.assertTrue(
            all(
                current > following
                for current, following in zip(request_sizes, request_sizes[1:])
            )
        )


    def test_prechunking_encodes_each_legacy_event_once(self):
        span_events = [
            self.span_event(f"{index:032x}", output_size=200)
            for index in range(1, 101)
        ]
        original_compact_json_bytes = MODULE.compact_json_bytes
        encode_count = 0

        def recording_compact_json_bytes(value):
            nonlocal encode_count
            encode_count += 1
            return original_compact_json_bytes(value)

        with mock.patch.object(
            MODULE,
            "compact_json_bytes",
            side_effect=recording_compact_json_bytes,
        ):
            chunks = MODULE.chunk_payload(
                {"batch": span_events}, max_payload_bytes=5_000
            )

        self.assertEqual(encode_count, len(span_events))
        self.assertEqual(
            [event for chunk, _size in chunks for event in chunk["batch"]],
            span_events,
        )
        self.assertTrue(
            all(
                size == MODULE.json_payload_bytes(chunk)
                for chunk, size in chunks
            )
        )

    def test_retries_retryable_otlp_http_status(self):
        trace_event = {"type": "trace-create", "body": self.trace_body()}
        payload = {"batch": [trace_event, self.span_event("2" * 32)]}
        unavailable = urllib.error.HTTPError(
            "https://litefuse.example/api/public/otel/v1/traces",
            503,
            "Service Unavailable",
            {},
            io.BytesIO(b'{"message":"temporarily unavailable"}'),
        )

        with mock.patch.object(
            MODULE.urllib.request,
            "urlopen",
            side_effect=[unavailable, FakeResponse()],
        ):
            status = MODULE.post_payload(
                "https://litefuse.example/api/public/otel/v1/traces",
                "public",
                "secret",
                payload,
                10_000,
                30,
                3,
                0,
            )

        self.assertEqual(status["http_retries"], 1)
        self.assertEqual(status["request_count"], 1)
        self.assertEqual(status["success_count"], 1)

    def test_verifies_pending_subagent_without_polling_complete_main_again(self):
        main, main_rows, _ = self.verification_fixture()
        child, child_rows, _ = self.verification_fixture(subagent=True)
        replies = [main_rows, child_rows[:1], child_rows]
        with mock.patch.object(MODULE, "fetch_trace", return_value={}), mock.patch.object(
            MODULE, "fetch_observations_legacy",
            side_effect=[{"data": rows} for rows in replies],
        ) as fetch, mock.patch.object(MODULE.time, "sleep") as sleep:
            MODULE.verify_traces(self.verification_args(), "public", "secret", [main, child])

        self.assertEqual([call.args[3] for call in fetch.call_args_list], ["main", "subagent", "subagent"])
        sleep.assert_called_once_with(5)
        for target in (main, child):
            self.assertEqual(target["result"]["verified"]["observation_count"], 3)
            self.assertNotIn("verification_diagnostic", target["result"])

    def test_reports_all_missing_subagents_and_preserves_main_success(self):
        main, main_rows, _ = self.verification_fixture()
        children = [self.verification_fixture(subagent=True)[0] for _ in range(2)]
        for index, child in enumerate(children):
            child["result"]["trace_id"] = f"child-{index}"
        replies = [main_rows, [], [], [], []]
        with mock.patch.object(MODULE, "fetch_trace", return_value={}), mock.patch.object(
            MODULE, "fetch_observations_legacy",
            side_effect=[{"data": rows} for rows in replies],
        ), mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "child-0.*child-1"):
                MODULE.verify_traces(
                    self.verification_args(verify_attempts=2), "public", "secret",
                    [main, *children],
                )
        self.assertIn("verified", main["result"])
        for child in children:
            self.assertNotIn("verified", child["result"])
            self.assertEqual(
                child["result"]["verification_diagnostic"]["missing_expected_observation_count"], 3
            )
        sleep.assert_called_once_with(5)

    def test_subagent_profile_accepts_root_only_and_session_message_shapes(self):
        for count in (1, 2):
            with self.subTest(count=count):
                target, rows, _ = self.verification_fixture(subagent=True, count=count)
                if count == 2:
                    rows[1]["name"] = "codex.subagent.message.assistant"
                    rows[1]["input"] = {"role": "assistant"}
                    rows[1]["output"] = {"text": "done"}
                with mock.patch.object(MODULE, "fetch_trace", return_value={}), mock.patch.object(
                    MODULE, "fetch_observations_legacy", return_value={"data": rows}
                ):
                    MODULE.verify_traces(self.verification_args(), "public", "secret", [target])
                self.assertEqual(target["result"]["verified"]["required_observation_count"], count)

    def test_subagent_rejects_missing_root_id_or_io_even_when_count_is_sufficient(self):
        for defect in ("root", "wrong_id", "duplicate", "empty_id", "input", "output"):
            with self.subTest(defect=defect):
                target, rows, _ = self.verification_fixture(subagent=True)
                if defect == "root":
                    rows[0]["name"] = "codex.command"
                elif defect == "wrong_id":
                    rows[1]["id"] = "unrelated-observation"
                elif defect == "duplicate":
                    rows[1]["id"] = rows[2]["id"]
                elif defect == "empty_id":
                    rows[1]["id"] = ""
                else:
                    rows[1][defect] = None
                with mock.patch.object(
                    MODULE, "fetch_trace", return_value={"input": "p", "output": "o"}
                ), mock.patch.object(
                    MODULE, "fetch_observations_legacy", return_value={"data": rows}
                ):
                    with self.assertRaisesRegex(RuntimeError, "subagent"):
                        MODULE.verify_traces(
                            self.verification_args(verify_attempts=1), "public", "secret", [target]
                        )

    def test_subagent_accepts_exported_empty_io_and_unexported_none(self):
        for value in ({}, [], "", False, 0, None):
            with self.subTest(value=value):
                _, rows, payload = self.verification_fixture(subagent=True, count=2)
                payload["batch"][-1]["body"]["output"] = value
                result = {"trace_id": "subagent", "observation_count": 2}
                target = MODULE.trace_verification_target(result, payload, subagent=True)
                rows[1]["output"] = value
                with mock.patch.object(MODULE, "fetch_trace", return_value={}), mock.patch.object(
                    MODULE, "fetch_observations_legacy", return_value={"data": rows}
                ):
                    MODULE.verify_traces(self.verification_args(), "public", "secret", [target])
                self.assertIn("verified", result)

    def test_v2_fallback_uses_exported_session_time_window(self):
        target, rows, payload = self.verification_fixture(subagent=True, count=2)
        payload["batch"][-1]["body"]["startTime"] = "2026-09-01T02:00:00+00:00"
        target = MODULE.trace_verification_target(target["result"], payload, subagent=True)
        requests = []

        def fake_urlopen(request, timeout):
            requests.append(request)
            if "/v2/observations?" not in request.full_url:
                raise urllib.error.URLError("legacy API unavailable")
            return JsonResponse({"data": rows, "meta": {}})

        with mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen):
            MODULE.verify_traces(self.verification_args(), "public", "secret", [target])
        query = urllib.parse.parse_qs(urllib.parse.urlparse(requests[-1].full_url).query)
        self.assertEqual(query["fromStartTime"], ["2026-08-31T23:59:59+00:00"])
        self.assertEqual(query["toStartTime"], ["2026-09-01T02:00:01+00:00"])
        self.assertEqual(target["result"]["verified"]["read_source"], "v2_observations")

    def test_subagent_trace_detail_fallback_still_requires_all_exported_ids(self):
        for complete in (False, True):
            with self.subTest(complete=complete):
                target, rows, _ = self.verification_fixture(subagent=True)
                with mock.patch.object(
                    MODULE, "fetch_trace",
                    return_value={"observations": rows if complete else rows[:1]},
                ), mock.patch.object(
                    MODULE, "fetch_observations_legacy", side_effect=RuntimeError("unavailable")
                ), mock.patch.object(
                    MODULE, "fetch_observations_v2", side_effect=RuntimeError("unavailable")
                ):
                    if complete:
                        MODULE.verify_traces(self.verification_args(), "public", "secret", [target])
                        self.assertIn("verified", target["result"])
                    else:
                        with self.assertRaisesRegex(RuntimeError, "subagent"):
                            MODULE.verify_traces(
                                self.verification_args(verify_attempts=1), "public", "secret", [target]
                            )

    def test_verifies_subagent_with_more_than_ten_legacy_pages(self):
        target, rows, _ = self.verification_fixture(subagent=True, count=1001)
        rows = list(reversed(rows))  # The oldest root is on page 11.
        requests = []

        def fake_urlopen(request, timeout):
            query = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
            page = int(query["page"][0])
            requests.append(page)
            return JsonResponse({"data": rows[(page - 1) * 100:page * 100]})

        with mock.patch.object(MODULE, "fetch_trace", return_value={}), mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=fake_urlopen
        ):
            MODULE.verify_traces(self.verification_args(), "public", "secret", [target])
        self.assertEqual(requests, list(range(1, 12)))
        self.assertEqual(target["result"]["verified"]["unique_observation_count"], 1001)

    def test_shared_deadline_bounds_all_traces_and_http_fallbacks(self):
        targets = [self.verification_fixture()[0]]
        targets.extend(self.verification_fixture(subagent=True)[0] for _ in range(100))
        for index, target in enumerate(targets):
            target["result"]["trace_id"] = f"trace-{index}"
        now = [0]
        timeouts = []

        def fake_urlopen(request, timeout):
            timeouts.append(timeout)
            now[0] += timeout
            raise TimeoutError("slow read")

        with mock.patch.object(MODULE.time, "monotonic", side_effect=lambda: now[0]), mock.patch.object(
            MODULE.urllib.request, "urlopen", side_effect=fake_urlopen
        ), mock.patch.object(MODULE.time, "sleep") as sleep:
            with self.assertRaisesRegex(RuntimeError, "trace-100"):
                MODULE.verify_traces(
                    self.verification_args(verify_timeout_seconds=35), "public", "secret", targets
                )
        self.assertEqual(timeouts, [30, 5])
        self.assertEqual(now[0], 35)
        sleep.assert_not_called()
        self.assertEqual(targets[-1]["result"]["verification_diagnostic"]["read_source"], "not_polled")

    def test_shared_deadline_clips_sleep_and_stops_before_next_round(self):
        targets = [self.verification_fixture()[0], self.verification_fixture(subagent=True)[0]]
        now = [0]

        def fake_sleep(seconds):
            now[0] += seconds

        with mock.patch.object(MODULE.time, "monotonic", side_effect=lambda: now[0]), mock.patch.object(
            MODULE, "fetch_trace", return_value={}
        ), mock.patch.object(
            MODULE, "fetch_observations_legacy", return_value={"data": []}
        ) as fetch, mock.patch.object(MODULE.time, "sleep", side_effect=fake_sleep) as sleep:
            with self.assertRaises(RuntimeError):
                MODULE.verify_traces(
                    self.verification_args(verify_timeout_seconds=2), "public", "secret", targets
                )
        self.assertEqual(fetch.call_count, 2)
        sleep.assert_called_once_with(2)

    def test_pagination_checks_remaining_deadline_before_every_request(self):
        for reader in (MODULE.fetch_observations_legacy, MODULE.fetch_observations_v2):
            with self.subTest(reader=reader.__name__):
                now = [0]
                timeouts = []

                def fake_urlopen(request, timeout):
                    timeouts.append(timeout)
                    now[0] += 3
                    return JsonResponse({"data": [{}] * 100, "meta": {"cursor": "next"}})

                with mock.patch.object(
                    MODULE.time, "monotonic", side_effect=lambda: now[0]
                ), mock.patch.object(MODULE.urllib.request, "urlopen", side_effect=fake_urlopen):
                    with self.assertRaisesRegex(TimeoutError, "deadline exhausted"):
                        reader("https://litefuse.example", "public", "secret", "trace", deadline=5)
                self.assertEqual(timeouts, [5, 2])

    def test_main_exports_and_verifies_all_traces_with_failure_diagnostics(self):
        for mode in ("success", "missing_subagent", "main_only", "no_verify", "dry_run"):
            with self.subTest(mode=mode):
                argv = [
                    "exporter", "--input-file", "prompt", "--events-file", "events",
                    "--session-id", "run-test", "--verify-attempts", "1",
                    "--verify-sleep-seconds", "0",
                ]
                if mode != "main_only":
                    argv.extend(["--subagent-sessions-dir", "/unused"])
                if mode != "no_verify":
                    argv.append("--verify")
                if mode == "dry_run":
                    argv.append("--dry-run")
                with mock.patch.object(sys, "argv", argv):
                    args = MODULE.parse_args()
                events = [
                    {
                        "type": "item.completed", "_line_number": index + 1,
                        "item": {"type": "agent_message", "text": text, "id": str(index)},
                    }
                    for index, text in enumerate(("reviewing", "done"))
                ]
                session_events = [
                    {"type": "session_meta", "payload": {"id": "child-thread"}},
                    {
                        "type": "response_item",
                        "payload": {"type": "message", "role": "assistant", "content": "done"},
                    },
                ]
                child = MODULE.build_subagent_session_payload(args, "child.jsonl", session_events)
                stored_rows = {}
                requests = []

                def fake_urlopen(request, timeout):
                    requests.append(request)
                    if request.method == "POST":
                        payload = json.loads(request.data)
                        for resource in payload["resourceSpans"]:
                            for scope in resource["scopeSpans"]:
                                for span in scope["spans"]:
                                    attrs = attribute_values(span)
                                    row = {"id": span["spanId"], "name": span["name"]}
                                    for field in ("input", "output"):
                                        value = attrs.get(f"langfuse.observation.{field}")
                                        if value is not None:
                                            row[field] = json.loads(value)
                                    stored_rows.setdefault(span["traceId"], []).append(row)
                        return FakeResponse()
                    if "/api/public/traces/" in request.full_url:
                        return JsonResponse({})
                    query = urllib.parse.parse_qs(urllib.parse.urlparse(request.full_url).query)
                    trace_id = query["traceId"][0]
                    rows = stored_rows[trace_id]
                    if mode == "missing_subagent" and trace_id == child["trace_id"]:
                        rows = rows[:-1]
                    return JsonResponse({"data": rows})

                stdout = io.StringIO()
                with mock.patch.object(MODULE, "parse_args", return_value=args), mock.patch.object(
                    MODULE, "read_text", return_value="review task"
                ), mock.patch.object(MODULE, "load_jsonl", return_value=events), mock.patch.object(
                    MODULE, "build_subagent_session_payloads", return_value=[child]
                ) as build_children, mock.patch.object(
                    MODULE.urllib.request, "urlopen", side_effect=fake_urlopen
                ), mock.patch.dict(
                    MODULE.os.environ,
                    {} if mode == "dry_run" else {"LANGFUSE_PUBLIC_KEY": "public", "LANGFUSE_SECRET_KEY": "secret"},
                    clear=True,
                ), redirect_stdout(stdout):
                    if mode == "missing_subagent":
                        with self.assertRaisesRegex(RuntimeError, child["trace_id"]):
                            MODULE.main()
                    else:
                        MODULE.main()
                result = json.loads(stdout.getvalue())
                if mode == "dry_run":
                    self.assertEqual(requests, [])
                    self.assertEqual(result["subagent_trace_count"], 1)
                elif mode == "no_verify":
                    self.assertTrue(requests)
                    self.assertTrue(all(request.method == "POST" for request in requests))
                    self.assertNotIn("verified", result)
                    self.assertNotIn("verified", result["subagent_traces"][0])
                else:
                    self.assertEqual(result["verified"]["agent_message_with_turn_input_count"], 1)
                    self.assertEqual(result["verified"]["agent_message_with_previous_count"], 1)
                    if mode == "main_only":
                        build_children.assert_not_called()
                        self.assertEqual(result["subagent_traces"], [])
                    elif mode == "missing_subagent":
                        self.assertIn("verification_error", result)
                        diagnostic = result["subagent_traces"][0]["verification_diagnostic"]
                        self.assertEqual(diagnostic["missing_expected_observation_count"], 1)
                        self.assertNotIn("verified", result["subagent_traces"][0])
                    else:
                        self.assertEqual(result["subagent_traces"][0]["verified"]["observation_count"], 3)

    def test_main_still_rejects_missing_agent_message_context(self):
        target, rows, _ = self.verification_fixture()
        rows[-1]["name"] = "codex.agent_message"
        with mock.patch.object(MODULE, "fetch_trace", return_value={}), mock.patch.object(
            MODULE, "fetch_observations_legacy", return_value={"data": rows}
        ):
            with self.assertRaisesRegex(RuntimeError, '"agent_message_all_have_context_window": false'):
                MODULE.verify_traces(
                    self.verification_args(verify_attempts=1), "public", "secret", [target]
                )

    def test_verify_complete_main_trace_uses_legacy_observations(self):
        args = mock.Mock(
            base_url="https://litefuse.example",
            verify_attempts=1,
            verify_timeout_seconds=120,
            verify_sleep_seconds=0,
            min_observations=3,
            min_step_observations=1,
        )
        observations = [
            {
                "id": "review",
                "name": "codex.review",
                "input": {"prompt": "p"},
                "output": {"text": "o"},
            },
            {
                "id": "turn",
                "name": "codex.turn",
                "input": {"prompt": "p"},
                "output": {"text": "o"},
            },
            {
                "id": "command",
                "name": "codex.command",
                "input": {"command": "pwd"},
                "output": {"status": "ok"},
            },
        ]

        with mock.patch.object(MODULE, "fetch_trace", return_value={}), mock.patch.object(
            MODULE, "fetch_observations_legacy", return_value={"data": observations}
        ), mock.patch.object(MODULE, "fetch_observations_v2") as v2_fetch:
            result = self.verify_single_trace(
                args, "public", "secret", "trace-id", len(observations)
            )

        self.assertEqual(result["read_source"], "legacy_observations")
        self.assertEqual(result["required_observation_count"], len(observations))
        v2_fetch.assert_not_called()

    def test_verify_rejects_partially_visible_trace(self):
        args = mock.Mock(
            base_url="https://litefuse.example",
            verify_attempts=1,
            verify_timeout_seconds=120,
            verify_sleep_seconds=0,
            min_observations=1,
            min_step_observations=1,
        )
        observations = [
            {
                "id": "review",
                "name": "codex.review",
                "input": {"prompt": "p"},
                "output": {"text": "o"},
            },
            {
                "id": "command",
                "name": "codex.command",
                "input": {"command": "pwd"},
                "output": {"status": "ok"},
            },
        ]

        with mock.patch.object(MODULE, "fetch_trace", return_value={}), mock.patch.object(
            MODULE, "fetch_observations_legacy", return_value={"data": observations}
        ), mock.patch.object(MODULE, "fetch_observations_v2"):
            with self.assertRaisesRegex(
                RuntimeError, '"required_observation_count": 3'
            ):
                self.verify_single_trace(
                    args, "public", "secret", "trace-id", 3
                )

    def test_verify_rejects_duplicate_observation_ids(self):
        args = mock.Mock(
            base_url="https://litefuse.example",
            verify_attempts=1,
            verify_timeout_seconds=120,
            verify_sleep_seconds=0,
            min_observations=1,
            min_step_observations=1,
        )
        observations = [
            {
                "id": "review",
                "name": "codex.review",
                "input": {"prompt": "p"},
                "output": {"text": "o"},
            },
            {
                "id": "command",
                "name": "codex.command",
                "input": {"command": "pwd"},
                "output": {"status": "ok"},
            },
            {
                "id": "command",
                "name": "codex.command",
                "input": {"command": "pwd"},
                "output": {"status": "ok"},
            },
        ]

        with mock.patch.object(MODULE, "fetch_trace", return_value={}), mock.patch.object(
            MODULE, "fetch_observations_legacy", return_value={"data": observations}
        ), mock.patch.object(MODULE, "fetch_observations_v2"):
            with self.assertRaisesRegex(
                RuntimeError, '"duplicate_observation_count": 1'
            ):
                self.verify_single_trace(
                    args, "public", "secret", "trace-id", 2
                )

    def test_verify_rejects_missing_or_empty_observation_ids(self):
        args = SimpleNamespace(
            base_url="https://litefuse.example",
            verify_attempts=1,
            verify_timeout_seconds=120,
            verify_sleep_seconds=0,
            min_observations=1,
            min_step_observations=1,
        )
        complete_observations = [
            {
                "id": "review",
                "name": "codex.review",
                "input": {"prompt": "p"},
                "output": {"text": "o"},
            },
            {
                "id": "command",
                "name": "codex.command",
                "input": {"command": "pwd"},
                "output": {"status": "ok"},
            },
        ]
        for id_fields in ({}, {"id": None}, {"id": ""}):
            with self.subTest(id_fields=id_fields):
                observations = [
                    *complete_observations,
                    {
                        **id_fields,
                        "name": "codex.command",
                        "input": {"command": "git status"},
                        "output": {"status": "ok"},
                    },
                ]
                with mock.patch.object(
                    MODULE, "fetch_trace", return_value={}
                ), mock.patch.object(
                    MODULE, "fetch_observations_legacy", return_value={"data": observations}
                ), mock.patch.object(MODULE, "fetch_observations_v2"):
                    # The two valid IDs already satisfy the count requirement;
                    # the additional ID-less row must independently fail verification.
                    with self.assertRaisesRegex(
                        RuntimeError, '"observations_missing_id_count": 1'
                    ):
                        self.verify_single_trace(args, "public", "secret", "trace-id", 2)


if __name__ == "__main__":
    unittest.main()
