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
import io
import json
from pathlib import Path
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

    def test_paginates_v2_observation_readback(self):
        responses = [
            JsonResponse({"data": [{"id": "newest"}], "meta": {"cursor": "next"}}),
            JsonResponse({"data": [{"id": "oldest"}], "meta": {}}),
        ]
        requests = []

        def fake_urlopen(request, timeout):
            requests.append((request, timeout))
            return responses.pop(0)

        with mock.patch.object(MODULE.urllib.request, "urlopen", fake_urlopen):
            payload = MODULE.fetch_observations_v2(
                "https://litefuse.example", "public", "secret", "trace-id"
            )

        self.assertEqual(payload["data"], [{"id": "newest"}, {"id": "oldest"}])
        first_query = urllib.parse.parse_qs(
            urllib.parse.urlparse(requests[0][0].full_url).query
        )
        second_query = urllib.parse.parse_qs(
            urllib.parse.urlparse(requests[1][0].full_url).query
        )
        self.assertEqual(first_query["limit"], ["1000"])
        self.assertNotIn("cursor", first_query)
        self.assertEqual(second_query["cursor"], ["next"])
        self.assertEqual([timeout for _request, timeout in requests], [30, 30])

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

    def test_verify_prefers_v2_observations(self):
        args = mock.Mock(
            base_url="https://litefuse.example",
            verify_attempts=1,
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
            MODULE, "fetch_observations_v2", return_value={"data": observations}
        ), mock.patch.object(MODULE, "fetch_observations_legacy") as legacy_fetch:
            result = MODULE.verify_trace(
                args, "public", "secret", "trace-id", len(observations)
            )

        self.assertEqual(result["read_source"], "v2_observations")
        self.assertEqual(result["required_observation_count"], len(observations))
        legacy_fetch.assert_not_called()

    def test_verify_rejects_partially_visible_trace(self):
        args = mock.Mock(
            base_url="https://litefuse.example",
            verify_attempts=1,
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
            MODULE, "fetch_observations_v2", return_value={"data": observations}
        ), mock.patch.object(MODULE, "fetch_observations_legacy"):
            with self.assertRaisesRegex(
                RuntimeError, '"required_observation_count": 3'
            ):
                MODULE.verify_trace(
                    args, "public", "secret", "trace-id", 3
                )

    def test_verify_rejects_duplicate_observation_ids(self):
        args = mock.Mock(
            base_url="https://litefuse.example",
            verify_attempts=1,
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
            MODULE, "fetch_observations_v2", return_value={"data": observations}
        ), mock.patch.object(MODULE, "fetch_observations_legacy"):
            with self.assertRaisesRegex(
                RuntimeError, '"duplicate_observation_count": 1'
            ):
                MODULE.verify_trace(
                    args, "public", "secret", "trace-id", 2
                )


if __name__ == "__main__":
    unittest.main()
