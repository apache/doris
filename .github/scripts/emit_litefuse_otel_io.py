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

import argparse
import base64
from datetime import datetime, timedelta, timezone
import json
import os
import secrets
import time
import urllib.error
import urllib.parse
import urllib.request


def read_text(path, max_chars, tail=False, optional=False):
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            data = handle.read()
    except FileNotFoundError:
        if optional:
            return ""
        raise
    return truncate_text(data, max_chars, tail)


def truncate_text(data, max_chars, tail=False):
    if max_chars <= 0 or len(data) <= max_chars:
        return data
    if tail:
        return f"[truncated to last {max_chars} chars]\n" + data[-max_chars:]
    return data[:max_chars] + f"\n[truncated to first {max_chars} chars]"


def truncate_json(value, max_chars):
    text = json.dumps(value, ensure_ascii=False, sort_keys=True)
    if max_chars <= 0 or len(text) <= max_chars:
        return value
    return {"truncated_json": truncate_text(text, max_chars)}


def json_attr(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def json_payload_bytes(value):
    return len(
        json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    )


def load_jsonl(path):
    events = []
    with open(path, "r", encoding="utf-8", errors="replace") as handle:
        for line_number, line in enumerate(handle, start=1):
            stripped = line.strip()
            if not stripped:
                continue
            try:
                event = json.loads(stripped)
            except json.JSONDecodeError as exc:
                events.append(
                    {
                        "type": "error",
                        "message": f"invalid jsonl line {line_number}: {exc}",
                    }
                )
                continue
            event["_line_number"] = line_number
            events.append(event)
    return events


def find_final_message(events, output_text):
    if output_text.strip():
        return output_text
    for event in reversed(events):
        item = event.get("item") or {}
        if event.get("type") == "item.completed" and item.get("type") == "agent_message":
            text = item.get("text") or ""
            if text.strip():
                return text
    return ""


def latest_turn_result(events):
    for event in reversed(events):
        if event.get("type") == "turn.completed":
            return "completed", event.get("usage") or {}
        if event.get("type") == "turn.failed":
            return "failed", event.get("error") or {}
    return "unknown", {}


def event_context_payload(event, max_json_chars):
    item = event.get("item") if isinstance(event.get("item"), dict) else None
    payload = {
        "event_type": event.get("type"),
        "line_number": event.get("_line_number"),
    }
    if not item:
        event_payload = {
            key: value for key, value in event.items() if key != "_line_number"
        }
        payload["payload"] = truncate_json(event_payload, max_json_chars)
        return payload

    item_type = item.get("type", "unknown")
    payload.update(
        {
            "item_id": item.get("id", ""),
            "item_type": item_type,
            "status": item.get("status"),
        }
    )

    if item_type == "command_execution":
        payload.update(
            {
                "command": item.get("command") or "",
                "exit_code": item.get("exit_code"),
                "aggregated_output": truncate_text(
                    item.get("aggregated_output") or "", max_json_chars, tail=True
                ),
            }
        )
    elif item_type == "mcp_tool_call":
        payload.update(
            {
                "server": item.get("server"),
                "tool": item.get("tool"),
                "arguments": truncate_json(item.get("arguments"), max_json_chars),
                "result": truncate_json(item.get("result"), max_json_chars),
                "error": truncate_json(item.get("error"), max_json_chars),
            }
        )
    elif item_type == "collab_tool_call":
        payload.update(
            {
                "tool": item.get("tool"),
                "prompt": truncate_text(item.get("prompt") or "", max_json_chars),
                "sender_thread_id": item.get("sender_thread_id"),
                "receiver_thread_ids": item.get("receiver_thread_ids") or [],
                "agents_states": truncate_json(item.get("agents_states"), max_json_chars),
            }
        )
    elif item_type == "agent_message":
        payload["text"] = truncate_text(item.get("text") or "", max_json_chars)
    else:
        payload["item"] = truncate_json(item, max_json_chars)

    return {key: value for key, value in payload.items() if value not in (None, "")}


def build_agent_message_inputs(
    events, turn_input, max_json_chars, max_context_json_chars
):
    inputs = {}
    previous_agent_message = None
    context_events = []
    for event in events:
        item = event.get("item") if isinstance(event.get("item"), dict) else {}
        is_agent_message = (
            event.get("type") == "item.completed"
            and item.get("type") == "agent_message"
        )
        if is_agent_message:
            current_id = item.get("id") or f"line:{event.get('_line_number', 0)}"
            input_payload = {
                "item_type": "agent_message",
                "context_window": {
                    "from": (
                        previous_agent_message.get("id")
                        if previous_agent_message
                        else "turn_start"
                    ),
                    "to": current_id,
                    "event_count": len(context_events),
                },
                "events_since_previous_agent_message": [
                    event_context_payload(context_event, max_context_json_chars)
                    for context_event in context_events
                ],
            }
            if previous_agent_message:
                input_payload["previous_agent_message"] = {
                    "id": previous_agent_message.get("id", ""),
                    "text": truncate_text(
                        previous_agent_message.get("text") or "", max_json_chars
                    ),
                }
            else:
                input_payload["turn_input"] = truncate_text(turn_input, max_json_chars)
            inputs[event.get("_line_number")] = input_payload
            previous_agent_message = item
            context_events = []
        else:
            context_events.append(event)
    return inputs


def observation_shape(item, max_json_chars, agent_message_input=None):
    item_type = item.get("type", "unknown")
    status = item.get("status")

    if item_type == "command_execution":
        command = item.get("command") or ""
        return {
            "name": "codex.command",
            "type": "span",
            "input": {"command": command},
            "output": {
                "status": status,
                "exit_code": item.get("exit_code"),
                "aggregated_output": truncate_text(
                    item.get("aggregated_output") or "", max_json_chars, tail=True
                ),
            },
            "metadata": {"command": command[:240], "status": status, "step_kind": "tool"},
        }

    if item_type == "mcp_tool_call":
        return {
            "name": f"codex.mcp.{item.get('server', 'unknown')}.{item.get('tool', 'unknown')}",
            "type": "span",
            "input": {
                "server": item.get("server"),
                "tool": item.get("tool"),
                "arguments": truncate_json(item.get("arguments"), max_json_chars),
            },
            "output": {
                "status": status,
                "result": truncate_json(item.get("result"), max_json_chars),
                "error": truncate_json(item.get("error"), max_json_chars),
            },
            "metadata": {
                "server": item.get("server"),
                "tool": item.get("tool"),
                "status": status,
                "step_kind": "tool",
            },
        }

    if item_type == "collab_tool_call":
        return {
            "name": f"codex.collab.{item.get('tool', 'unknown')}",
            "type": "span",
            "input": {
                "tool": item.get("tool"),
                "prompt": truncate_text(item.get("prompt") or "", max_json_chars),
                "sender_thread_id": item.get("sender_thread_id"),
            },
            "output": {
                "status": status,
                "receiver_thread_ids": item.get("receiver_thread_ids") or [],
                "agents_states": truncate_json(item.get("agents_states"), max_json_chars),
            },
            "metadata": {"tool": item.get("tool"), "status": status, "step_kind": "tool"},
        }

    if item_type == "web_search":
        return {
            "name": "codex.web_search",
            "type": "span",
            "input": {"query": item.get("query"), "action": item.get("action")},
            "output": {"status": "completed", "query": item.get("query")},
            "metadata": {"query": (item.get("query") or "")[:240], "step_kind": "tool"},
        }

    if item_type == "file_change":
        return {
            "name": "codex.file_change",
            "type": "span",
            "input": {"changes": item.get("changes") or []},
            "output": {"status": status, "changes": item.get("changes") or []},
            "metadata": {"status": status},
        }

    if item_type == "todo_list":
        return {
            "name": "codex.todo_list",
            "type": "span",
            "input": {"item_type": item_type},
            "output": {"items": item.get("items") or []},
            "metadata": {"item_count": len(item.get("items") or [])},
        }

    if item_type == "reasoning":
        return {
            "name": "codex.reasoning",
            "type": "span",
            "input": {"item_type": item_type},
            "output": {"text": truncate_text(item.get("text") or "", max_json_chars)},
            "metadata": {},
        }

    if item_type == "agent_message":
        return {
            "name": "codex.agent_message",
            "type": "generation",
            "input": agent_message_input or {"item_type": item_type},
            "output": {"text": truncate_text(item.get("text") or "", max_json_chars)},
            "metadata": {},
        }

    if item_type == "error":
        return {
            "name": "codex.error",
            "type": "span",
            "input": {"item_type": item_type},
            "output": {"message": item.get("message") or ""},
            "metadata": {},
        }

    return {
        "name": f"codex.{item_type}",
        "type": "span",
        "input": {"item_type": item_type},
        "output": truncate_json(item, max_json_chars),
        "metadata": {"item_type": item_type},
    }


def iso_from_ns(ns):
    return (
        datetime.fromtimestamp(ns / 1_000_000_000, timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


def ingestion_event(event_type, timestamp, body):
    return {
        "id": secrets.token_hex(16),
        "timestamp": timestamp,
        "type": event_type,
        "body": body,
    }


def build_ingestion_payload(args, input_text, output_text, events):
    now = time.time_ns()
    trace_id = secrets.token_hex(16)
    root_observation_id = secrets.token_hex(16)
    turn_status, turn_payload = latest_turn_result(events)
    final_message = find_final_message(events, output_text)
    trace_output = final_message or json_attr({"turn_status": turn_status})

    trace_metadata = {
        "repository": args.repository,
        "workflow": args.workflow,
        "run_id": args.run_id,
        "pr_number": args.pr_number,
        "head_sha": args.head_sha,
        "base_sha": args.base_sha,
        "model_reasoning_effort": args.reasoning_effort,
        "codex_jsonl": True,
    }
    trace_metadata = {
        key: value for key, value in trace_metadata.items() if value not in (None, "")
    }

    completed_items = [
        event
        for event in events
        if event.get("type") == "item.completed" and isinstance(event.get("item"), dict)
    ]
    agent_message_inputs = build_agent_message_inputs(
        events, input_text, args.max_json_chars, args.max_context_json_chars
    )
    child_count = len(completed_items) + 1
    root_end = now + (child_count + 2) * 1_000_000
    timestamp = iso_from_ns(now)

    batch = [
        ingestion_event(
            "trace-create",
            timestamp,
            {
                "id": trace_id,
                "timestamp": timestamp,
                "name": args.trace_name,
                "input": input_text,
                "output": trace_output,
                "sessionId": args.session_id,
                "environment": args.environment,
                "metadata": trace_metadata,
                "tags": ["doris-ai-review", "codex-jsonl"],
            },
        ),
        ingestion_event(
            "span-create",
            timestamp,
            {
                "id": root_observation_id,
                "traceId": trace_id,
                "name": "codex.review",
                "startTime": iso_from_ns(now),
                "endTime": iso_from_ns(root_end),
                "input": {"prompt": input_text},
                "output": {"final_message": trace_output, "turn_status": turn_status},
                "environment": args.environment,
                "metadata": {
                    **trace_metadata,
                    "codex_event_count": len(events),
                    "codex_completed_item_count": len(completed_items),
                },
            },
        ),
    ]

    turn_body = {
        "id": secrets.token_hex(16),
        "traceId": trace_id,
        "parentObservationId": root_observation_id,
        "name": "codex.turn",
        "startTime": iso_from_ns(now + 1_000_000),
        "endTime": iso_from_ns(now + 2_000_000),
        "model": args.model,
        "input": {"prompt": input_text},
        "output": {"status": turn_status, "final_message": trace_output},
        "environment": args.environment,
        "metadata": {**trace_metadata, "item_type": "turn"},
        "level": "ERROR" if turn_status == "failed" else "DEFAULT",
    }
    if turn_status == "completed":
        turn_body["usageDetails"] = {
            "input": int(turn_payload.get("input_tokens") or 0),
            "output": int(turn_payload.get("output_tokens") or 0),
            "cache_read_input_tokens": int(turn_payload.get("cached_input_tokens") or 0),
            "reasoning_output_tokens": int(
                turn_payload.get("reasoning_output_tokens") or 0
            ),
        }
    else:
        turn_body["statusMessage"] = json_attr(turn_payload)

    batch.append(ingestion_event("generation-create", iso_from_ns(now + 1_000_000), turn_body))

    for offset, event in enumerate(completed_items, start=2):
        item = event["item"]
        shape = observation_shape(
            item, args.max_json_chars, agent_message_inputs.get(event.get("_line_number"))
        )
        item_type = item.get("type", "unknown")
        item_status = item.get("status", "completed")
        body = {
            "id": secrets.token_hex(16),
            "traceId": trace_id,
            "parentObservationId": root_observation_id,
            "name": shape["name"],
            "startTime": iso_from_ns(now + offset * 1_000_000),
            "endTime": iso_from_ns(now + offset * 1_000_000 + 750_000),
            "input": shape["input"],
            "output": shape["output"],
            "environment": args.environment,
            "level": "ERROR" if item_status in ("failed", "declined") else "DEFAULT",
            "metadata": {
                **trace_metadata,
                "item_id": item.get("id", ""),
                "item_type": item_type,
                "event_line": event.get("_line_number", 0),
            },
        }
        for key, value in shape.get("metadata", {}).items():
            if value not in (None, ""):
                body["metadata"][key] = value
        event_type = "generation-create" if shape["type"] == "generation" else "span-create"
        if shape["type"] == "generation":
            body["model"] = args.model
        batch.append(ingestion_event(event_type, body["startTime"], body))

    return trace_id, {"batch": batch}, len(batch) - 1


def main_trace_metadata(args):
    trace_metadata = {
        "repository": args.repository,
        "workflow": args.workflow,
        "run_id": args.run_id,
        "pr_number": args.pr_number,
        "head_sha": args.head_sha,
        "base_sha": args.base_sha,
        "model_reasoning_effort": args.reasoning_effort,
    }
    return {
        key: value for key, value in trace_metadata.items() if value not in (None, "")
    }


def receiver_thread_ids(events):
    thread_ids = set()
    for event in events:
        item = event.get("item") if isinstance(event.get("item"), dict) else {}
        if item.get("type") != "collab_tool_call":
            continue
        for thread_id in item.get("receiver_thread_ids") or []:
            if thread_id:
                thread_ids.add(str(thread_id))
    return thread_ids


def session_jsonl_files(root):
    if not root or not os.path.isdir(root):
        return []
    paths = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            if filename.endswith(".jsonl"):
                paths.append(os.path.join(dirpath, filename))
    return sorted(paths)


def session_meta(events):
    for event in events:
        if event.get("type") != "session_meta":
            continue
        payload = event.get("payload")
        if isinstance(payload, dict):
            return payload
    return {}


def subagent_spawn(meta):
    source = meta.get("source") if isinstance(meta, dict) else {}
    source = source if isinstance(source, dict) else {}
    subagent = source.get("subagent")
    subagent = subagent if isinstance(subagent, dict) else {}
    spawn = subagent.get("thread_spawn")
    return spawn if isinstance(spawn, dict) else {}


def is_subagent_meta(meta, expected_thread_ids):
    if not isinstance(meta, dict):
        return False
    thread_id = str(meta.get("id") or "")
    if expected_thread_ids:
        return thread_id in expected_thread_ids
    if meta.get("thread_source") == "subagent":
        return True
    source = meta.get("source")
    return isinstance(source, dict) and bool(source.get("subagent"))


def compact_session_meta(meta, max_json_chars):
    compact = {}
    for key in (
        "id",
        "parent_thread_id",
        "timestamp",
        "cwd",
        "originator",
        "cli_version",
        "thread_source",
        "agent_nickname",
        "agent_role",
        "model_provider",
        "model",
    ):
        if meta.get(key) not in (None, ""):
            compact[key] = meta.get(key)
    for key in ("base_instructions", "user_instructions"):
        if key in meta:
            compact[f"{key}_present"] = True
    source = meta.get("source")
    if isinstance(source, dict):
        compact["source"] = truncate_json(source, max_json_chars)
    return compact


def session_content_text(content, max_chars):
    if isinstance(content, str):
        return truncate_text(content, max_chars)
    if not isinstance(content, list):
        return truncate_json(content, max_chars)
    parts = []
    for item in content:
        if isinstance(item, str):
            parts.append(item)
            continue
        if not isinstance(item, dict):
            parts.append(json_attr(item))
            continue
        for key in ("text", "input_text", "output_text"):
            if isinstance(item.get(key), str):
                parts.append(item[key])
                break
        else:
            parts.append(json_attr(item))
    return truncate_text("\n".join(parts), max_chars)


def session_first_user_message(events, max_chars):
    for event in events:
        if event.get("type") != "response_item":
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if payload.get("type") == "message" and payload.get("role") == "user":
            text = session_content_text(payload.get("content"), max_chars)
            if isinstance(text, str) and text.strip():
                return text
    return ""


def session_final_assistant_message(events, max_chars):
    for event in reversed(events):
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if event.get("type") == "response_item":
            if payload.get("type") == "message" and payload.get("role") == "assistant":
                text = session_content_text(payload.get("content"), max_chars)
                if isinstance(text, str) and text.strip():
                    return text
        elif event.get("type") == "event_msg" and payload.get("type") == "agent_message":
            text = payload.get("message") or ""
            if text.strip():
                return truncate_text(text, max_chars)
    return ""


def safe_name_component(value):
    text = str(value or "unknown")
    safe = "".join(
        char if char.isalnum() or char in "._-" else "_" for char in text
    ).strip("._-")
    return safe[:120] or "unknown"


def jsonish(value):
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def session_call_outputs(events):
    outputs = {}
    for event in events:
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if payload.get("type") != "function_call_output":
            continue
        call_id = payload.get("call_id")
        if call_id and call_id not in outputs:
            outputs[call_id] = (payload, event)
    return outputs


def session_event_timestamp(event, fallback_ns):
    timestamp = event.get("timestamp")
    if isinstance(timestamp, str) and timestamp:
        return timestamp
    return iso_from_ns(fallback_ns)


def ns_from_iso(timestamp):
    if not isinstance(timestamp, str) or not timestamp:
        return None
    try:
        normalized = timestamp.replace("Z", "+00:00")
        return int(datetime.fromisoformat(normalized).timestamp() * 1_000_000_000)
    except ValueError:
        return None


def session_observation_shape(event, call_outputs, max_json_chars):
    event_type = event.get("type") or "unknown"
    payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}

    if event_type == "session_meta":
        compact_meta = compact_session_meta(payload, max_json_chars)
        return {
            "name": "codex.subagent.session_meta",
            "type": "span",
            "input": compact_meta,
            "output": {
                "thread_source": compact_meta.get("thread_source"),
                "status": "recorded",
            },
            "metadata": {"session_event_type": event_type},
        }

    if event_type == "turn_context":
        return {
            "name": "codex.subagent.turn_context",
            "type": "span",
            "input": truncate_json(payload, max_json_chars),
            "output": {"status": "recorded"},
            "metadata": {"session_event_type": event_type},
        }

    if event_type == "event_msg":
        message_type = payload.get("type") or "unknown"
        message = payload.get("message") or ""
        body = {
            key: value
            for key, value in payload.items()
            if key not in ("message", "text_elements", "images", "local_images")
        }
        if payload.get("text_elements"):
            body["text_elements"] = truncate_json(
                payload.get("text_elements"), max_json_chars
            )
        if payload.get("images"):
            body["images"] = truncate_json(payload.get("images"), max_json_chars)
        return {
            "name": f"codex.subagent.event.{safe_name_component(message_type)}",
            "type": "generation" if message_type == "agent_message" else "span",
            "input": {
                "session_event_type": event_type,
                "message_type": message_type,
                "payload": truncate_json(body, max_json_chars),
            },
            "output": {
                "message": truncate_text(message, max_json_chars),
                "status": "recorded",
            },
            "metadata": {
                "session_event_type": event_type,
                "message_type": message_type,
            },
        }

    if event_type != "response_item":
        return {
            "name": f"codex.subagent.{safe_name_component(event_type)}",
            "type": "span",
            "input": {"session_event_type": event_type},
            "output": truncate_json(payload, max_json_chars),
            "metadata": {"session_event_type": event_type},
        }

    item_type = payload.get("type") or "unknown"
    if item_type == "message":
        role = payload.get("role") or "unknown"
        text = session_content_text(payload.get("content"), max_json_chars)
        is_assistant = role == "assistant"
        return {
            "name": f"codex.subagent.message.{safe_name_component(role)}",
            "type": "generation" if is_assistant else "span",
            "input": {
                "session_event_type": event_type,
                "item_type": item_type,
                "role": role,
                "phase": payload.get("phase"),
                **({} if is_assistant else {"content": text}),
            },
            "output": (
                {"text": text}
                if is_assistant
                else {"status": "recorded", "role": role}
            ),
            "metadata": {
                "session_event_type": event_type,
                "item_type": item_type,
                "role": role,
                "phase": payload.get("phase"),
            },
        }

    if item_type == "function_call":
        call_id = payload.get("call_id")
        output_payload, output_event = call_outputs.get(call_id, ({}, {}))
        return {
            "name": f"codex.subagent.tool.{safe_name_component(payload.get('name'))}",
            "type": "span",
            "input": {
                "name": payload.get("name"),
                "call_id": call_id,
                "arguments": truncate_json(
                    jsonish(payload.get("arguments")), max_json_chars
                ),
            },
            "output": {
                "call_id": call_id,
                "output": truncate_json(
                    jsonish(output_payload.get("output")), max_json_chars
                ),
                "status": "completed" if output_payload else "unknown",
            },
            "metadata": {
                "session_event_type": event_type,
                "item_type": item_type,
                "tool_name": payload.get("name"),
                "call_id": call_id,
                "output_line": output_event.get("_line_number"),
            },
        }

    if item_type == "function_call_output":
        return {
            "name": "codex.subagent.tool_output",
            "type": "span",
            "input": {"call_id": payload.get("call_id")},
            "output": truncate_json(jsonish(payload.get("output")), max_json_chars),
            "metadata": {
                "session_event_type": event_type,
                "item_type": item_type,
                "call_id": payload.get("call_id"),
            },
        }

    if item_type == "reasoning":
        return {
            "name": "codex.subagent.reasoning",
            "type": "span",
            "input": {
                "session_event_type": event_type,
                "item_type": item_type,
                "encrypted_content_present": bool(payload.get("encrypted_content")),
            },
            "output": {"summary": truncate_json(payload.get("summary"), max_json_chars)},
            "metadata": {"session_event_type": event_type, "item_type": item_type},
        }

    return {
        "name": f"codex.subagent.response_item.{safe_name_component(item_type)}",
        "type": "span",
        "input": {"session_event_type": event_type, "item_type": item_type},
        "output": truncate_json(payload, max_json_chars),
        "metadata": {"session_event_type": event_type, "item_type": item_type},
    }


def build_subagent_session_payload(args, session_path, session_events):
    now = time.time_ns()
    meta = session_meta(session_events)
    spawn = subagent_spawn(meta)
    thread_id = str(meta.get("id") or os.path.basename(session_path))
    parent_thread_id = (
        spawn.get("parent_thread_id") or meta.get("parent_thread_id") or ""
    )
    agent_nickname = meta.get("agent_nickname") or spawn.get("agent_nickname") or ""
    agent_role = meta.get("agent_role") or spawn.get("agent_role") or ""
    trace_id = secrets.token_hex(16)
    root_observation_id = secrets.token_hex(16)
    trace_input = session_first_user_message(session_events, args.max_input_chars)
    if not trace_input:
        trace_input = json_attr(compact_session_meta(meta, args.max_json_chars))
    trace_output = session_final_assistant_message(session_events, args.max_output_chars)
    if not trace_output:
        trace_output = json_attr({"session_status": "recorded"})

    trace_metadata = {
        **main_trace_metadata(args),
        "codex_session_jsonl": True,
        "subagent_session": True,
        "main_session_id": args.session_id,
        "thread_id": thread_id,
        "parent_thread_id": parent_thread_id,
        "agent_nickname": agent_nickname,
        "agent_role": agent_role,
        "session_file": session_path,
        "session_event_count": len(session_events),
    }
    trace_metadata = {
        key: value for key, value in trace_metadata.items() if value not in (None, "")
    }
    trace_session_id = f"{args.session_id}:subagent:{thread_id}"
    first_timestamp = (
        session_event_timestamp(session_events[0], now)
        if session_events
        else iso_from_ns(now)
    )
    base_ns = ns_from_iso(first_timestamp) or now
    event_start_nses = [
        ns_from_iso(event.get("timestamp")) or base_ns + offset * 1_000_000
        for offset, event in enumerate(session_events, start=2)
    ]
    latest_event_start_ns = max(event_start_nses) if event_start_nses else base_ns
    root_end = iso_from_ns(latest_event_start_ns + 1_000_000)
    call_outputs = session_call_outputs(session_events)
    function_call_ids = {
        (event.get("payload") or {}).get("call_id")
        for event in session_events
        if event.get("type") == "response_item"
        and isinstance(event.get("payload"), dict)
        and event["payload"].get("type") == "function_call"
        and event["payload"].get("call_id")
    }

    batch = [
        ingestion_event(
            "trace-create",
            first_timestamp,
            {
                "id": trace_id,
                "timestamp": first_timestamp,
                "name": args.subagent_trace_name,
                "input": trace_input,
                "output": trace_output,
                "sessionId": trace_session_id,
                "environment": args.environment,
                "metadata": trace_metadata,
                "tags": ["doris-ai-review-subagent", "codex-session-jsonl"],
            },
        ),
        ingestion_event(
            "span-create",
            first_timestamp,
            {
                "id": root_observation_id,
                "traceId": trace_id,
                "name": "codex.subagent.review",
                "startTime": first_timestamp,
                "endTime": root_end,
                "input": {"session_file": session_path, "thread_id": thread_id},
                "output": {"final_message": trace_output},
                "environment": args.environment,
                "metadata": trace_metadata,
            },
        ),
    ]

    observation_count = 1
    for offset, event in enumerate(session_events, start=2):
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if (
            event.get("type") == "response_item"
            and payload.get("type") == "function_call_output"
            and payload.get("call_id") in function_call_ids
        ):
            continue
        shape = session_observation_shape(event, call_outputs, args.max_json_chars)
        fallback_start_ns = base_ns + offset * 1_000_000
        start_time = session_event_timestamp(event, fallback_start_ns)
        end_ns = (ns_from_iso(start_time) or fallback_start_ns) + 750_000
        body = {
            "id": secrets.token_hex(16),
            "traceId": trace_id,
            "parentObservationId": root_observation_id,
            "name": shape["name"],
            "startTime": start_time,
            "endTime": iso_from_ns(end_ns),
            "input": shape["input"],
            "output": shape["output"],
            "environment": args.environment,
            "metadata": {
                **trace_metadata,
                "session_event_type": event.get("type"),
                "session_line": event.get("_line_number"),
            },
        }
        for key, value in shape.get("metadata", {}).items():
            if value not in (None, ""):
                body["metadata"][key] = value
        event_type = "generation-create" if shape["type"] == "generation" else "span-create"
        if shape["type"] == "generation":
            body["model"] = args.model
        batch.append(ingestion_event(event_type, start_time, body))
        observation_count += 1

    return {
        "trace_id": trace_id,
        "session_id": trace_session_id,
        "thread_id": thread_id,
        "path": session_path,
        "event_count": len(session_events),
        "observation_count": observation_count,
        "payload": {"batch": batch},
    }


def build_subagent_session_payloads(args, main_events):
    expected_thread_ids = receiver_thread_ids(main_events)
    payloads = []
    for path in session_jsonl_files(args.subagent_sessions_dir):
        events = load_jsonl(path)
        meta = session_meta(events)
        if not is_subagent_meta(meta, expected_thread_ids):
            continue
        payloads.append(build_subagent_session_payload(args, path, events))
        if len(payloads) >= args.max_subagent_sessions:
            break
    return payloads


def compact_json_bytes(payload):
    return json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")


def compact_context_event(event, max_chars):
    compact = {}
    for key in (
        "event_type",
        "line_number",
        "item_id",
        "item_type",
        "status",
        "server",
        "tool",
        "exit_code",
    ):
        if event.get(key) not in (None, ""):
            compact[key] = event.get(key)
    if event.get("command"):
        compact["command"] = truncate_text(event.get("command") or "", max_chars)
    for key in ("arguments", "result", "error", "payload", "item"):
        if key in event:
            compact[key] = truncate_json(event.get(key), max_chars)
    for key in ("aggregated_output", "text", "prompt"):
        if event.get(key):
            compact[key] = truncate_text(event.get(key) or "", max_chars, tail=True)
    return compact


def shrink_event_for_payload(event, max_payload_bytes):
    shrunk = json.loads(json.dumps(event, ensure_ascii=False))
    body = shrunk.get("body") if isinstance(shrunk.get("body"), dict) else {}
    event_name = body.get("name")

    def candidate_with_limits(max_chars, max_context_events=None):
        candidate = json.loads(json.dumps(shrunk, ensure_ascii=False))
        candidate_body = candidate.get("body") if isinstance(candidate.get("body"), dict) else {}
        input_object = candidate_body.get("input")
        if isinstance(input_object, dict):
            context_events = input_object.get("events_since_previous_agent_message")
            if isinstance(context_events, list):
                compact_events = [
                    compact_context_event(context_event, max_chars)
                    for context_event in context_events
                    if isinstance(context_event, dict)
                ]
                if max_context_events is not None:
                    omitted_count = max(len(compact_events) - max_context_events, 0)
                    if max_context_events <= 0:
                        compact_events = []
                    else:
                        compact_events = compact_events[-max_context_events:]
                    if omitted_count:
                        input_object[
                            "events_since_previous_agent_message_omitted_count"
                        ] = omitted_count
                input_object["events_since_previous_agent_message"] = compact_events
            if isinstance(input_object.get("previous_agent_message"), dict):
                previous_message = input_object["previous_agent_message"]
                previous_message["text"] = truncate_text(
                    previous_message.get("text") or "", max_chars
                )
            if input_object.get("turn_input"):
                input_object["turn_input"] = truncate_text(
                    input_object.get("turn_input") or "", max_chars
                )
            if not (
                isinstance(context_events, list)
                or "previous_agent_message" in input_object
                or "turn_input" in input_object
            ):
                candidate_body["input"] = truncate_json(input_object, max_chars)
        elif input_object not in (None, ""):
            candidate_body["input"] = truncate_json(input_object, max_chars)

        output_object = candidate_body.get("output")
        if output_object not in (None, ""):
            candidate_body["output"] = truncate_json(output_object, max_chars)
        metadata = candidate_body.get("metadata")
        if metadata not in (None, ""):
            candidate_body["metadata"] = truncate_json(metadata, max_chars)
        return candidate

    for max_chars in (2_000, 1_000, 500, 200, 80):
        candidate = candidate_with_limits(max_chars)

        if json_payload_bytes({"batch": [candidate]}) <= max_payload_bytes:
            return candidate

    for max_context_events in (50, 20, 10, 5, 2, 1, 0):
        for max_chars in (80, 40, 20, 10):
            candidate = candidate_with_limits(max_chars, max_context_events)
            if json_payload_bytes({"batch": [candidate]}) <= max_payload_bytes:
                return candidate

    raise RuntimeError(
        "Litefuse ingestion event is too large after truncation: "
        f"{json_payload_bytes({'batch': [event]})} bytes > {max_payload_bytes} bytes; "
        f"type={event.get('type')}, name={event_name}"
    )


def chunk_payload(payload, max_payload_bytes):
    batch = payload.get("batch") or []
    chunks = []
    current = []
    current_bytes = json_payload_bytes({"batch": current})

    for event in batch:
        event_payload = {"batch": [event]}
        event_bytes = json_payload_bytes(event_payload)
        if event_bytes > max_payload_bytes:
            event = shrink_event_for_payload(event, max_payload_bytes)
            event_payload = {"batch": [event]}
            event_bytes = json_payload_bytes(event_payload)

        candidate = {"batch": current + [event]}
        candidate_bytes = json_payload_bytes(candidate)
        if current and candidate_bytes > max_payload_bytes:
            chunks.append(({"batch": current}, current_bytes))
            current = [event]
            current_bytes = event_bytes
        else:
            current.append(event)
            current_bytes = candidate_bytes

    if current:
        chunks.append(({"batch": current}, current_bytes))
    return chunks


def post_payload_once(endpoint, public_key, secret_key, payload):
    auth = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode()
    request = urllib.request.Request(
        endpoint,
        data=compact_json_bytes(payload),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Basic {auth}",
        },
        method="POST",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        body = response.read().decode()
        detail = json.loads(body) if body else {}
        errors = detail.get("errors") if isinstance(detail, dict) else None
        if errors:
            raise RuntimeError(f"Litefuse ingestion returned errors: {json_attr(errors)}")
        return {
            "status": response.status,
            "success_count": len(detail.get("successes") or [])
            if isinstance(detail, dict)
            else 0,
        }


def retry_payload_chunks_after_413(payload, request_size, max_payload_bytes):
    batch = payload.get("batch") or []
    if not batch:
        raise RuntimeError(
            "Litefuse ingestion returned 413 for an empty payload chunk"
        )

    next_limit = max(1_000, min(max_payload_bytes - 1, request_size // 2))
    if len(batch) == 1:
        event = shrink_event_for_payload(batch[0], next_limit)
        return [({"batch": [event]}, json_payload_bytes({"batch": [event]}))]

    return chunk_payload(payload, next_limit)


def post_payload(endpoint, public_key, secret_key, payload, max_payload_bytes):
    statuses = []
    success_count = 0
    request_sizes = []
    retry_count = 0
    chunks = chunk_payload(payload, max_payload_bytes)
    while chunks:
        chunk, request_size = chunks.pop(0)
        try:
            status = post_payload_once(endpoint, public_key, secret_key, chunk)
        except urllib.error.HTTPError as exc:
            if exc.code != 413:
                raise
            retry_count += 1
            chunks = (
                retry_payload_chunks_after_413(
                    chunk, request_size, max_payload_bytes
                )
                + chunks
            )
            continue
        statuses.append(status["status"])
        success_count += int(status.get("success_count") or 0)
        request_sizes.append(request_size)
    return {
        "statuses": statuses,
        "request_count": len(statuses),
        "request_sizes": request_sizes,
        "max_request_size": max(request_sizes) if request_sizes else 0,
        "payload_too_large_retries": retry_count,
        "success_count": success_count,
    }


def fetch_trace(base_url, public_key, secret_key, trace_id):
    auth = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode()
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/public/traces/{trace_id}",
        headers={"Authorization": f"Basic {auth}"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())


def fetch_observations_v2(base_url, public_key, secret_key, trace_id):
    auth = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode()
    now = datetime.now(timezone.utc)
    params = urllib.parse.urlencode(
        {
            "traceId": trace_id,
            "fromStartTime": (now - timedelta(hours=1)).isoformat().replace("+00:00", "Z"),
            "toStartTime": (now + timedelta(minutes=5)).isoformat().replace("+00:00", "Z"),
            "fields": "core,basic,io,trace_context,model,usage",
            "limit": "100",
        }
    )
    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/api/public/v2/observations?{params}",
        headers={"Authorization": f"Basic {auth}"},
        method="GET",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.loads(response.read().decode())


def fetch_observations_legacy(
    base_url, public_key, secret_key, trace_id, max_pages=10
):
    auth = base64.b64encode(f"{public_key}:{secret_key}".encode()).decode()
    limit = 100
    rows = []
    last_payload = {}
    for page in range(1, max_pages + 1):
        params = urllib.parse.urlencode(
            {"traceId": trace_id, "limit": str(limit), "page": str(page)}
        )
        request = urllib.request.Request(
            f"{base_url.rstrip('/')}/api/public/observations?{params}",
            headers={"Authorization": f"Basic {auth}"},
            method="GET",
        )
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode())
        last_payload = payload if isinstance(payload, dict) else {}
        page_rows = observation_rows_from_v2(last_payload)
        rows.extend(page_rows)
        if len(page_rows) < limit:
            break
    return {**last_payload, "data": rows}


def observation_rows_from_v2(payload):
    rows = payload.get("data") if isinstance(payload, dict) else None
    if isinstance(rows, list):
        return rows
    return []


def observation_io_object(observation, field):
    value = observation.get(field)
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return {}
        if isinstance(parsed, dict):
            return parsed
    return {}


def context_event_types(events_value):
    if isinstance(events_value, list):
        return [
            event.get("item_type") or event.get("event_type")
            for event in events_value
            if isinstance(event, dict)
        ]
    if isinstance(events_value, dict) and "truncated_json" in events_value:
        return ["truncated_json"]
    return []


def context_events_readback_ok(input_object):
    context_window = input_object.get("context_window")
    event_count = 0
    if isinstance(context_window, dict):
        try:
            event_count = int(context_window.get("event_count") or 0)
        except (TypeError, ValueError):
            event_count = 0
    events_value = input_object.get("events_since_previous_agent_message")
    if isinstance(events_value, list):
        return True
    return event_count == 0 and events_value in (None, "", {})


def verify_trace(args, public_key, secret_key, trace_id):
    last_diagnostic = {}
    for _ in range(args.verify_attempts):
        legacy_trace_error = ""
        try:
            legacy_detail = fetch_trace(args.base_url, public_key, secret_key, trace_id)
        except Exception as exc:
            legacy_detail = {}
            legacy_trace_error = type(exc).__name__
        try:
            observations_payload = fetch_observations_legacy(
                args.base_url, public_key, secret_key, trace_id
            )
            observations = observation_rows_from_v2(observations_payload)
            read_source = "legacy_observations"
        except Exception as exc:
            try:
                observations_payload = fetch_observations_v2(
                    args.base_url, public_key, secret_key, trace_id
                )
                observations = observation_rows_from_v2(observations_payload)
                read_source = "v2_observations"
            except Exception:
                observations = legacy_detail.get("observations") or []
                read_source = f"legacy_trace_fallback:{type(exc).__name__}"
        observations_missing_io = [
            observation
            for observation in observations
            if not (observation.get("input") and observation.get("output"))
        ]
        step_observations = [
            observation
            for observation in observations
            if observation.get("name") not in ("codex.review", "codex.turn")
        ]
        agent_message_observations = [
            observation
            for observation in observations
            if observation.get("name") == "codex.agent_message"
        ]
        agent_message_input_objects = [
            observation_io_object(observation, "input")
            for observation in agent_message_observations
        ]
        agent_message_input_keys = sorted(
            {
                key
                for input_object in agent_message_input_objects
                for key in input_object.keys()
            }
        )
        agent_message_all_have_context_window = all(
            bool(input_object.get("context_window"))
            for input_object in agent_message_input_objects
        )
        agent_message_all_have_context_events = all(
            context_events_readback_ok(input_object)
            for input_object in agent_message_input_objects
        )
        agent_message_with_previous_count = sum(
            1
            for input_object in agent_message_input_objects
            if bool(input_object.get("previous_agent_message"))
        )
        agent_message_with_turn_input_count = sum(
            1
            for input_object in agent_message_input_objects
            if bool(input_object.get("turn_input"))
        )
        agent_message_context_event_counts = [
            (input_object.get("context_window") or {}).get("event_count", 0)
            for input_object in agent_message_input_objects[:20]
            if isinstance(input_object.get("context_window"), dict)
        ]
        agent_message_event_type_samples = [
            context_event_types(
                input_object.get("events_since_previous_agent_message")
            )[:8]
            for input_object in agent_message_input_objects[:5]
        ]
        agent_message_structure_ok = (
            agent_message_all_have_context_window
            and agent_message_all_have_context_events
            and agent_message_with_turn_input_count == 1
            and (
                agent_message_with_previous_count
                == max(len(agent_message_observations) - 1, 0)
            )
        )
        root_observation = next(
            (observation for observation in observations if observation.get("name") == "codex.review"),
            {},
        )
        trace_input = legacy_detail.get("input") or root_observation.get("input")
        trace_output = legacy_detail.get("output") or root_observation.get("output")
        last_diagnostic = {
            "read_source": read_source,
            "legacy_trace_input": bool(legacy_detail.get("input")),
            "legacy_trace_output": bool(legacy_detail.get("output")),
            "legacy_trace_error": legacy_trace_error,
            "trace_input": bool(trace_input),
            "trace_output": bool(trace_output),
            "observation_count": len(observations),
            "step_observation_count": len(step_observations),
            "agent_message_count": len(agent_message_observations),
            "agent_message_input_keys": agent_message_input_keys,
            "agent_message_all_have_context_window": agent_message_all_have_context_window,
            "agent_message_all_have_context_events": agent_message_all_have_context_events,
            "agent_message_with_previous_count": agent_message_with_previous_count,
            "agent_message_with_turn_input_count": agent_message_with_turn_input_count,
            "agent_message_context_event_counts": agent_message_context_event_counts,
            "agent_message_event_type_samples": agent_message_event_type_samples,
            "observations_missing_io": [
                observation.get("name") for observation in observations_missing_io[:20]
            ],
            "observation_names": [observation.get("name") for observation in observations[:20]],
        }
        ok = all(
            [
                trace_input,
                trace_output,
                len(observations) >= args.min_observations,
                len(step_observations) >= args.min_step_observations,
                not observations_missing_io,
                not agent_message_observations or agent_message_structure_ok,
            ]
        )
        if ok:
            return {
                "trace_input": True,
                "trace_output": True,
                "read_source": read_source,
                "observation_count": len(observations),
                "step_observation_count": len(step_observations),
                "agent_message_count": len(agent_message_observations),
                "agent_message_input_keys": agent_message_input_keys,
                "agent_message_all_have_context_window": agent_message_all_have_context_window,
                "agent_message_all_have_context_events": agent_message_all_have_context_events,
                "agent_message_with_previous_count": agent_message_with_previous_count,
                "agent_message_with_turn_input_count": agent_message_with_turn_input_count,
                "agent_message_context_event_counts": agent_message_context_event_counts,
                "agent_message_event_type_samples": agent_message_event_type_samples,
                "observations_missing_io": [],
                "observation_names": [
                    observation.get("name") for observation in observations[:20]
                ],
            }
        time.sleep(args.verify_sleep_seconds)
    raise RuntimeError(
        "Litefuse trace "
        f"{trace_id} did not expose multi-step I/O in time; "
        f"last_diagnostic={json.dumps(last_diagnostic, sort_keys=True)}"
    )


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="https://litefuse.cloud")
    parser.add_argument("--endpoint", default=None)
    parser.add_argument("--input-file", required=True)
    parser.add_argument("--events-file", required=True)
    parser.add_argument("--output-file", default="")
    parser.add_argument("--trace-name", default="doris-ai-review")
    parser.add_argument("--subagent-trace-name", default="doris-ai-review-subagent")
    parser.add_argument("--subagent-sessions-dir", default="")
    parser.add_argument("--session-id", required=True)
    parser.add_argument("--repository", default="")
    parser.add_argument("--workflow", default="")
    parser.add_argument("--run-id", default="")
    parser.add_argument("--pr-number", default="")
    parser.add_argument("--head-sha", default="")
    parser.add_argument("--base-sha", default="")
    parser.add_argument("--model", default="gpt-5.5")
    parser.add_argument("--reasoning-effort", default="")
    parser.add_argument("--environment", default="github-actions")
    parser.add_argument("--max-input-chars", type=int, default=200_000)
    parser.add_argument("--max-output-chars", type=int, default=200_000)
    parser.add_argument("--max-json-chars", type=int, default=40_000)
    parser.add_argument("--max-context-json-chars", type=int, default=0)
    parser.add_argument("--max-payload-bytes", type=int, default=4_000_000)
    parser.add_argument("--max-subagent-sessions", type=int, default=100)
    parser.add_argument("--verify", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verify-attempts", type=int, default=24)
    parser.add_argument("--verify-sleep-seconds", type=int, default=5)
    parser.add_argument("--min-observations", type=int, default=3)
    parser.add_argument("--min-step-observations", type=int, default=1)
    return parser.parse_args()


def main():
    args = parse_args()
    endpoint = args.endpoint or f"{args.base_url.rstrip('/')}/api/public/ingestion"
    if args.max_context_json_chars <= 0:
        args.max_context_json_chars = args.max_json_chars

    input_text = read_text(args.input_file, args.max_input_chars)
    output_text = (
        read_text(args.output_file, args.max_output_chars, tail=True, optional=True)
        if args.output_file
        else ""
    )
    events = load_jsonl(args.events_file)
    trace_id, payload, observation_count = build_ingestion_payload(
        args, input_text, output_text, events
    )
    subagent_payloads = (
        build_subagent_session_payloads(args, events)
        if args.subagent_sessions_dir
        else []
    )

    result = {
        "dry_run": args.dry_run,
        "event_count": len(events),
        "observation_count": observation_count,
        "trace_id": trace_id,
        "session_id": args.session_id,
        "trace_name": args.trace_name,
        "subagent_trace_count": len(subagent_payloads),
        "model": args.model,
        "reasoning_effort": args.reasoning_effort,
    }
    if args.dry_run:
        result["batch_count"] = len(payload["batch"])
        result["event_types"] = [event["type"] for event in payload["batch"][:10]]
        chunks = chunk_payload(payload, args.max_payload_bytes)
        result["request_count"] = len(chunks)
        result["request_sizes"] = [request_size for _, request_size in chunks]
        result["max_request_size"] = (
            max(result["request_sizes"]) if result["request_sizes"] else 0
        )
        result["subagent_traces"] = []
        for subagent_payload in subagent_payloads:
            chunks = chunk_payload(subagent_payload["payload"], args.max_payload_bytes)
            request_sizes = [request_size for _chunk, request_size in chunks]
            result["subagent_traces"].append(
                {
                    "trace_id": subagent_payload["trace_id"],
                    "session_id": subagent_payload["session_id"],
                    "thread_id": subagent_payload["thread_id"],
                    "path": subagent_payload["path"],
                    "event_count": subagent_payload["event_count"],
                    "observation_count": subagent_payload["observation_count"],
                    "batch_count": len(subagent_payload["payload"]["batch"]),
                    "request_count": len(chunks),
                    "request_sizes": request_sizes,
                    "max_request_size": max(request_sizes) if request_sizes else 0,
                }
            )
        print(json.dumps(result, sort_keys=True))
        return

    public_key = os.environ["LANGFUSE_PUBLIC_KEY"]
    secret_key = os.environ["LANGFUSE_SECRET_KEY"]
    result["status"] = post_payload(
        endpoint, public_key, secret_key, payload, args.max_payload_bytes
    )
    result["subagent_traces"] = []
    for subagent_payload in subagent_payloads:
        status = post_payload(
            endpoint,
            public_key,
            secret_key,
            subagent_payload["payload"],
            args.max_payload_bytes,
        )
        result["subagent_traces"].append(
            {
                "trace_id": subagent_payload["trace_id"],
                "session_id": subagent_payload["session_id"],
                "thread_id": subagent_payload["thread_id"],
                "path": subagent_payload["path"],
                "event_count": subagent_payload["event_count"],
                "observation_count": subagent_payload["observation_count"],
                "status": status,
            }
        )

    if args.verify:
        try:
            result["verified"] = verify_trace(args, public_key, secret_key, trace_id)
        except Exception as exc:
            result["verification_error"] = str(exc)
            print(json.dumps(result, sort_keys=True))
            raise
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
