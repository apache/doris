#!/usr/bin/env python3
"""Render a Doris MergedProfile as a Graphviz DAG with highlighted bottlenecks."""

from __future__ import annotations

import argparse
import re
import shutil
import subprocess
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from summarize_profile import (
    clean_pipeline_label,
    extract_section_bounds,
    metric_rank,
    normalize_spaces,
    parse_instance_num,
    parse_key_value_section,
    parse_merged_profile,
)


ROW_COUNTERS = {"InputRows", "RowsProduced", "ScanRows", "ProbeRows"}


@dataclass
class DagNode:
    node_id: str
    fragment: str
    pipeline: str
    instance_num: Optional[int]
    operator: str
    line_no: int

    @property
    def cluster_id(self) -> str:
        return self.fragment

    @property
    def key(self) -> Tuple[str, str, str]:
        return (self.fragment, self.pipeline, self.operator)


def is_operator_header(stripped: str) -> bool:
    if not stripped.endswith(":") or stripped.startswith("-"):
        return False
    if stripped.endswith("Scanner:"):
        return False
    if stripped in {
        "MergedProfile:",
        "MergedProfile",
        "Fragments:",
        "CommonCounters:",
        "CustomCounters:",
        "Scanner:",
        "FragmentLevelProfile:",
    }:
        return False
    if stripped.startswith("Fragment "):
        return False
    if stripped.startswith("Pipeline "):
        return False
    if stripped.startswith("Execution Profile "):
        return False
    if stripped.startswith("PipelineXTask"):
        return False
    return True


def parse_merged_dag_nodes(lines: List[str]) -> Tuple[List[DagNode], List[Tuple[str, str]]]:
    start = extract_section_bounds(lines, r"^MergedProfile:?$")
    if start is None:
        return [], []

    end = extract_section_bounds(lines[start + 1 :], r"^(DetailProfile\(|Execution Profile\b)")
    if end is None:
        end = len(lines)
    else:
        end = start + 1 + end

    nodes: List[DagNode] = []
    edges: List[Tuple[str, str]] = []
    pipeline_nodes: Dict[Tuple[str, str], List[str]] = defaultdict(list)

    fragment = ""
    pipeline = ""
    instance_num: Optional[int] = None

    for index in range(start + 1, end):
        stripped = normalize_spaces(lines[index])
        if not stripped:
            continue

        if re.match(r"^Fragment \d+:$", stripped):
            fragment = stripped[:-1]
            pipeline = ""
            instance_num = None
            continue

        if re.match(r"^Pipeline(?:\s*:)?\s*\d+(?:\(instance_num=\d+\))?:$", stripped):
            pipeline = clean_pipeline_label(stripped[:-1])
            instance_num = parse_instance_num(stripped)
            continue

        if not fragment or not pipeline or not is_operator_header(stripped):
            continue

        operator = stripped[:-1]
        node_id = f"n{len(nodes)}"
        node = DagNode(
            node_id=node_id,
            fragment=fragment,
            pipeline=pipeline,
            instance_num=instance_num,
            operator=operator,
            line_no=index + 1,
        )
        nodes.append(node)

        key = (fragment, pipeline)
        pipeline_nodes[key].append(node_id)
        current_pipeline_nodes = pipeline_nodes[key]
        if len(current_pipeline_nodes) >= 2:
            upstream = current_pipeline_nodes[-1]
            downstream = current_pipeline_nodes[-2]
            edges.append((upstream, downstream))

    return nodes, edges


def operator_id(operator: str) -> Optional[str]:
    match = re.search(r"\(id=([-]?\d+)\)", operator)
    if match:
        return match.group(1)
    match = re.search(r"\bid=([-]?\d+)\b", operator)
    if match:
        return match.group(1)
    return None


def destination_id(operator: str) -> Optional[str]:
    for pattern in (r"\bdst_id=([-]?\d+)\b", r"\bdest_id=([-]?\d+)\b"):
        match = re.search(pattern, operator)
        if match:
            return match.group(1)
    return None


def add_exchange_edges(nodes: List[DagNode], edges: List[Tuple[str, str]]) -> List[Tuple[str, str, str]]:
    exchange_lookup: Dict[Tuple[str, str], str] = {}
    extra_edges: List[Tuple[str, str, str]] = []

    for node in nodes:
        op_id = operator_id(node.operator)
        if not op_id:
            continue
        if "EXCHANGE_OPERATOR" in node.operator and "LOCAL_EXCHANGE_SINK_OPERATOR" not in node.operator:
            exchange_lookup[("exchange", op_id)] = node.node_id
        if "LOCAL_EXCHANGE_OPERATOR" in node.operator:
            exchange_lookup[("local", op_id)] = node.node_id

    seen = set(edges)
    for node in nodes:
        dst_id = destination_id(node.operator)
        if "DATA_STREAM_SINK_OPERATOR" in node.operator and dst_id:
            target = exchange_lookup.get(("exchange", dst_id))
            if target and (node.node_id, target) not in seen:
                extra_edges.append((node.node_id, target, "exchange"))
                seen.add((node.node_id, target))
        if "LOCAL_EXCHANGE_SINK_OPERATOR" in node.operator:
            op_id = operator_id(node.operator)
            target = exchange_lookup.get(("local", op_id)) if op_id else None
            if target and (node.node_id, target) not in seen:
                extra_edges.append((node.node_id, target, "local"))
                seen.add((node.node_id, target))

    return extra_edges


def group_metrics(metrics) -> Dict[Tuple[str, str, str], List]:
    grouped: Dict[Tuple[str, str, str], List] = defaultdict(list)
    for metric in metrics:
        grouped[(metric.fragment, metric.pipeline, metric.operator)].append(metric)
    return grouped


def pick_metric(metric_list, predicate, mode: str):
    candidates = [metric for metric in metric_list if predicate(metric)]
    if not candidates:
        return None
    return max(candidates, key=lambda metric: metric_rank(metric, mode))


def node_visual_data(node: DagNode, grouped_metrics: Dict[Tuple[str, str, str], List]) -> Dict[str, Optional[str]]:
    metrics = grouped_metrics.get(node.key, [])
    exec_metric = pick_metric(metrics, lambda metric: metric.counter_name == "ExecTime", "exec")
    wait_metric = pick_metric(
        metrics,
        lambda metric: metric.kind == "time"
        and (
            "WaitForDependency" in metric.counter_name
            or metric.counter_name.startswith("WaitFor")
        ),
        "wait",
    )
    mem_metric = pick_metric(
        metrics,
        lambda metric: metric.kind == "bytes"
        and ("Memory" in metric.counter_name or "Arena" in metric.counter_name or "HashTable" in metric.counter_name),
        "memory",
    )
    row_metric = pick_metric(metrics, lambda metric: metric.counter_name in ROW_COUNTERS, "rows")

    exec_score = metric_rank(exec_metric, "exec") if exec_metric else 0.0
    wait_score = metric_rank(wait_metric, "wait") if wait_metric else 0.0
    memory_score = metric_rank(mem_metric, "memory") if mem_metric else 0.0
    row_score = metric_rank(row_metric, "rows") if row_metric else 0.0

    return {
        "exec": summarize_metric(exec_metric, "avg") if exec_metric and exec_score > 0 else None,
        "wait": summarize_metric(wait_metric, "avg") if wait_metric and wait_score > 0 else None,
        "memory": summarize_metric(mem_metric, "max") if mem_metric and memory_score > 0 else None,
        "rows": summarize_metric(row_metric, "sum") if row_metric and row_score > 0 else None,
        "exec_score": exec_score,
        "wait_score": wait_score,
        "memory_score": memory_score,
    }


def escape_dot(text: str) -> str:
    return text.replace('"', '\\"')


def fragment_sort_key(fragment_name: str) -> Tuple[int, str]:
    match = re.search(r"Fragment\s+(\d+)", fragment_name)
    if match:
        return (int(match.group(1)), fragment_name)
    return (10**9, fragment_name)


def compact_operator(operator: str) -> str:
    operator = normalize_spaces(operator)
    name_match = re.match(r"^([A-Z_]+)", operator)
    name = name_match.group(1) if name_match else operator
    name = name.replace("_OPERATOR", "")

    op_id = operator_id(operator)
    header = f"{name} #{op_id}" if op_id is not None else name

    table_match = re.search(r"table(?:_name| name)\s*=\s*([^)]+)", operator)
    if table_match:
        table_name = table_match.group(1).strip()
        table_name = table_name.split(".")[-1]
        if len(table_name) > 28:
            table_name = table_name[:25] + "..."
        return f"{header}\\n{table_name}"
    return header


def short_pipeline(pipeline: str, instance_num: Optional[int]) -> str:
    match = re.search(r"Pipeline\s+(\d+)", pipeline)
    pipeline_id = match.group(1) if match else pipeline
    if instance_num is not None:
        return f"P{pipeline_id} x{instance_num}"
    return f"P{pipeline_id}"


def format_time_value(seconds: float) -> str:
    if seconds >= 60:
        minutes = int(seconds // 60)
        remaining = seconds - minutes * 60
        if remaining >= 10 or remaining.is_integer():
            return f"{minutes}m{int(round(remaining))}s"
        return f"{minutes}m{remaining:.1f}s"
    if seconds >= 1:
        return f"{seconds:.1f}s" if seconds < 10 else f"{seconds:.0f}s"
    if seconds >= 1e-3:
        milliseconds = seconds * 1000
        return f"{milliseconds:.1f}ms" if milliseconds < 10 else f"{milliseconds:.0f}ms"
    if seconds >= 1e-6:
        microseconds = seconds * 1_000_000
        return f"{microseconds:.1f}us" if microseconds < 10 else f"{microseconds:.0f}us"
    return f"{seconds * 1_000_000_000:.0f}ns"


def format_bytes_value(amount: float) -> str:
    units = ["B", "KB", "MB", "GB", "TB"]
    value = float(amount)
    for unit in units:
        if value < 1024 or unit == units[-1]:
            if unit == "B":
                return f"{int(round(value))}{unit}"
            if value < 10:
                return f"{value:.1f}{unit}"
            return f"{value:.0f}{unit}"
        value /= 1024
    return f"{value:.0f}TB"


def format_count_value(amount: float) -> str:
    abs_amount = abs(amount)
    for suffix, divisor in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs_amount >= divisor:
            value = amount / divisor
            if abs(value) < 10:
                return f"{value:.1f}{suffix}"
            return f"{value:.0f}{suffix}"
    return str(int(round(amount)))


def summarize_metric(metric, preferred_stat: str) -> str:
    if metric is None:
        return ""
    chosen = metric.stats.get(preferred_stat)
    stat_name = preferred_stat
    if chosen is None:
        for fallback in ("max", "avg", "sum", "value", "min"):
            chosen = metric.stats.get(fallback)
            if chosen is not None:
                stat_name = fallback
                break
    if chosen is None:
        return metric.counter_name

    if metric.kind == "time":
        value = format_time_value(chosen)
    elif metric.kind == "bytes":
        value = format_bytes_value(chosen)
    else:
        value = format_count_value(chosen)

    counter_name = metric.counter_name
    if counter_name == "PeakMemoryUsage":
        counter_name = "Mem"
    elif counter_name == "ExecTime":
        counter_name = "Exec"
    elif counter_name.startswith("WaitFor"):
        counter_name = "Wait"
    elif counter_name in ROW_COUNTERS:
        counter_name = "Rows" if counter_name != "ScanRows" else "Scan"
    elif len(counter_name) > 14:
        counter_name = counter_name[:14] + "..."

    return f"{counter_name} {stat_name} {value}"


def top_node_ids(node_data: Dict[str, Dict[str, Optional[str]]], field: str, limit: int) -> set:
    ranked = [
        (data.get(field, 0.0), node_id)
        for node_id, data in node_data.items()
        if data.get(field, 0.0) and data.get(field, 0.0) > 0
    ]
    ranked.sort(reverse=True)
    return {node_id for _, node_id in ranked[:limit]}


def build_dot(
    profile_path: Path,
    summary: Dict[str, str],
    nodes: List[DagNode],
    edges: List[Tuple[str, str]],
    extra_edges: List[Tuple[str, str, str]],
    grouped_metrics: Dict[Tuple[str, str, str], List],
    highlight_top: int,
) -> str:
    node_data = {node.node_id: node_visual_data(node, grouped_metrics) for node in nodes}
    top_wait = top_node_ids(node_data, "wait_score", highlight_top)
    top_exec = top_node_ids(node_data, "exec_score", highlight_top)
    top_memory = top_node_ids(node_data, "memory_score", highlight_top)

    title = summary.get("Profile ID", profile_path.name)
    total = summary.get("Total", "unknown")
    lines = [
        "digraph doris_profile {",
        '  graph [rankdir=TB, newrank=true, splines=ortho, overlap=false, pad=0.3, nodesep=0.28, ranksep=0.7, pack=false, fontname="Helvetica", bgcolor="white", labeljust="l", labelloc="t"];',
        '  node [shape=box, style="rounded,filled", margin="0.10,0.08", fontname="Helvetica", fontsize=10, color="#475569", fillcolor="#eef2f7"];',
        '  edge [color="#94a3b8", arrowsize=0.7, penwidth=1.1];',
        f'  label="{escape_dot(f"Doris Query Profile DAG\\n{title} | Total: {total}\\nRed=wait bottleneck, Orange=compute bottleneck, Purple=memory hotspot")}" ;',
    ]

    clusters: Dict[str, List[DagNode]] = defaultdict(list)
    for node in nodes:
        clusters[node.cluster_id].append(node)

    cluster_first_nodes: List[str] = []
    sorted_cluster_names = sorted(clusters, key=fragment_sort_key)
    for cluster_index, cluster_name in enumerate(sorted_cluster_names):
        cluster_nodes = clusters[cluster_name]
        lines.append(f"  subgraph cluster_{cluster_index} {{")
        lines.append(f'    label="{escape_dot(cluster_name)}";')
        lines.append('    color="#cbd5e1";')
        lines.append('    style="rounded";')
        if cluster_nodes:
            cluster_first_nodes.append(cluster_nodes[0].node_id)
        for node in cluster_nodes:
            data = node_data[node.node_id]
            fill = "#eef2f7"
            border = "#475569"
            penwidth = "1.2"
            if node.node_id in top_memory:
                fill = "#f5d8ff"
                border = "#9333ea"
                penwidth = "2.0"
            if node.node_id in top_exec:
                fill = "#ffe7c2"
                border = "#c2410c"
                penwidth = "2.2"
            if node.node_id in top_wait:
                fill = "#ffd7d7"
                border = "#b91c1c"
                penwidth = "2.6"

            label_lines = [
                compact_operator(node.operator),
                short_pipeline(node.pipeline, node.instance_num),
            ]
            if data["exec"]:
                label_lines.append(data["exec"])
            if data["wait"]:
                label_lines.append(data["wait"])
            if data["memory"]:
                label_lines.append(data["memory"])
            if data["rows"]:
                label_lines.append(data["rows"])

            label = "\\n".join(label_lines)
            lines.append(
                f'    {node.node_id} [label="{escape_dot(label)}", fillcolor="{fill}", color="{border}", penwidth={penwidth}];'
            )
        lines.append("  }")

    for src, dst in edges:
        lines.append(f"  {src} -> {dst};")

    for src, dst, edge_type in extra_edges:
        color = "#2563eb" if edge_type == "exchange" else "#7c3aed"
        lines.append(
            f'  {src} -> {dst} [style=dashed, color="{color}", penwidth=1.0, constraint=false];'
        )

    for upper, lower in zip(cluster_first_nodes, cluster_first_nodes[1:]):
        lines.append(f"  {upper} -> {lower} [style=invis, weight=50];")

    lines.append("}")
    return "\n".join(lines) + "\n"


def write_output(dot_text: str, output_path: Path, output_format: str) -> None:
    if output_format == "dot":
        output_path.write_text(dot_text)
        return

    dot_binary = shutil.which("dot")
    if not dot_binary:
        raise SystemExit("Graphviz 'dot' is not installed; use --format dot or install graphviz.")

    temp_dot = output_path.with_suffix(".dot")
    temp_dot.write_text(dot_text)
    subprocess.run(
        [dot_binary, f"-T{output_format}", str(temp_dot), "-o", str(output_path)],
        check=True,
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", help="Path to a Doris query profile")
    parser.add_argument("--output", "-o", help="Output file path")
    parser.add_argument(
        "--format",
        choices=("dot", "svg", "png"),
        default="dot",
        help="Output format. 'svg' and 'png' require Graphviz.",
    )
    parser.add_argument(
        "--highlight-top",
        type=int,
        default=3,
        help="How many top wait/exec/memory nodes to highlight",
    )
    args = parser.parse_args()

    profile_path = Path(args.profile)
    lines = profile_path.read_text(errors="replace").splitlines()

    summary = parse_key_value_section(
        lines,
        "Summary:",
        ("Execution Summary:", "ChangedSessionVariables:", "MergedProfile:"),
    )
    metrics = parse_merged_profile(lines)
    nodes, edges = parse_merged_dag_nodes(lines)
    if not nodes:
        raise SystemExit("No MergedProfile DAG was found in this profile.")

    extra_edges = add_exchange_edges(nodes, edges)
    grouped = group_metrics(metrics)
    dot_text = build_dot(profile_path, summary, nodes, edges, extra_edges, grouped, args.highlight_top)

    if args.output:
        output_path = Path(args.output)
    else:
        output_path = Path.cwd() / f"{profile_path.stem}_dag.{args.format}"

    write_output(dot_text, output_path, args.format)
    print(f"Wrote {args.format.upper()} DAG to {output_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
