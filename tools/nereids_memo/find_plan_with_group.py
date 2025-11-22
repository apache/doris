#!/usr/bin/env python3
"""Select the lowest-cost plan that contains a target group from a Nereids memo dump."""
from __future__ import annotations

import argparse
import re
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple

GroupId = int
PropertyKey = str
_STATS_PATTERN = re.compile(r"stats\s*=\s*([^,\s)]+)")


def normalize_property(text: str) -> PropertyKey:
    """Collapse whitespace for stable property comparisons."""
    return re.sub(r"\s+", " ", text.strip())


def parse_cost(cost_text: str) -> float:
    return float(cost_text.replace(",", ""))


def split_child_properties(text: str) -> Tuple[PropertyKey, ...]:
    if not text:
        return tuple()

    items: List[str] = []
    current: List[str] = []
    depth = 0
    for ch in text:
        if ch in "([{":
            depth += 1
        elif ch in ")]}":
            depth = max(depth - 1, 0)
        if ch == "," and depth == 0:
            item = "".join(current).strip()
            if item:
                items.append(normalize_property(item))
            current = []
            continue
        current.append(ch)

    tail = "".join(current).strip()
    if tail:
        items.append(normalize_property(tail))
    return tuple(items)


@dataclass(frozen=True)
class PlanEntry:
    property_key: PropertyKey
    property_display: str
    cost: float
    cost_display: str
    expr_id: int
    plan: str
    children: Tuple[GroupId, ...]
    child_properties: Tuple[PropertyKey, ...]


@dataclass
class PlanNode:
    group_id: GroupId
    property_key: PropertyKey
    expr_id: int
    plan: str
    children: List["PlanNode"]
    stats_rows: Optional[str] = None


@dataclass
class PlanResult:
    cost: float
    cost_display: str
    property_display: str
    node: PlanNode


class MemoStructure:
    def __init__(self) -> None:
        self.groups: Dict[GroupId, Dict[PropertyKey, PlanEntry]] = {}
        self.root_id: Optional[GroupId] = None
        self.child_refs: Set[GroupId] = set()
        self._plan_cache: Dict[Tuple[GroupId, PropertyKey], PlanNode] = {}
        self._building: Set[Tuple[GroupId, PropertyKey]] = set()
        self.group_stats: Dict[GroupId, str] = {}

    @classmethod
    def from_file(cls, path: Path) -> "MemoStructure":
        structure = cls()
        structure._parse(path)
        return structure

    def _parse(self, path: Path) -> None:
        lines = path.read_text(encoding="utf-8").splitlines()
        group_pattern = re.compile(r"Group\[@(\d+)\]")
        root_pattern = re.compile(r"root:Group\[@(\d+)\]")

        current_group: Optional[GroupId] = None
        idx = 0
        total = len(lines)

        while idx < total:
            raw_line = lines[idx]
            stripped = raw_line.strip()

            if not stripped:
                idx += 1
                continue

            if self.root_id is None:
                root_match = root_pattern.search(stripped)
                if root_match:
                    self.root_id = int(root_match.group(1))

            group_match = group_pattern.match(stripped)
            if group_match:
                current_group = int(group_match.group(1))
                self.groups.setdefault(current_group, {})
                idx += 1
                continue

            if current_group is None:
                idx += 1
                continue

            if stripped.startswith("stats"):
                idx += 1
                while idx < total:
                    stat_line = lines[idx]
                    stat_stripped = stat_line.strip()
                    if not stat_stripped:
                        idx += 1
                        continue
                    if not stat_line.startswith("    "):
                        break
                    rows_match = re.search(r"rows\s*=\s*([^\s,]+)", stat_stripped)
                    if rows_match:
                        self.group_stats[current_group] = rows_match.group(1)
                    idx += 1
                continue

            if "lowest Plan" in stripped:
                idx += 1
                idx = self._parse_lowest_plan(current_group, lines, idx)
                continue

            idx += 1

    def _parse_lowest_plan(
        self, group_id: GroupId, lines: Sequence[str], start: int
    ) -> int:
        idx = start
        total = len(lines)

        while idx < total:
            line = lines[idx]
            stripped = line.strip()

            if not stripped:
                idx += 1
                continue

            if not line.startswith("    "):
                break

            property_parts = [stripped]
            idx += 1
            while idx < total:
                lookahead = lines[idx].strip()
                if not lookahead:
                    idx += 1
                    continue
                if lookahead.startswith("id:"):
                    break
                property_parts.append(lookahead)
                idx += 1

            if idx >= total:
                break

            detail_line = lines[idx].strip()
            if not detail_line.startswith("id:"):
                break
            idx += 1

            while idx < total and not lines[idx].strip():
                idx += 1
            if idx >= total:
                break

            child_line = lines[idx].strip()
            idx += 1

            entry = self._build_plan_entry(property_parts, detail_line, child_line)
            self.groups.setdefault(group_id, {})[entry.property_key] = entry
            for child in entry.children:
                self.child_refs.add(child)

        return idx

    def _build_plan_entry(
        self, property_parts: List[str], detail_line: str, child_line: str
    ) -> PlanEntry:
        first = property_parts[0]
        cost_match = re.match(r"([0-9,\.]+)\s+(.*)", first)
        if not cost_match:
            raise ValueError(f"无法解析 lowest plan 行: {first}")

        cost_display = cost_match.group(1)
        property_tokens = [cost_match.group(2)] + property_parts[1:]
        property_display = " ".join(token.strip() for token in property_tokens if token.strip())
        property_key = normalize_property(property_display)

        expr_match = re.search(r"id:(\d+)", detail_line)
        if not expr_match:
            raise ValueError(f"无法解析表达式 ID: {detail_line}")
        expr_id = int(expr_match.group(1))

        children_match = re.search(r"children=\[(.*?)\]", detail_line)
        children: Tuple[GroupId, ...] = tuple()
        if children_match:
            child_tokens = [token.strip() for token in children_match.group(1).split(",") if token.strip()]
            children = tuple(int(token.lstrip("@")) for token in child_tokens)

        plan_start = detail_line.find("plan=")
        if plan_start == -1:
            raise ValueError(f"无法解析 plan: {detail_line}")
        plan_text = detail_line[plan_start + 5 :].strip()
        if detail_line.endswith("))") and plan_text.endswith(")"):
            plan_text = plan_text[:-1].rstrip()

        child_properties = tuple()
        if child_line.startswith("[") and child_line.endswith("]"):
            inner = child_line[1:-1].strip()
            child_properties = split_child_properties(inner)
        else:
            child_properties = tuple()

        if len(children) != len(child_properties):
            if len(children) == 0 and len(child_properties) == 0:
                pass
            else:
                raise ValueError(
                    f"children 与属性数量不匹配: children={children}, props={child_properties}"
                )

        return PlanEntry(
            property_key=property_key,
            property_display=property_display,
            cost=parse_cost(cost_display),
            cost_display=cost_display,
            expr_id=expr_id,
            plan=plan_text,
            children=children,
            child_properties=child_properties,
        )

    def get_root_groups(self) -> List[GroupId]:
        if self.root_id is not None:
            return [self.root_id]
        roots = sorted(set(self.groups.keys()) - self.child_refs)
        if not roots:
            raise ValueError("无法确定根 group")
        return roots

    def build_plan(self, group_id: GroupId, property_key: PropertyKey) -> PlanNode:
        key = (group_id, property_key)
        if key in self._plan_cache:
            return self._plan_cache[key]
        if key in self._building:
            raise ValueError(f"检测到循环引用: group={group_id}, property={property_key}")

        group_entries = self.groups.get(group_id)
        if not group_entries:
            raise ValueError(f"group {group_id} 缺少 lowest plan 信息")
        entry = group_entries.get(property_key)
        if not entry:
            raise ValueError(f"group {group_id} 未记录属性 {property_key}")

        self._building.add(key)
        children_nodes: List[PlanNode] = []
        for child_id, child_property in zip(entry.children, entry.child_properties):
            children_nodes.append(self.build_plan(child_id, child_property))
        node = PlanNode(
            group_id=group_id,
            property_key=property_key,
            expr_id=entry.expr_id,
            plan=entry.plan,
            children=children_nodes,
            stats_rows=self.group_stats.get(group_id),
        )
        self._plan_cache[key] = node
        self._building.remove(key)
        return node

    def find_best_plan_with_group(
        self, target: GroupId, roots: Optional[List[GroupId]] = None
    ) -> Optional[Tuple[GroupId, PlanResult]]:
        if roots is None:
            roots = self.get_root_groups()

        building: Set[Tuple[GroupId, PropertyKey, bool]] = set()
        target_pattern = re.compile(rf"@{target}(?!\d)")

        @lru_cache(maxsize=None)
        def build_with_property(group: GroupId, property_key: PropertyKey, include_target: bool) -> Optional[PlanResult]:
            entries = self.groups.get(group)
            if not entries:
                return None
            entry = entries.get(property_key)
            if entry is None:
                return None

            if not include_target and group == target:
                return None

            key = (group, property_key, include_target)
            if key in building:
                return None
            building.add(key)
            try:
                if not entry.children:
                    if include_target and group != target:
                        return None
                    node = PlanNode(
                        group_id=group,
                        property_key=property_key,
                        expr_id=entry.expr_id,
                        plan=entry.plan,
                        children=[],
                        stats_rows=self.group_stats.get(group),
                    )
                    return PlanResult(entry.cost, entry.cost_display, entry.property_display, node)

                children_nodes = assemble_children(group, entry, include_target)
                if children_nodes is None:
                    return None
                node = PlanNode(
                    group_id=group,
                    property_key=property_key,
                    expr_id=entry.expr_id,
                    plan=entry.plan,
                    children=children_nodes,
                    stats_rows=self.group_stats.get(group),
                )
                return PlanResult(entry.cost, entry.cost_display, entry.property_display, node)
            finally:
                building.remove(key)

        @lru_cache(maxsize=None)
        def build_any(group: GroupId, include_target: bool) -> Optional[PlanResult]:
            entries = self.groups.get(group)
            if not entries:
                return None
            best: Optional[PlanResult] = None
            for property_key in sorted(entries):
                result = build_with_property(group, property_key, include_target)
                if result is None:
                    continue
                if best is None or result.cost < best.cost:
                    best = result
            return best

        def assemble_children(group: GroupId, entry: PlanEntry, include_target: bool) -> Optional[List[PlanNode]]:
            child_infos: List[Tuple[Optional[PlanResult], Optional[PlanResult]]] = []
            for child_id, child_property in zip(entry.children, entry.child_properties):
                without = build_with_property(child_id, child_property, False)
                with_property = build_with_property(child_id, child_property, True)
                with_any = build_any(child_id, True)
                best_with = with_property
                if with_any is not None and (best_with is None or with_any.cost < best_with.cost):
                    best_with = with_any
                child_infos.append((without, best_with))

            assigned: List[PlanNode] = []
            has_target = group == target or bool(target_pattern.search(entry.plan))

            for without, with_candidate in child_infos:
                if without is None:
                    if not include_target:
                        return None
                    if with_candidate is None:
                        return None
                    assigned.append(with_candidate.node)
                    has_target = True
                else:
                    assigned.append(without.node)

            if include_target:
                # Prefer cheaper plans that already include target.
                for idx, (without, with_candidate) in enumerate(child_infos):
                    if with_candidate is None:
                        continue
                    if assigned[idx] is with_candidate.node:
                        has_target = True
                        continue
                    if without is None or with_candidate.cost < without.cost:
                        assigned[idx] = with_candidate.node
                        has_target = True

                if not has_target:
                    best_idx: Optional[int] = None
                    best_delta = float("inf")
                    for idx, (without, with_candidate) in enumerate(child_infos):
                        if with_candidate is None:
                            continue
                        if without is None:
                            best_idx = idx
                            break
                        delta = with_candidate.cost - without.cost
                        if delta < best_delta:
                            best_delta = delta
                            best_idx = idx
                    if best_idx is None:
                        return None
                    assigned[best_idx] = child_infos[best_idx][1].node  # type: ignore[index]
                    has_target = True
            else:
                # Ensure target does not appear in any selected child.
                for node in assigned:
                    if node_contains(node):
                        return None

            return assigned

        def node_contains(node: PlanNode) -> bool:
            if node.group_id == target:
                return True
            if target_pattern.search(node.plan):
                return True
            return any(node_contains(child) for child in node.children)

        best: Optional[Tuple[GroupId, PlanResult]] = None
        for root in roots:
            result = build_any(root, True)
            if result is None:
                continue
            if best is None or result.cost < best[1].cost:
                best = (root, result)
        return best


def _plan_with_stats(node: PlanNode) -> str:
    plan = node.plan
    rows = node.stats_rows
    if not rows:
        return plan

    match = _STATS_PATTERN.search(plan)
    if match:
        start, end = match.span(1)
        return plan[:start] + rows + plan[end:]

    open_idx = plan.find("(")
    close_idx = plan.rfind(")")
    if open_idx != -1 and close_idx > open_idx:
        inner = plan[open_idx + 1 : close_idx].strip()
        prefix = plan[: open_idx + 1]
        suffix = plan[close_idx:]
        if inner:
            return f"{prefix} stats={rows}, {inner}{suffix}"
        return f"{prefix} stats={rows}{suffix}"

    return f"{plan} (stats={rows})"


def format_plan(node: PlanNode) -> List[str]:
    lines = [_plan_with_stats(node)]
    for idx, child in enumerate(node.children):
        lines.extend(_format_child(child, "", idx == len(node.children) - 1))
    return lines


def _format_child(node: PlanNode, prefix: str, is_last: bool) -> List[str]:
    connector = "+--" if is_last else "|--"
    line = f"{prefix}{connector}{_plan_with_stats(node)}"
    lines = [line]
    child_prefix = prefix + ("   " if is_last else "|  ")
    for idx, child in enumerate(node.children):
        lines.extend(_format_child(child, child_prefix, idx == len(node.children) - 1))
    return lines


def main(argv: Optional[Iterable[str]] = None) -> None:

    parser = argparse.ArgumentParser(description="选择包含指定 group 的最低成本计划")
    parser.add_argument("memo", type=Path, help="memo 文件路径")
    parser.add_argument("group", type=int, help="需要包含的 group id")
    parser.add_argument("--root", type=int, default=None, help="可选: 指定根 group")
    args = parser.parse_args(argv)

    memo = MemoStructure.from_file(args.memo)
    if args.group not in memo.groups:
        raise SystemExit(f"group {args.group} 不存在于 memo 中")

    roots = [args.root] if args.root is not None else memo.get_root_groups()
    best = memo.find_best_plan_with_group(args.group, roots)

    if best is None:
        print(f"无法找到包含 group {args.group} 的计划")
        return

    root_id, result = best
    header = (
        f"root group {root_id}, property='{result.property_display}', "
        f"cost={result.cost_display}"
    )
    print(header)
    print(f"plan covering group {args.group}:")
    for line in format_plan(result.node):
        print(line)


if __name__ == "__main__":
    main()
