#!/usr/bin/env python3
"""Find the lowest common ancestors between two groups in a Nereids memo dump."""
from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Optional, Set

GroupId = int


class MemoStructure:
    def __init__(self, root: GroupId, edges: Dict[GroupId, Set[GroupId]], existing_groups: Set[GroupId]):
        self.root = root
        self.edges = edges
        self.groups = existing_groups
        self.parents = self._build_parent_map(edges)

    @staticmethod
    def _build_parent_map(edges: Dict[GroupId, Set[GroupId]]) -> Dict[GroupId, Set[GroupId]]:
        parents: DefaultDict[GroupId, Set[GroupId]] = defaultdict(set)
        for parent, children in edges.items():
            for child in children:
                parents[child].add(parent)
        return parents

    def ancestors(self, node: GroupId) -> Set[GroupId]:
        visited: Set[GroupId] = set()

        def dfs(current: GroupId) -> None:
            if current in visited:
                return
            visited.add(current)
            for parent in self.parents.get(current, ()):  # type: ignore[arg-type]
                dfs(parent)

        dfs(node)
        return visited

    def enumerate_paths_to(self, start: GroupId, target: GroupId) -> List[List[GroupId]]:
        paths: List[List[GroupId]] = []

        def dfs(node: GroupId, current: List[GroupId], seen: Set[GroupId]) -> None:
            if node in seen:
                return
            current.append(node)
            seen.add(node)

            if node == target:
                paths.append(list(current))
            else:
                for parent in sorted(self.parents.get(node, ())):  # type: ignore[arg-type]
                    dfs(parent, current, seen)

            current.pop()
            seen.remove(node)

        dfs(start, [], set())
        return paths


def parse_memo(file_path: Path) -> MemoStructure:
    root_id: Optional[GroupId] = None
    group_children: DefaultDict[GroupId, Set[GroupId]] = defaultdict(set)
    groups: Set[GroupId] = set()
    current_group: Optional[GroupId] = None

    root_pattern = re.compile(r"root:Group\[@(\d+)\]")
    group_pattern = re.compile(r"Group\[@(\d+)\]")
    children_pattern = re.compile(r"@([0-9]+)")

    with file_path.open("r", encoding="utf-8") as memo:
        for raw_line in memo:
            line = raw_line.strip()
            if not line:
                continue

            if root_id is None:
                root_match = root_pattern.search(line)
                if root_match:
                    root_id = int(root_match.group(1))

            group_match = group_pattern.search(line)
            if group_match:
                current_group = int(group_match.group(1))
                groups.add(current_group)
                continue

            if current_group is None:
                continue

            if "children=[" in line:
                child_ids = children_pattern.findall(line)
                if child_ids:
                    group_children[current_group].update(int(child) for child in child_ids)

    if root_id is None:
        all_children = {child for children in group_children.values() for child in children}
        roots = [group for group in groups if group not in all_children]
        if len(roots) != 1:
            raise ValueError("无法确定唯一的根节点, 请检查输入文件")
        root_id = roots[0]

    return MemoStructure(root_id, dict(group_children), groups)


def lowest_common_ancestors(memo: MemoStructure, g1: GroupId, g2: GroupId) -> Set[GroupId]:
    if g1 not in memo.groups:
        raise ValueError(f"group {g1} 不存在于 memo 中")
    if g2 not in memo.groups:
        raise ValueError(f"group {g2} 不存在于 memo 中")

    ancestors1 = memo.ancestors(g1)
    ancestors2 = memo.ancestors(g2)
    intersection = ancestors1 & ancestors2
    if not intersection:
        return set()

    # 计算每个节点到两个起点的距离，以便找到最低的共同祖先
    distances1 = distance_to_ancestors(memo, g1, intersection)
    distances2 = distance_to_ancestors(memo, g2, intersection)

    # 合并距离，选择最小的（层数越小越低）
    total_distance: Dict[GroupId, int] = {
        ancestor: distances1[ancestor] + distances2[ancestor]
        for ancestor in intersection
    }
    min_distance = min(total_distance.values())
    return {ancestor for ancestor, dist in total_distance.items() if dist == min_distance}


def distance_to_ancestors(memo: MemoStructure, start: GroupId, targets: Set[GroupId]) -> Dict[GroupId, int]:
    distances: Dict[GroupId, int] = {}

    def dfs(node: GroupId, depth: int) -> None:
        if node in distances and distances[node] <= depth:
            return
        distances[node] = depth
        for parent in memo.parents.get(node, ()):  # type: ignore[arg-type]
            dfs(parent, depth + 1)

    dfs(start, 0)
    return {target: distances[target] for target in targets}


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="求两个 Group 的最低共同祖先")
    parser.add_argument("memo", type=Path, help="memo 文件路径")
    parser.add_argument("group1", type=int, help="第一个 group id")
    parser.add_argument("group2", type=int, help="第二个 group id")
    args = parser.parse_args(argv)

    memo = parse_memo(args.memo)
    lcas = lowest_common_ancestors(memo, args.group1, args.group2)

    if not lcas:
        print(f"group {args.group1} 与 group {args.group2} 没有共同祖先")
        return

    for idx, lca in enumerate(sorted(lcas)):
        if idx == 0:
            print(f"lowest common ancestor: {lca}")
        else:
            print(f"lowest common ancestor (alternative): {lca}")

        for group_id in (args.group1, args.group2):
            paths = memo.enumerate_paths_to(group_id, lca)
            header = f"{group_id}->{lca}"
            print(header)
            if not paths:
                print("    (no path)")
                continue
            for path_idx, path in enumerate(paths, 1):
                pretty = " -> ".join(str(node) for node in path)
                print(f"    p{path_idx}: {pretty}")


if __name__ == "__main__":
    main()
