#!/usr/bin/env python3
"""Find all paths from a group to the root group in a Nereids memo dump."""
from __future__ import annotations

import argparse
import re
from collections import defaultdict
from pathlib import Path
from typing import DefaultDict, Dict, Iterable, List, Set, Tuple

GroupId = int


def parse_memo(file_path: Path) -> Tuple[GroupId, Dict[GroupId, Set[GroupId]], Set[GroupId]]:
    """Parse the memo file to extract the root group and the child relations."""
    root_id: GroupId | None = None
    group_children: DefaultDict[GroupId, Set[GroupId]] = defaultdict(set)
    groups: Set[GroupId] = set()
    current_group: GroupId | None = None

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
        # If the file does not specify the root explicitly, detect it by parents
        all_children = {child for children in group_children.values() for child in children}
        roots = [group for group in groups if group not in all_children]
        if len(roots) != 1:
            raise ValueError("无法确定唯一的根节点, 请检查输入文件")
        root_id = roots[0]

    return root_id, group_children, groups


def build_parent_map(group_children: Dict[GroupId, Set[GroupId]]) -> Dict[GroupId, Set[GroupId]]:
    parents: DefaultDict[GroupId, Set[GroupId]] = defaultdict(set)
    for parent, children in group_children.items():
        for child in children:
            parents[child].add(parent)
    return parents


def enumerate_paths(start: GroupId, root: GroupId, parents: Dict[GroupId, Set[GroupId]]) -> List[List[GroupId]]:
    paths: List[List[GroupId]] = []

    def dfs(node: GroupId, path: List[GroupId], path_set: Set[GroupId]) -> None:
        if node in path_set:
            return
        path.append(node)
        path_set.add(node)

        if node == root:
            paths.append(list(path))
        else:
            for parent in sorted(parents.get(node, [])):
                dfs(parent, path, path_set)

        path.pop()
        path_set.remove(node)

    dfs(start, [], set())
    return paths


def main(argv: Iterable[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="列出给定 Group 到 root Group 的所有路径")
    parser.add_argument("memo", type=Path, help="memo 文件路径")
    parser.add_argument("group", type=int, help="起始的 Group ID")
    args = parser.parse_args(argv)

    root_id, group_children, groups = parse_memo(args.memo)
    parents = build_parent_map(group_children)

    if args.group not in groups:
        raise SystemExit(f"输入的 group {args.group} 不存在于 memo 文件中")

    paths = enumerate_paths(args.group, root_id, parents)
    if not paths:
        print(f"从 group {args.group} 到 root group {root_id} 没有路径")
        return

    print(f"root group: {root_id}")
    for idx, path in enumerate(paths, 1):
        # path 中的顺序是从起点到根, 为了更直观, 直接输出此顺序
        print(f"路径 {idx}: {' -> '.join(f'@{gid}' for gid in path)}")


if __name__ == "__main__":
    main()
