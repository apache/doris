#!/usr/bin/env python3
"""Prepare the AGENTS.md guide list for automated PR review prompts."""

from __future__ import annotations

import argparse
from pathlib import Path, PurePosixPath


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--changed-files",
        required=True,
        type=Path,
        help="Newline-delimited PR changed file paths.",
    )
    parser.add_argument(
        "--required-agents",
        required=True,
        type=Path,
        help="Output file containing one required AGENTS.md path per line.",
    )
    parser.add_argument(
        "--prompt-block",
        required=True,
        type=Path,
        help="Output file containing the bullet list inserted into the review prompt.",
    )
    parser.add_argument(
        "--repo-root",
        default=Path("."),
        type=Path,
        help="Repository root. Defaults to the current working directory.",
    )
    return parser.parse_args()


def valid_changed_path(path: str) -> PurePosixPath | None:
    value = path.strip()
    if not value:
        return None

    candidate = PurePosixPath(value)
    if candidate.is_absolute() or ".." in candidate.parts:
        raise ValueError(f"Changed file path escapes the repository: {path!r}")
    return candidate


def add_if_present(repo_root: Path, agents: list[str], seen: set[str], path: PurePosixPath) -> None:
    text_path = path.as_posix()
    if text_path in seen:
        return
    if (repo_root / Path(text_path)).is_file():
        agents.append(text_path)
        seen.add(text_path)


def required_agents(repo_root: Path, changed_files: list[PurePosixPath]) -> list[str]:
    agents: list[str] = []
    seen: set[str] = set()

    add_if_present(repo_root, agents, seen, PurePosixPath("AGENTS.md"))
    for changed_file in changed_files:
        for index in range(1, len(changed_file.parts)):
            directory = PurePosixPath(*changed_file.parts[:index])
            add_if_present(repo_root, agents, seen, directory / "AGENTS.md")

    return agents


def main() -> None:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    changed_files = [
        path
        for path in (valid_changed_path(line) for line in args.changed_files.read_text().splitlines())
        if path is not None
    ]
    agents = required_agents(repo_root, changed_files)

    args.required_agents.write_text("".join(f"{path}\n" for path in agents))
    if agents:
        args.prompt_block.write_text("".join(f"- {path}\n" for path in agents))
    else:
        args.prompt_block.write_text("- No AGENTS.md files were found for the changed file ancestors.\n")


if __name__ == "__main__":
    main()
