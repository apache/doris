#!/usr/bin/env python3
"""Prepare review guides and a release-branch skill fallback for PR reviews."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
from pathlib import Path, PurePosixPath


SKILL_PATH = ".claude/skills/code-review/SKILL.md"
SKILL_FALLBACK_BRANCHES = {"branch-4.2", "branch-4.1", "branch-4.0"}
GUIDE_REFERENCE = re.compile(r"`([^`\n]+/AGENTS\.md)`")


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
    parser.add_argument("--base-ref", default="", help="Live PR target branch.")
    parser.add_argument("--trusted-ref", default="", help="Immutable workflow commit SHA.")
    parser.add_argument(
        "--skill-prompt-block", type=Path,
        help="Output skill instructions; fallback assets are stored beside this file.",
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


def prepare_review_skill(repo_root: Path, base_ref: str, trusted_ref: str, context_dir: Path) -> str:
    if base_ref not in SKILL_FALLBACK_BRANCHES:
        return (
            "Before reviewing any code, you MUST read and follow the code review skill in this repository. "
            "During review, you must strictly follow those instructions.\n"
        )
    skill_path = SKILL_PATH
    instructions = ""
    # Older release PR heads predate the repository skill. Limit this rollout to
    # the three supported branches, and retain a skill supplied by the checkout.
    if not (repo_root / SKILL_PATH).is_file():
        if not re.fullmatch(r"[0-9a-f]{40}", trusted_ref):
            raise ValueError("Skill fallback requires an immutable workflow commit SHA")
        context_dir = context_dir.resolve()
        context_dir.relative_to(repo_root)
        # Full-history checkout normally already contains the workflow commit.
        # Fetch that exact commit if the PR checkout did not bring it in.
        available = subprocess.run(
            ["git", "cat-file", "-e", f"{trusted_ref}^{{commit}}"],
            cwd=repo_root, capture_output=True,
        )
        if available.returncode:
            subprocess.run(
                ["git", "fetch", "--no-tags", "origin", trusted_ref], cwd=repo_root, check=True,
            )

        def read_trusted(path: str) -> str:
            return subprocess.run(
                ["git", "show", f"{trusted_ref}:{path}"], cwd=repo_root,
                check=True, stdout=subprocess.PIPE, text=True,
            ).stdout

        skill = read_trusted(SKILL_PATH)
        bundle = context_dir / "review-guidance"
        bundle.mkdir()
        sources = {}

        def resolve_guide(match: re.Match) -> str:
            path = match.group(1)
            # The checklist also uses abbreviated paths, e.g. fe/.../persist.
            # Their full paths appear in the module guide table in the skill.
            if "..." in PurePosixPath(path).parts:
                return match.group(0)
            valid_changed_path(path)
            if path not in sources:
                checkout_path = repo_root / path
                if checkout_path.is_file():
                    checkout_path.resolve().relative_to(repo_root)
                    sources[path] = path
                else:
                    target = bundle / path
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_text(read_trusted(path))
                    sources[path] = target.relative_to(repo_root).as_posix()
            return f"`{sources[path]}`"

        # Point the skill directly at existing checkout guides or prepared copies;
        # otherwise the skill would still require paths absent on old branches.
        skill = GUIDE_REFERENCE.sub(resolve_guide, skill)
        target = bundle / SKILL_PATH
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_text(skill)
        skill_path = target.relative_to(repo_root).as_posix()
        manifest = {
            "base_ref": base_ref, "trusted_ref": trusted_ref,
            "skill": skill_path, "guides": sources,
        }
        (context_dir / "review_guidance_sources.json").write_text(json.dumps(manifest, indent=2) + "\n")
        print(f"Prepared code-review skill for {base_ref} from workflow commit {trusted_ref}: {skill_path}")
        instructions = (
            f"The workflow prepared this missing skill and its module guides from commit {trusted_ref}. "
            "The AGENTS.md paths in this skill resolve to the existing checkout guides where available, "
            "or to prepared fallback copies. Existing branch guides and actual branch code take precedence. "
            "Use fallback guides as review checklists; verify each rule against the checked-out branch, "
            "and do not assume newer source layouts or features exist on this release branch. "
            "Source-code paths mentioned inside module guides are relative to the repository root.\n"
        )
    return (
        f"Before reviewing any code, you MUST read and follow the code-review skill at `{skill_path}`. "
        "Pass this exact skill path to every review subagent.\n" + instructions
    )


def main() -> None:
    args = parse_args()
    repo_root = args.repo_root.resolve()
    changed_files = [
        path
        for path in (valid_changed_path(line) for line in args.changed_files.read_text().splitlines())
        if path is not None
    ]
    if args.skill_prompt_block is not None:
        args.skill_prompt_block.write_text(prepare_review_skill(
            repo_root, args.base_ref, args.trusted_ref, args.skill_prompt_block.parent,
        ))
    agents = required_agents(repo_root, changed_files)

    args.required_agents.write_text("".join(f"{path}\n" for path in agents))
    if agents:
        args.prompt_block.write_text("".join(f"- {path}\n" for path in agents))
    else:
        args.prompt_block.write_text("- No AGENTS.md files were found for the changed file ancestors.\n")


if __name__ == "__main__":
    main()
