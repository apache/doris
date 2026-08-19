# AGENTS.md — build-support

Operational rules for the architecture/hygiene gates in this directory (the
`check-*.sh` / `check-*.py` scripts wired into FE `mvn validate` and BE
configure). These scripts fail other people's builds — treat them as
production code with a stricter bar than the code they guard.

## Gate maintenance discipline

- **Zero false positives, non-negotiable.** A per-commit gate must be
  deterministic, pure-text, and second-level. Anything heuristic
  (nm audits, closure sweeps, wall-clock trends) belongs in offline reports,
  never in the configure/validate path. One false positive burns more trust
  than ten missed regressions.
- **Fail loud, never silently skip.** A missing tool, a table entry pointing
  at a renamed file, a sentinel nobody references any more — each is an
  error with a message, not a silent pass (precedents:
  `doris_skip_unity_inclusion` in `be/CMakeLists.txt`, the missing-python3
  branch of `check-build-hygiene.sh`, the missing-header errors in
  `check-header-deps.py`).
- **Every failure message carries three parts**: the violation, the
  *mechanism* (why this edge/entry is expensive — model:
  `check-header-deps.py`'s reason/chain/fix form), and a concrete fix path
  the reader can act on without opening the script.
- **Escape hatches are tables in the script, not flags.** Budgets,
  whitelists, exception sets and `ALLOW` lists live next to the rules so a
  deliberate change is a one-line reviewed diff in the same commit. Do not
  add bypass environment variables or config files.
- **Rebaselining budgets**: forward closure budgets carry zero slack (bump =
  explicit, justified edit); reverse reach baselines carry +10% and are
  re-measured on the audit cadence with `check-header-deps.py --budget`.

## Changing a gate script

- Any behavior change to a `check-*` script requires updating its self-test
  in `build-support/tests/` (red/green injection form: every seeded
  violation turns red with the fix path in the message, restoring turns it
  green) and running `bash build-support/tests/run.sh` — all of it, since
  the entry scripts aggregate.
- The BE hygiene self-tests briefly mutate working-tree files (backed up and
  restored by EXIT traps): do not run them concurrently with a
  build/configure of the same tree, and do not "fix" them by pointing at
  fixtures — the gate tables name real headers on purpose.
- Keep scripts portable: bash 3.2 (macOS), BSD *and* GNU userland — in
  particular `sed -i` needs the `-i.bak` + `rm` form. Python: stdlib only,
  no third-party imports.
- Performance envelope: the combined configure gate is ~1s today; keep any
  addition within a low single-digit second budget, with zero build
  dependency (no compiler, no compile_commands.json).
