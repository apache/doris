# AGENTS.md — Doris BE

Scope: `be/`. This file is operational — what to do while developing or
reviewing BE C++ code so the compile-time invariants hold. The gate mechanics
and the `*_fwd.h` convention are documented in `be/README.md` (read it when a
gate fires). Repository-wide rules (commit format, build/test commands,
clang-format/clang-tidy) live in the root `AGENTS.md`; nothing here overrides
them.

## Machine-Checked Obligations (configure-time gates)

BE configure runs `build-support/check-build-hygiene.sh` (option
`ENABLE_BUILD_HYGIENE`, default ON; scripts and self-tests live in
`build-support/` and `build-support/tests/`). It is pure text and takes about
a second — **run it directly after any change touching BE headers, includes,
template instantiations, unity skip lists, or test files that include src
sources**; do not wait for configure or CI to tell you.

Every failure message carries the mechanism and the fix path. The legal way
past a gate is never to bypass it, but one of:

- fix the edge as the message says (forward-declare + include in the `.cpp`,
  or route declarations through a `*_fwd.h`);
- when the change is deliberate, edit the corresponding table
  (rule exception set / `ANGLE_BANS` / `FORWARD_CLOSURE_BUDGETS` /
  `REVERSE_REACH_BASELINES` / `PCH_QUOTED_WHITELIST` / `ALLOW`) **in the same
  commit** and justify it in the commit message — the table diff is the
  review signal.

## Rules while writing BE code

- **Adding an `#include` to a widely-included (hub) header is a design
  decision, not a convenience.** Everything a hub includes is reparsed by
  every TU behind it (~1000 TUs for `exec_env.h`, `thread_context.h`,
  `runtime_state.h`, `function.h`, `dependency.h`, `column.h`, ...). Prefer
  forward declarations; put the real include in the `.cpp`. If many files
  need the declarations, use a `*_fwd.h` (declarations and lightweight
  aliases only — never bodies or non-fwd project includes).
- **Never add a quoted include to `pch/pch.h`.** Every header on the PCH
  rebuilds the whole backend (plus the PCH itself) when touched; it is the
  single most leveraged regression surface in the repo.
- **Keep `extern template` families paired.** An explicit instantiation in a
  `.cpp` needs the matching `extern template` in the header, spelled with the
  same template arguments — a missing extern compiles and links fine and just
  silently re-instantiates in every TU. Instantiations expanded from macros
  (e.g. `DECLARE_OPERATOR` in `operator.cpp`) are invisible to the pairing
  gate on both sides: keep those in sync by hand.
- **A test that `#include`s a be/src `.cpp`** needs that file opted out of
  unity batching via `doris_skip_unity_inclusion` in the owning
  `be/src/.../CMakeLists.txt`; otherwise the BE UT link fails with duplicate
  symbols an hour later. The gate error names the exact entry to add.
- **Do not re-add banned third-party includes** (`fmt`/`boost`/
  `concurrentqueue.h`/`<ranges>`) to the headers listed in `ANGLE_BANS`:
  their bodies were deliberately moved out of line, and `<ranges>` in a src
  header additionally breaks the `-fno-access-control` UT build on libc++.

## Review checkpoints (AI review and self-review)

- [ ] New includes in hub headers: could a forward declaration or `*_fwd.h`
      carry this instead? Does the PR pay a closure/reach budget bump — and
      if so, does the commit message justify it?
- [ ] Any edit to a gate table (`RULES` exceptions, budgets, whitelist,
      `ALLOW`) must be deliberate, minimal, and explained in the same
      commit; an unexplained table edit is a red flag, not a fix.
- [ ] Any change to `pch/pch.h` is near-always wrong; demand the reasoning.
- [ ] New explicit instantiation lists or `extern template` blocks: both
      sides present, same spelling? New test `#include` of a src `.cpp`:
      skip entry present?
- [ ] `git grep` for a deleted/renamed header in skip lists and gate tables:
      stale entries fail configure loudly — fix them in the same PR.
