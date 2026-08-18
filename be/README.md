<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Doris BE development notes

## Compile-time hygiene gates

BE configure runs `build-support/check-build-hygiene.sh` (option
`ENABLE_BUILD_HYGIENE`, default `ON`): a set of seconds-level text checks that
keep the backend's compile-time invariants from regressing. They exist because
include-graph regressions are silent -- the code still compiles, only every
build afterwards is slower. The gates make them loud, in the first seconds of
configure, with the mechanism and the fix in the message.

What each gate guards, and what to do when it fires:

* **Header layering rules** (`check-header-deps.py`). A hub header
  (`exec_env.h`, `thread_context.h`, ...) must not reach a named subsystem,
  because everything a hub includes is reparsed by the ~1000 TUs behind it.
  Fix: forward-declare the type in the header and include the real header in
  the `.cpp`; if only declarations are needed by many files, route them
  through a `*_fwd.h` (see below). A genuinely leaf-like header can be added
  to the rule's exception set -- in the same PR, with the reasoning in the
  commit message.
* **Third-party bans**. A few headers deliberately moved their `fmt`/`boost`/
  `<ranges>`-using bodies out of line; the ban keeps the template machinery
  from coming back. Fix: put the code that needs the library into the matching
  `.cpp`.
* **Closure / reach budgets**. The safety net for edges no rule names: light
  hubs must stay light (`--closure <header>` lists what grew), heavy payloads
  must not spread (`--reach <header>` ranks the spreading edges). Fix: cut the
  new edge, or -- when the growth is intended -- bump the number in the budget
  table in the same PR and say why in the commit message. The diff of the
  budget table is the review signal; the gate is never a dead end.
* **PCH whitelist**. `pch/pch.h`'s quoted includes are pinned exactly: every
  header on the PCH rebuilds the whole backend when touched. Adding one is
  almost never right; if it is, change the whitelist in the same PR.
* **extern template pairing** (`check-extern-template-pairing.py`). Every
  `extern template` declaration in a header must have its explicit
  instantiation definition in a `.cpp`, and -- within a family that uses
  externs -- every definition must have its declaration. The missing-extern
  direction is the one silent case: it compiles and links, every TU just
  quietly re-instantiates the specialization. Fix: keep both sides in sync,
  spelling the template arguments the same way.
* **Unity-skip coverage** (`check-unity-skip-coverage.py`). A test that
  `#include`s a be/src `.cpp` needs that file opted out of unity batching
  (`doris_skip_unity_inclusion`), otherwise the BE UT link fails with
  duplicate symbols an hour later. The error names the exact entry to add.

Escape hatch for emergencies: configure with `-DENABLE_BUILD_HYGIENE=OFF`.
Self-tests live in `build-support/tests/` (`run.sh`).

## The `*_fwd.h` convention

`*_fwd.h` headers are the sanctioned way through a layering barrier: they
carry forward declarations (and lightweight aliases) only, so they cost
nothing to include and are exempt from the layering rules by suffix. When a
hub header needs a subsystem's type names but not its definitions, put the
declarations in `<subsystem>_fwd.h` (precedent:
`exec/common/hash_table/phmap_fwd_decl.h`) and include the real headers only
in `.cpp` files. Do not put function bodies, class bodies, or includes of
non-fwd project headers into a `*_fwd.h`.
