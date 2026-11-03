# AGENTS.md — Apache Doris

## Submodule Initialization

Submodules are not part of the default checkout or worktree setup. If the task does not compile Doris or run tests that require submodules, leave all submodules uninitialized. Build and test entry points initialize the submodule paths they require. Only initialize a submodule manually when the task actually needs it and an entry point does not handle it; initialize the exact required path instead of running an unscoped recursive update for every submodule.
