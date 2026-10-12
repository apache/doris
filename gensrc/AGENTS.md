# AGENTS.md — gensrc (thrift / protobuf)

Everything under `gensrc/thrift` and `gensrc/proto` is shared by every release, on the wire and on disk. One rule: a change here must keep every upgrade compatible, 3.1 → 4.0 → 4.1 → 4.2 → master. A field id or enum value means the same thing on every branch that has it, an id is never reused, and a field that only one side has is optional.

## Review

Read every touched definition on HEAD and on each release branch that still takes picks (today branch-4.2, branch-4.1, branch-4.0 and branch-3.1; add a new branch when it is cut) with `git show <remote>/branch-4.2:<path>`. An id or enum value that means something else on another branch, a reused id, or a new required field is a finding. A collision that already exists between two shipped releases cannot be fixed by a PR; note it.
