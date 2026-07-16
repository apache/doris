# Dynamic CPU Core Query Design

## Goal

Provide a lightweight `CpuInfo` API that reports the CPU core count available under the
current cgroup and `config::num_cores` limits without a dedicated refresh thread or cached
dynamic value.

## Design

`CpuInfo::init()` reads the host CPU core count once and stores the unbounded value in
`host_num_cores_`. A shared helper applies the existing cgroup and `config::num_cores` limits.
Initialization uses the helper to preserve the startup snapshot exposed by `CpuInfo::num_cores()`.

`CpuInfo::get_current_num_cores()` calls the same helper with `host_num_cores_` whenever a caller
needs the current effective value. The getter does not mutate `num_cores_` or any other global
state. It therefore observes later cgroup and dynamic configuration changes while preserving the
existing startup-snapshot behavior for all current `CpuInfo::num_cores()` callers.

The design assumes that the host CPU count does not change during the BE process lifetime. It
supports dynamic container CPU allocation through cgroup quota and cpuset updates, but does not
attempt to detect host CPU hotplug events.

## Removed Behavior

The dynamic atomic cache, explicit refresh API, refresh interval, and dedicated daemon thread are
removed. Callers choose when to query the current value. This PR does not dynamically resize any
existing thread pool.

## Limit and Error Semantics

The shared helper preserves the existing priority and fallback behavior:

1. Start with the stored host CPU core count.
2. Apply `CGroupUtil::get_cgroup_limited_cpu_number()`.
3. Override the result when `config::num_cores` is positive.
4. Clamp the final result to at least one.

Existing cgroup parsing behavior and the meaning of `config::num_cores` are outside this change.

## Testing

Replace the cache/refresh unit test with coverage showing that `get_current_num_cores()` applies
the latest `config::num_cores` value immediately and does not require an explicit refresh call.
The test restores the global configuration after completion.
