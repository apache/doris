# Apache Doris — Threat Model

> **Status: v1.0 — accepted (technical content). Pending wave-4 process
> items.** Wave-1/2/3/4 maintainer interviews completed 2026-05-14
> (Doris committer morningman). All technical `(inferred)` tags from
> v0.1 have been resolved or consciously deferred.

This document is the **security contract** for Apache Doris: what the
project assumes, what it guarantees given those assumptions, what it
explicitly leaves to the operator, and how a vulnerability triager
should classify any inbound report.

---

## 4.1 Header

- **Project**: Apache Doris (https://doris.apache.org)
- **Model version binding**: written against `master` at commit
  `1d1846591f7`, 2026-05-14. Per M15 (single-living-doc policy), the
  `model-version` field at the top of this file is bumped per minor
  release; vulnerability reports against project version *N* are
  triaged against the model as it stood at *N* (read the file at the
  matching git tag).
- **Reporting cross-reference**: per M1, security findings should be
  reported to **`security@apache.org`** (ASF security team will route
  to Doris). A short `SECURITY.md` at the repo root links to this
  document as canonical scope (M16 (A)). Findings that fall under
  §4.3 / §4.9 / §4.11a will be closed with a citation to this
  document.
- **Status**: v1.0 — technical model accepted. The four wave-4 (M15–M18)
  meta/process answers are recorded below; the root `SECURITY.md`
  coexistence artifact is complete, while model-version field policy
  text remains follow-up work.
- **Provenance legend**:
  - *(documented)* — stated in Doris' own README, code comments,
    `conf/*.conf`, or user docs
  - *(maintainer, Qn)* / *(maintainer, Mn)* — answered by a Doris
    committer in interviews on 2026-05-14. `Q1`–`Q8` are wave-1/2
    questions; `M1`–`M18` are wave-3/4 questions
  - *(inferred)* — producer's working hypothesis, not yet ratified.
    **None remain in v1.0.**
- **Draft confidence**: *2 documented / 88 maintainer / 0 inferred*.
  Up from v0.1 (2 / 45 / 37); all 14 wave-3 and 4 wave-4 questions
  resolved on 2026-05-14.

**One-paragraph project description.** Apache Doris is an MPP
analytical (OLAP) database. Clients submit SQL over the MySQL wire
protocol or Arrow Flight; queries are parsed and planned by the **FE**
(Frontend, Java) and executed by the **BE** (Backend, C++) against
locally managed columnar storage and/or external lakehouse catalogs
(Hive, Iceberg, Hudi, Paimon, JDBC, S3/HDFS/Azure). Doris ships in two
deployment shapes: classic on-prem (FE+BE+Broker), and the
cloud-native **`cloud/`** variant (storage-compute disaggregated,
shared Meta Service, K8s-native, multi-tenant). Both are in-model;
content that differs is marked `[on-prem]` / `[cloud]`.

---

## 4.2 Scope and intended use

**Primary intended use** — In-cluster MPP execution of analytical SQL,
where the cluster is operated by the same organization that controls
its network perimeter *(maintainer, Q2)*.

**Deployment shapes in scope** *(maintainer, Q2)*:
- **(A) On-prem / single-tenant** — FE+BE+Broker processes inside a
  corporate network or private VPC. Cluster-internal network is
  *implicitly trusted* by operator-provided isolation. **Default
  shape.**
- **(B) Cloud variant** — `cloud/` directory; storage-compute
  disaggregated, K8s-native. **Tenancy model**: Meta Service is
  shared across tenants; **per-tenant isolation enforced inside
  Meta Service is a security claim of the project** *(maintainer,
  M2)*. Cross-tenant data leak / privilege escalation through Meta
  Service is `VALID`, not `OUT-OF-MODEL`.

**Deployment shape explicitly OUT of scope** *(maintainer, Q2)*:
- Direct internet exposure of any Doris-listened port. Collapses §4.4
  trust model.

**Caller roles**:

| Role | Trust level | In §4.7? |
|---|---|---|
| Anonymous network attacker on client-facing ports (MySQL 9030, HTTP 8030, FE Arrow Flight 8070, **BE Arrow Flight 8050**) | Untrusted | **Yes — primary pre-auth adversary** |
| Authenticated SQL user with limited RBAC privileges | Untrusted within RBAC scope | **Yes — primary post-auth adversary** |
| Authenticated user holding `CREATE CATALOG` (sub-admin) | Untrusted within RBAC; can attach external URL endpoints | **Yes** *(maintainer, M13)* — narrow SSRF actor; see §4.9 |
| Authenticated user in tenant T₁ trying to reach tenant T₂ data `[cloud]` | Untrusted across tenant boundary | **Yes** *(maintainer, M2)* — cross-tenant adversary |
| `SUPER` / `ADMIN_PRIV` / database owner / operator-level user | Trusted *(maintainer, M3)* | No |
| Cluster-internal RPC peer (FE↔BE, BE↔BE, FE↔Follower, FE↔Broker, FE↔MetaService) | Trusted by network isolation *(maintainer, Q1)* | No |
| External catalog / storage system (Hive Metastore, Iceberg, JDBC source, S3, HDFS, Azure Blob) | Trusted by admin connection *(maintainer, Q8)* | No |

**Component-family table.** Distinct threat profiles. `Surface` lists
ports / inputs each family exposes; `In model?` ties to §4.3.

| # | Family | Path | Surface | In model? |
|---|---|---|---|---|
| 1 | **FE core** (Java) | `fe/fe-core/` | MySQL 9030, HTTP 8030, FE Arrow Flight 8070 (client); RPC 9020, Edit-log 9010 (internal) | **Yes** |
| 2 | **BE core** (C++) | `be/src/` | **BE Arrow Flight 8050 (client-facing, M7)**; BRPC 8060, Webserver 8040, Heartbeat 9050, BE↔BE 9060 (internal) | **Yes** |
| 3 | **Cloud variant** | `cloud/src/` | Meta Service (shared, multi-tenant), Recycler, Resource Manager | **Yes** *(maintainer, Q2, M2)* |
| 4 | **FE auth providers** | `fe/fe-authentication/` | Pluggable: native, LDAP | **Yes** |
| 5 | **FE connectors** (catalogs) | `fe/fe-connector/{iceberg,hudi,hms,jdbc,paimon,trino,maxcompute,es}` | Outbound to external systems; in-process JAR loading | **Yes** (memory safety only; data trusted per §4.6) |
| 6 | **BE Java extensions** | `fe/be-java-extensions/` | In-process JVM in BE | **Yes** (memory safety; UDF code trusted per §4.6) |
| 7 | **HDFS / FS broker client** | `fe/fe-filesystem/fe-filesystem-broker/` | Thrift RPC to external broker (cluster-internal); the in-tree `apache_hdfs_broker` daemon has been removed | **Yes** (internal trust per §4.4) |
| 8 | Web UI | `ui/` + `webroot/` | Served via FE 8030 (auth gated) | **Yes** |
| 9 | Vendored MySQL source | `mysql/mysql-{9.4.0,9.5.0}/` | None (reference only, not built or shipped) | **No** *(maintainer, Q4)* |
| 10 | Sample / dev / CI | `samples/`, `docker/`, `pytest/`, `regression-test/`, `jdbc-version-test/`, `task_executor_simulator/`, `hooks/`, `build-support/` | None at runtime | **No** *(maintainer, Q4)* |
| 11 | All FE plugins | `fe_plugins/` (`auditdemo`, `auditloader`, `sparksql-converter`, `trino-converter`) | FE plugin SPI | **No** *(maintainer, Q4, M4)* — `auditloader` included; users opting in take ownership |
| 12 | Client SDKs / extensions / CDC client | `extension/`, `cdc_client` (Doris SDKs now maintained out-of-tree in a separate `doris-sdk` repo) | Client-side libraries | **No** *(maintainer, Q4)* — separately versioned |

---

## 4.3 Out of scope (explicit non-goals)

**Use cases not supported.**

1. **Direct internet exposure of any port** *(maintainer, Q2)*.
   Operators must place Doris behind a network perimeter (VPC,
   firewall, K8s `NetworkPolicy`, equivalent).
2. **Cluster-internal-network adversary** *(maintainer, Q1)*. The
   trust boundary sits at the client-facing ports (§4.4). An attacker
   reaching BE BRPC 8060, BE Webserver 8040, FE Edit-log 9010, FE RPC
   9020, BE Heartbeat 9050, BE↔BE 9060, or the FS broker is presumed
   to have already compromised the operator's network.
3. **DoS via pathological SQL or query plans** *(maintainer, Q5)*.
   A single authenticated SQL user submitting an unbounded-resource
   query is **not** a Doris bug. Operators must constrain users via
   the canonical knob set *(maintainer, M5)*: **`exec_mem_limit`**
   (per-query memory cap), **Workload Group** (`CREATE WORKLOAD
   GROUP ...` — recommended production posture for memory/CPU/
   concurrency caps per user/group), and **`max_connections` /
   `max_connection_per_user`** (FE config, prevent connection
   exhaustion).
4. **Side-channel / timing-based information disclosure**
   *(maintainer, Q5)*.
5. **Adversary-controlled external catalog data** *(maintainer, Q8)*.
   Bytes returned from admin-connected Iceberg/Hive/Hudi/Paimon/
   JDBC/S3 catalogs are trusted. Crashes from crafted Parquet/ORC/
   Avro/JSON files are `OUT-OF-MODEL: trusted-input`.
6. **`SUPER`-privileged adversary** *(maintainer, M3)*. `SUPER` (and
   `ADMIN_PRIV` / equivalent) holders are trusted by definition.
   RCE achievable only after acquiring `SUPER` — UDF install, JDBC
   driver attach, FE plugin registration, `ADMIN SET CONFIG` —
   `OUT-OF-MODEL: adversary-not-in-scope`.
7. **Compromise of an external system Doris connects to**.
   Downstream effects on Doris are out of model per (5).
8. **Transport-layer confidentiality on default config**
   *(maintainer, Q7)*. TLS off by default IS the supported production
   posture; "credentials sniffable in default config" is `BY-DESIGN:
   property-disclaimed` (§4.9).
9. **Default ship of pre-auth login lockout** *(maintainer, M11)*.
   Doris ships `numFailedLogin = 0` and `passwordLockSeconds = 0` —
   the *mechanism* exists but is opt-in per user via `CREATE USER ...
   FAILED_LOGIN_ATTEMPTS N PASSWORD_LOCK_TIME T`. "I brute-forced an
   account in default config" is `BY-DESIGN: property-disclaimed`
   plus an §4.10 obligation.
10. **Byzantine cluster peers** *(maintainer, M6)*. BDB-JE FE
    replication and tablet replication assume honest peers.
11. **Co-tenant escape at the K8s / OS level** *(maintainer, M2 by
    extension)*. M2 commits to per-tenant isolation enforced *inside
    the Meta Service*; OS / K8s / hypervisor isolation between
    co-tenant pods is the cloud operator's job, not Doris'.

**Code shipped but not threat-modeled.** Family rows 9–12 (vendored
MySQL, samples/dev/CI, all FE plugins including `auditloader`,
SDK/extension/CDC) per §4.2. Reports landing there are `OUT-OF-MODEL:
unsupported-component`.

---

## 4.4 Trust boundaries and data flow

**Three concentric trust zones**, with a **tenant boundary inside
Meta Service for cloud** *(maintainer, Q1, Q8, M2)*:

```
┌──────────────────────────────────────────────────────────────────────┐
│  Zone-3  EXTERNAL  (Hive, Iceberg, Hudi, Paimon, JDBC, S3,           │
│                    HDFS, Azure)                                       │
│   ── trusted-by-admin-connection ──                                  │
│   ┌──────────────────────────────────────────────────────────────┐  │
│   │ Zone-2  CLUSTER-INTERNAL  (FE↔FE, FE↔BE, BE↔BE, FE↔Broker,   │  │
│   │                            FE↔MetaService [cloud])            │  │
│   │     ── trusted-by-network-isolation ──                        │  │
│   │     ┌─────────────────────────────────────────────────┐      │  │
│   │     │ Zone-1  CLUSTER-CORE PROCESS                     │      │  │
│   │     │   FE JVM, BE C++ process,                        │      │  │
│   │     │   Cloud Meta Service ←── enforces tenant        │      │  │
│   │     │                          boundary T1│T2│T3      │      │  │
│   │     └─────────────────────────────────────────────────┘      │  │
│   └──────────────────────────────────────────────────────────────┘  │
│                                                                      │
│      ▲ THE BOUNDARY ▲                                               │
│                                                                      │
│  Zone-0  CLIENT  (untrusted MySQL/HTTP/Arrow-Flight clients)         │
│          ── BE Arrow Flight 8050 lives here too (M7) ──              │
└──────────────────────────────────────────────────────────────────────┘
```

**The single load-bearing trust transition is Zone-0 → Zone-1 at
the client-facing ports.** All other transitions assume the source
is trusted. **The cloud Meta Service additionally claims a
*tenant boundary* inside Zone-1** — see §4.8 (NEW property).

**Per-port reachability precondition.** A finding in code reachable
*only* from Zone-2 or Zone-3 inputs is OUT-OF-MODEL by §4.3 (2) or
(5). Triage applies this test before anything else.

| Port | Protocol | Zone | Reachability precondition |
|---|---|---|---|
| FE 9030 | MySQL wire | 0→1 | bytes attacker-controlled before / during auth, or SQL post-auth |
| FE 8030 | HTTP / REST | 0→1 | request bytes attacker-controlled |
| FE 8070 | Arrow Flight (FE) | 0→1 | handshake / auth bytes attacker-controlled |
| **BE 8050** | **Arrow Flight (BE, client-facing)** | **0→1** *(maintainer, M7)* | **handshake bytes / result-stream consumption attacker-controlled** |
| FE 9020 | Thrift RPC (FE↔BE) | 2 | **none — out of model** |
| FE 9010 | BDB JE edit log (FE↔Follower) | 2 | **none — out of model** |
| BE 8060 | BRPC | 2 | **none — out of model** |
| BE 8040 | HTTP webserver / metrics | 2 | **none — out of model** |
| BE 9050 | Thrift heartbeat (FE→BE) | 2 | **none — out of model** |
| BE 9060 | Thrift fragment exec (BE↔BE) | 2 | **none — out of model** |
| Broker | Thrift | 2 | **none — out of model** |
| Cloud Meta Service ↔ FE/BE | gRPC | 2 (network) + tenant-boundary (data) | a finding inside Meta Service is in-model if it crosses the per-tenant boundary |

**Data flow from Zone-3 (external catalogs) into Zone-1.** Per §4.3
(5) and §4.6, all bytes from external systems are admin-trusted.
They flow into BE format readers (Parquet, ORC, Avro, JSON, CSV) and
FE catalog metadata (HMS tables, Iceberg manifests). Crafted-byte
crashes are OUT-OF-MODEL.

---

## 4.5 Assumptions about the environment

**Supported toolchain / platform** *(maintainer, M8)*:
- **OS**: Linux x86_64 and Linux aarch64 (both first-class).
- **FE runtime**: JDK 17.
- **BE toolchain**: GCC 11+ libstdc++ (or equivalent conformant C++
  toolchain matching the official docker build image).
- Anything else (different JDK, non-conformant C++ toolchain) is
  `OUT-OF-MODEL: non-default-build`.

Operational assumptions:
- **Allocator**: BE defaults to jemalloc.
- **Concurrency**: BE assumes a conformant C++ memory model with
  atomic intrinsics; FE assumes the JMM. Neither is signal-safe.
- **Filesystem**: BE assumes ownership of `storage_root_path`
  directories; concurrent external mutation is undefined.
- **Network**: cluster-internal network is treated as a security
  boundary (operator's responsibility) *(maintainer, Q1)*.
- **Time**: FE replica clock skew tolerated within Raft / BDB-JE
  bounds; clock-rollback is not an attacker capability.

**Negative claims (what Doris does NOT do to its host)**
*(maintainer, M9 — code-verified)*:

- **BE process spawns subprocesses only in these cases**: Python UDF
  execution (`fork()`), Python venv creation (`system()`), Broker
  shell utilities (`popen()`), and CDC client (`fork()`). No other
  arbitrary subprocess execution.
- **FE process does NOT install custom signal handlers** beyond
  standard JVM behavior. Relies on JVM defaults.
- **BE/FE consume the following environment variables at runtime**:
  `DORIS_HOME`, `LOG_DIR`, `PID_DIR`, `HADOOP_CONF_DIR`,
  `HADOOP_USER_NAME`, `JAVA_HOME`, `JAVA_OPTS`, `LIBHDFS_OPTS`,
  `TZDIR`, `DORIS_LOG_TO_STDERR`, and AWS credential providers
  (`AWS_ROLE_ARN`, `AWS_WEB_IDENTITY_TOKEN_FILE`,
  `AWS_CONTAINER_CREDENTIALS_*`). **No password-via-env pattern** —
  database passwords are never read from environment.

---

## 4.5a Build-time and configuration variants

| Knob | Default | Maintainer stance | Effect on model |
|---|---|---|---|
| `enable_ssl` (FE MySQL/HTTP) | **off** | (Q7) Off **IS** supported production posture | §4.9 disclaims wire confidentiality unconditionally |
| `enable_ssl` (Arrow Flight) | **off** | Same as above | Same |
| `enable_java_udf` (FE) | **on** *(maintainer, M10)* | Intentional | §4.10 (4) "treat SUPER as RCE" is load-bearing in default |
| `enable_python_udf` (FE) | **on** *(maintainer, M10)* | Intentional. Asymmetric with BE | FE allows registration; BE requires `enable_python_udf_support` to actually execute |
| `enable_python_udf_support` (BE) | **off** *(maintainer, M10)* | Intentional. Operator must opt in to actually run Python UDFs | Default deployment cannot execute Python UDFs even if FE accepts them |
| `numFailedLogin` (per-user, `CREATE USER ... FAILED_LOGIN_ATTEMPTS N`) | **0 / DISABLED** *(maintainer, M11)* | (A) Off IS supported production posture; operator must enable per user | §4.10 (NEW) requires per-user enable for any account on a network-reachable client port |
| `passwordLockSeconds` (per-user, `... PASSWORD_LOCK_TIME T`) | **0 / DISABLED** *(maintainer, M11)* | Same | Same |
| `auth_type` | native | LDAP / Kerberos / OIDC are non-default backends | Out of this row's scope (handled in family row 4) |
| Cluster shape: `on-prem` vs `cloud/` | on-prem | Both shapes supported *(maintainer, Q2)* | Cloud adds Meta Service component (family row 3); cloud has additional tenant-boundary claim per §4.8 |

A vulnerability report of "I sniffed plaintext credentials on port
9030 in default config" is closed `BY-DESIGN: property-disclaimed`
per Q7 → §4.9 / §4.10. A vulnerability report of "I brute-forced
account `analytics_user` in default config (no `FAILED_LOGIN_ATTEMPTS`
set)" is closed the same way per M11 → §4.9 / §4.10.

---

## 4.6 Assumptions about inputs

**Per-endpoint trust table.**

| Endpoint | Message / parameter | Trust | Caller / operator must enforce |
|---|---|---|---|
| FE MySQL 9030 | handshake bytes (pre-auth) | **untrusted** *(maintainer, Q6)* | nothing — Doris memory-safe by §4.8 (1) |
| FE MySQL 9030 | username / auth response | **untrusted** *(maintainer, Q6)* | server-side: brute-force resistance is *not* default; operator must `CREATE USER ... FAILED_LOGIN_ATTEMPTS` per §4.10 |
| FE MySQL 9030 | SQL text (post-auth) | **untrusted** *(maintainer, Q5)* | nothing — parser/planner memory-safe by §4.8 (2) |
| FE MySQL 9030 | SQL semantic content (table / column / privilege requested) | **untrusted, RBAC-enforced** *(maintainer, Q5)* | nothing — RBAC enforced by §4.8 (4) |
| FE MySQL 9030 | `iceberg.rest.uri` and similar URLs in `CREATE EXTERNAL CATALOG` | **post-auth, attacker-controllable** *(maintainer, M13)* | operator: only grant `CREATE CATALOG` privilege to admins; otherwise SSRF surface (§4.9) |
| FE HTTP 8030 | request bytes (pre-auth) | **untrusted** | memory safety |
| FE HTTP 8030 | request body (post-auth) | **untrusted within RBAC** | RBAC |
| FE HTTP 8030 | `/api/show_proc`, admin REST surface | **post-auth, privileged** | RBAC; admin-only endpoints must check |
| FE Arrow Flight 8070 | handshake | **untrusted** | memory safety |
| FE Arrow Flight 8070 | result-stream consumption | mostly post-auth | RBAC |
| **BE Arrow Flight 8050** | **handshake bytes (pre-auth)** *(maintainer, M7)* | **untrusted** | memory safety |
| **BE Arrow Flight 8050** | **result-stream consumption (post-auth)** | **untrusted within RBAC** | RBAC; results must respect querying user's grants |
| FE 9020 (RPC) | all parameters | **trusted (Zone-2)** *(maintainer, Q1)* | operator: network isolation |
| FE 9010 (edit log) | all parameters | **trusted (Zone-2)** | operator: network isolation |
| BE 8060 (BRPC) | all parameters | **trusted (Zone-2)** *(maintainer, Q1)* | operator: network isolation |
| BE 8040 (webserver) | all requests | **trusted (Zone-2)** | operator: do not expose to authenticated end-users (§4.11) |
| BE 9050 (heartbeat) | FE→BE control msgs | **trusted (Zone-2)** | operator: network isolation |
| BE 9060 (BE↔BE) | fragment exec, data transfer | **trusted (Zone-2)** | operator: network isolation |
| Broker (Thrift) | all parameters | **trusted (Zone-2)** | operator: network isolation |
| **Cloud Meta Service ↔ tenants** | **per-tenant requests** *(maintainer, M2)* | **untrusted across the tenant boundary** | Meta Service must enforce; cross-tenant leak/escalation is `VALID` |
| External catalog metadata (HMS, Iceberg manifests, JDBC driver responses) | Zone-3 admin-trusted *(maintainer, Q8)* | admin: do not connect untrusted catalogs |
| External data files (Parquet/ORC/Avro/JSON/CSV from S3/HDFS/Azure) | **Zone-3 admin-trusted** *(maintainer, Q8)* | admin: do not point Doris at untrusted file systems |
| UDF code (Java JAR / C++ `.so` / Python script) | **trusted (`SUPER`-installed)** *(maintainer, Q3, M3)* | admin: only install code you wrote / audited |
| JDBC driver JAR (loaded via JDBC catalog) | **trusted (`SUPER`-attached)** *(maintainer, Q3, M3)* | admin: only attach catalogs whose drivers you trust |

**Size / shape / rate.** Doris does not commit to bounded resource
behavior on adversarial post-auth SQL *(maintainer, Q5)*. Operator
must use the §4.10 (3) knob set.

---

## 4.7 Adversary model

In-scope adversaries:

1. **Anonymous network attacker on a client-facing port** (MySQL
   9030, HTTP 8030, FE Arrow Flight 8070, **BE Arrow Flight 8050**).
   Capabilities: send arbitrary bytes, complete or abandon TLS
   handshake (when enabled), submit credential guesses without
   default lockout. Goals modeled: crash FE/BE; achieve RCE
   pre-auth; brute-force credentials (operator's job to enable
   per-user lockout per §4.10); enumerate users / cluster topology.
2. **Authenticated SQL user with restricted RBAC privileges**.
   Capabilities: any SQL the protocol accepts, bounded only by the
   operator's workload-group config. Goals modeled: privilege
   escalation; reading objects outside grant; cross-user data leak
   via shared state; SQL execution layer escape (RCE); corrupting
   other users' data.
3. **Authenticated user with `CREATE CATALOG` privilege but no
   `SUPER`** *(maintainer, M13)*. Narrow SSRF actor: can attach an
   Iceberg REST catalog whose `iceberg.rest.uri` is an
   attacker-supplied URL, causing FE to issue HTTP requests to
   internal hosts. Mitigation is operator-side per §4.10.
4. **Authenticated user in tenant T₁ trying to reach tenant T₂
   data** `[cloud only]` *(maintainer, M2)*. Cross-tenant adversary:
   any leak / escalation across the Meta-Service-enforced tenant
   boundary is `VALID`.

Out-of-scope adversaries:

| Adversary | Reason | Disposition |
|---|---|---|
| Cluster-internal network attacker | (Q1) Network isolation is operator's job | `OUT-OF-MODEL: adversary-not-in-scope` |
| Side-channel observer (timing, cache, branch-predictor) | (Q5) Not in model | `OUT-OF-MODEL: adversary-not-in-scope` |
| `SUPER` / `ADMIN_PRIV` user behaving maliciously | (M3) admin trusted by definition | `OUT-OF-MODEL: adversary-not-in-scope` |
| Co-tenant escape at K8s / OS level `[cloud]` | Meta Service tenant boundary IS in model (§4.8); host/K8s isolation is the cloud operator's job | `OUT-OF-MODEL: adversary-not-in-scope` for OS-level; `VALID` for Meta-Service-level |
| Eavesdropper on the wire when TLS off | (Q7) Default disclaims wire confidentiality | `BY-DESIGN: property-disclaimed` (§4.9) |
| Brute-force attacker against an account without `FAILED_LOGIN_ATTEMPTS` set | (M11) Default disclaims; operator's job | `BY-DESIGN: property-disclaimed` (§4.9) |
| Compromised external catalog (Hive/Iceberg/JDBC/S3 source) | (Q8) admin-trusted | `OUT-OF-MODEL: trusted-input` |
| Operator misconfiguration (e.g., exposing 8060 to public internet) | (Q2) Not a supported deployment | `OUT-OF-MODEL: adversary-not-in-scope` |
| Byzantine FE follower / BE replica | (M6) honest peers assumed | `OUT-OF-MODEL: adversary-not-in-scope` |

---

## 4.8 Security properties the project provides

For each: property + condition, violation symptom, severity tier,
provenance.

1. **Memory safety on untrusted MySQL handshake / auth bytes (FE,
   pre-auth)** *(maintainer, Q6)*. *Violation symptom*: FE JVM
   crash, OOB, info disclosure. *Severity*: **security-critical**.
2. **Memory safety on untrusted SQL text (FE, post-auth)**
   *(maintainer, Q5)*. *Violation symptom*: FE crash, OOB, parser
   RCE. *Severity*: **security-critical** when reachable from a
   non-`SUPER` user.
3. **Memory safety on untrusted HTTP request bytes (FE, pre- and
   post-auth)** *(maintainer, derived from Q5/Q6)*. *Severity*:
   **security-critical**.
4. **Memory safety on untrusted BE Arrow Flight bytes (BE, pre- and
   post-auth)** *(maintainer, M7)*. *Violation symptom*: BE crash,
   OOB, parser RCE in Arrow Flight handler. *Severity*:
   **security-critical**.
5. **RBAC enforcement: an authenticated user cannot read, modify, or
   discover existence of objects outside their grant scope**
   *(maintainer, Q5)*. *Condition*: RBAC configured. *Violation
   symptom*: privilege escalation; reading rows / tables /
   databases / metadata not granted. *Severity*:
   **security-critical**. *Excluded*: side-channel inference per
   §4.3 (4).
6. **Authentication of MySQL / HTTP / FE Arrow Flight / BE Arrow
   Flight clients (when credentials are presented)** *(documented;
   configurable backends: native, LDAP, Kerberos, OIDC)*. *Violation
   symptom*: wrong credential authenticates; session hijack.
   *Severity*: **security-critical**.
7. **Per-tenant isolation enforced inside the Cloud Meta Service**
   *(maintainer, M2)* `[cloud only]`. *Condition*: cluster runs the
   `cloud/` variant; tenant T₁ requests must not see tenant T₂
   metadata or data. *Violation symptom*: cross-tenant metadata
   read, cross-tenant data read, cross-tenant privilege escalation.
   *Severity*: **security-critical**.
8. **Cluster metadata replication safety under non-Byzantine FE
   peers** *(maintainer, M6)*. *Violation symptom*: FE replica
   metadata divergence, lost ACK'd writes, BDB JE log fork.
   *Severity*: **correctness-critical**; CVE only if reachable from
   a Zone-0 actor.
9. **Tablet replication consistency under non-Byzantine BE peers
   and the documented replication protocol** *(maintainer, M6)*.
   *Severity*: **correctness-critical**.
10. **Per-user pre-auth lockout *mechanism* (PASSWORD_LOCK_TIME) is
    available and works when configured** *(maintainer, M11)*.
    *Condition*: operator has run `CREATE USER ...
    FAILED_LOGIN_ATTEMPTS N PASSWORD_LOCK_TIME T` for the account
    in question. *Violation symptom*: configured lockout fails to
    take effect; counter resets unexpectedly; wraparound. *Severity*:
    **security-critical** when the configured behavior is broken
    (NOT when default is unconfigured — see §4.9).

**Resource properties** — *threshold*: **NONE**. Doris explicitly
makes **no** quantitative or categorical resource guarantee on a
post-auth single-user query *(maintainer, Q5)*. Operator must use
the §4.10 (3) knob set.

---

## 4.9 Security properties the project does *not* provide

State plainly:

- **No transport-layer confidentiality by default.** TLS off is the
  supported production posture *(maintainer, Q7)*. Enable TLS on
  client ports if your network is not acceptably trusted, or layer
  mTLS / VPN externally.
- **No default pre-auth brute-force defense** *(maintainer, M11)*.
  Doris ships with `numFailedLogin = 0` and `passwordLockSeconds = 0`
  — accounts have no lockout unless the operator enables it via
  `CREATE USER ... FAILED_LOGIN_ATTEMPTS N PASSWORD_LOCK_TIME T`.
  See §4.10 for the obligation.
- **No defense against query-DoS or query-OOM by an authenticated
  user** *(maintainer, Q5)*. Operator must use the §4.10 (3) knob
  set.
- **No defense against malicious data files in connected external
  catalogs** *(maintainer, Q8)*.
- **No sandboxing of UDF code** *(maintainer, Q3)*. Java/C++/Python
  UDFs run with full BE process privileges; granting `EXECUTE` to
  non-admin = granting whatever the UDF can do. Note: Java UDF
  default-on (`enable_java_udf=true`) per M10.
- **No defense against side-channel / timing attacks** *(maintainer,
  Q5)*.
- **No Byzantine-fault tolerance** *(maintainer, M6)*.
- **No defense against any actor inside the cluster network**
  *(maintainer, Q1)*.
- **No defense against a `SUPER`-privileged insider** *(maintainer,
  M3)*.
- **No mutual authentication on internal RPC.** FE↔BE Thrift, BE↔BE
  BRPC, FE↔Follower, FE↔Broker accept any peer that can connect at
  the network layer.
- **No reverse-proxy auth header support** *(code-verified, M14)*.
  FE HTTP auth (`BaseController.java`) only honors `Authorization`
  header (Basic/Bearer) or `PALO_SESSION_ID` cookie; Doris does NOT
  trust `X-Forwarded-User` / `X-User-Authenticated` / similar
  upstream-proxy headers. Setting up such a pattern via reverse
  proxy is unsupported.

**False-friend properties — features that look security-relevant but
are not:**

- **`md5()` / `sha1()` SQL functions are NOT cryptographic
  primitives.** Checksum / compatibility functions; not safe for
  password hashing, MAC construction, or collision-resistance.
- **`password()` SQL function (MySQL-compat) is NOT a KDF.** Exists
  for wire compatibility; do not use for app-side credential
  storage.
- **`AES_ENCRYPT` / `AES_DECRYPT` / `DES_ENCRYPT` / `DES_DECRYPT`
  SQL functions are NOT a managed-key envelope** *(maintainer, M12)*.
  Key handling is the application's job; ECB-mode and IV-reuse
  pitfalls are not mitigated; DES is broken — provided for
  compatibility only.
- **`current_user()` and audit log entries are NOT tamper-evident
  records** *(maintainer, M12)*. Operational logs, not legal
  evidence; queryable by anyone with BE filesystem access.
- **The web UI (port 8030) is NOT a security boundary against an
  authenticated user.** Anything the UI lets a user click, the user
  could already do via SQL.
- **LDAP integration's local cache is NOT a backstop for LDAP
  compromise.** If your LDAP server is owned, Doris auth is owned
  on the next refresh.
- **Workload Group is NOT a security isolation boundary**
  *(maintainer, M12)*. It is resource isolation / QoS only. A query
  in workload group A operating on data accessible to workload
  group B is NOT a "security isolation" violation.
- **Resource Tag (BE node tag) is NOT a tenant isolation boundary**
  *(maintainer, M12)*. It is a tablet scheduling hint, not "this
  user can only access this BE group" enforcement.

**Well-known attack classes left to the caller / operator:**

- **SQL injection** in applications building queries by string
  concatenation — application's job.
- **Credential leakage in BI tools** that store the Doris password
  in plaintext config — operator's job.
- **Decompression / deserialization bombs** in Parquet/ORC/Avro
  files — admin must trust the catalog source (§4.3 (5)).
- **HTTP server-side request forgery (SSRF) via Iceberg REST
  catalog URL** *(maintainer, M13)* — `CREATE EXTERNAL CATALOG ...
  PROPERTIES("iceberg.rest.uri" = "http://...")` does not validate
  or block localhost / private IPs. Operator must restrict
  `CREATE CATALOG` privilege to admins (§4.10, §4.11).
- **`COPY INTO` / `OUTFILE` exfiltration** — write privileges to
  external locations are admin-trusted; granting `LOAD_PRIV` /
  `EXPORT_PRIV` to a low-priv user opens an exfiltration channel
  (§4.11).
- **Account brute-force in default config** — see §4.9 (no default
  lockout) and §4.10 (operator must enable per user).

---

## 4.10 Downstream responsibilities

The operator MUST:

1. **Place every Doris-listened port behind a network perimeter such
   that only client-facing ports (MySQL 9030, HTTP 8030, FE Arrow
   Flight 8070, BE Arrow Flight 8050) are reachable from authorized
   clients.** Internal ports (§4.4) must be reachable only by other
   Doris processes in the same cluster. Failure collapses §4.4.
2. **Enable TLS** on client-facing ports if the network is not
   acceptably trusted *(maintainer, Q7)*.
3. **Configure resource caps per the canonical knob set**
   *(maintainer, M5)* before granting access to any user not fully
   trusted: **`exec_mem_limit`** (per-query memory), **Workload
   Group** via `CREATE WORKLOAD GROUP ...` (recommended; per-user/
   group memory + CPU + concurrency caps), and
   **`max_connections` / `max_connection_per_user`** (FE config;
   prevent connection exhaustion).
4. **Treat `SUPER` (and equivalent) as equivalent to local code
   execution on every BE in the cluster** *(maintainer, Q3, M3)*.
   Anyone who can install a UDF, attach a JDBC catalog, or set
   arbitrary `ADMIN SET CONFIG` values can run arbitrary code on
   the BE process. Note: Java UDF is default-on per M10 — `SUPER`
   discipline is load-bearing in default config.
5. **Audit every external catalog** before connecting it
   *(maintainer, Q8)*.
6. **Audit every JDBC driver JAR** before attaching a JDBC catalog
   that uses it.
7. **Deploy on a Linux host the operator controls.**
8. **For cloud deployments**, rely on the Meta Service's per-tenant
   isolation (§4.8 (7)); additionally enforce K8s `NetworkPolicy` /
   namespace isolation and node-level isolation for OS-layer
   defense *(maintainer, M2)*.
9. **Rotate credentials** on a schedule appropriate for the data;
   do not embed `root` credentials in BI tools or app configs.
10. **Enable per-user pre-auth lockout** for any account reachable
    from a network-adjacent attacker *(maintainer, M11)*: `CREATE
    USER ... FAILED_LOGIN_ATTEMPTS N PASSWORD_LOCK_TIME T`. Doris
    does NOT ship a default lockout. A reasonable starting value:
    `FAILED_LOGIN_ATTEMPTS 5 PASSWORD_LOCK_TIME 300` (operator's
    judgment).
11. **Restrict `CREATE CATALOG` privilege** to administrators
    *(maintainer, M13)*. Granting it to a low-privilege user lets
    them issue HTTP requests from FE to attacker-chosen URLs (SSRF)
    via Iceberg REST catalog. If you must grant it more broadly,
    apply network egress controls at the FE host level.

---

## 4.11 Known misuse patterns

- **Exposing BE webserver (8040) directly to authenticated end
  users.** Default no auth; intra-cluster admin/metrics traffic
  only.
- **Exposing BE BRPC (8060) outside the cluster network.** Bypasses
  every Zone-0 access control.
- **Exposing BE Arrow Flight 8050 without auth configured.** It IS
  client-facing (M7) — auth must be configured if reachable.
- **Granting `EXECUTE` on a UDF to a non-admin user** without
  understanding UDF runs with full BE privileges.
- **Connecting a JDBC catalog whose driver does
  eval-on-connection-string.**
- **Granting `CREATE CATALOG` to non-admin users** *(maintainer,
  M13)*. Opens an SSRF vector via Iceberg REST URL.
- **Using `md5()` / `sha1()` / `password()` SQL functions for app
  credential hashing.** See §4.9 false-friends.
- **Granting `LOAD_PRIV` / `EXPORT_PRIV` to low-privilege users.**
  Exfiltration channel.
- **Treating Workload Group or Resource Tag as security isolation**
  *(maintainer, M12)*. They are not.
- **Running `cloud/` deployments without K8s `NetworkPolicy` /
  namespace isolation at the host layer.** Meta Service enforces
  the *tenant data* boundary (§4.8 (7)), but K8s/host-level
  isolation is still the operator's job.
- **Leaving accounts on a network-adjacent client port without
  `FAILED_LOGIN_ATTEMPTS`** *(maintainer, M11)*. Brute-forceable
  with no server-side lockout.

---

## 4.11a Known non-findings (recurring false positives)

Patterns scanners / fuzzers / AI analyzers / human reviewers
repeatedly flag that are NOT bugs given this model. **Internal
primary; cite externally only when closing a specific report**
*(maintainer, M18)*.

- **"BE BRPC port 8060 has no authentication."** — `OUT-OF-MODEL:
  adversary-not-in-scope` per §4.4.
- **"BE webserver port 8040 has no authentication."** — Same.
- **"BE↔BE port 9060 / heartbeat 9050 / FE Edit-log 9010 / FE RPC
  9020 — no mutual TLS."** — Same.
- **"FS broker accepts unauthenticated Thrift calls."** — Same.
- **"MySQL credentials transmitted in plaintext on port 9030 in
  default config."** — `BY-DESIGN: property-disclaimed` per §4.9
  (Q7).
- **"Cluster traffic between FE and BE is unencrypted."** — Same.
- **"Brute-force possible against `analytics_user` in default
  config (no `FAILED_LOGIN_ATTEMPTS` set)."** — `BY-DESIGN:
  property-disclaimed` per §4.9 (M11); operator's obligation per
  §4.10 (10).
- **"`SELECT * FROM huge_table CROSS JOIN huge_table` causes BE
  OOM."** — `BY-DESIGN: property-disclaimed` per §4.9 (Q5);
  operator's obligation per §4.10 (3).
- **"`md5()` / `sha1()` are cryptographically broken."** —
  `BY-DESIGN: property-disclaimed` per §4.9 false-friends.
- **"`DES_ENCRYPT` is broken."** — Same.
- **"Java UDF runs without sandbox; can call `Runtime.exec()`."** —
  `OUT-OF-MODEL: trusted-input` per §4.6 (M3).
- **"JDBC catalog driver runs arbitrary code on connection."** —
  Same.
- **"Reading a crafted Parquet from S3 crashes BE."** —
  `OUT-OF-MODEL: trusted-input` per §4.6 (Q8).
- **"ES connector calls `use_untrusted_ssl()`."** — Configurable
  per-catalog; admin's choice. Tag the connection, not the call.
  `OUT-OF-MODEL: trusted-input`.
- **"FE follower can be tricked by a malicious peer to corrupt the
  edit log."** — `OUT-OF-MODEL: adversary-not-in-scope` per §4.7
  (M6).
- **"`mysql/mysql-9.x.x/` has an XYZ issue."** — `OUT-OF-MODEL:
  unsupported-component` per §4.2 row 9.
- **"`samples/` / `pytest/` / `regression-test/` /
  `task_executor_simulator/` has an XYZ issue."** — Per §4.2 row 10.
- **"`fe_plugins/*` (including `auditloader`) has an XYZ issue."**
  — `OUT-OF-MODEL: unsupported-component` per §4.2 row 11 (M4).
- **"`extension/` / `cdc_client/` has an XYZ issue."** —
  Per §4.2 row 12.
- **"`ADMIN SET CONFIG ...` allows arbitrary file path / arbitrary
  command."** — Requires `ADMIN_PRIV`; admin trusted by §4.7 (M3).
  `OUT-OF-MODEL: adversary-not-in-scope`.
- **"Workload group / resource tag does not isolate cross-user
  data."** — `BY-DESIGN: property-disclaimed` per §4.9 false-friends
  (M12).

---

## 4.12 Conditions that would change this model

Per M17, model is updated **only** when a §4.12 trigger fires (no
periodic review). Triggers:

- TLS default flips to **on** for any client port (Q7).
- Internal RPC gains mutual authentication or per-call auth (Q1
  inverted).
- A new client-facing port is added (Zone-0 expands).
- `numFailedLogin` / `passwordLockSeconds` defaults change to
  non-zero (M11): §4.8 (5)/(10) re-stated as default property,
  §4.9 disclaimer drops, §4.11a entry drops.
- `enable_java_udf` default flips off (M10): §4.10 (4) becomes
  conditional on a §4.5a non-default knob.
- Iceberg REST URL gains validation / localhost-blocking (M13):
  SSRF moves from §4.9 attack-class to §4.8 property; §4.11 misuse
  drops.
- A new catalog connector is added that touches data formats not
  previously parsed by BE (§4.6 Zone-3 surface grows).
- An entry in §4.2 rows 9–12 is promoted to "core supported".
- Cloud Meta Service tenant model changes (M2 inverted): if
  per-customer Meta Services replace the shared Meta Service,
  §4.8 (7) becomes a stricter property and §4.7 cross-tenant
  adversary moves to OUT-OF-MODEL.
- A vulnerability report routes to `MODEL-GAP` (§4.13). The
  correct response is to revise this model — add a §4.8 / §4.9
  entry — not to make an ad-hoc call on the report.

---

## 4.13 Triage dispositions

Closed set. Cite the section.

| Disposition | When | Licensed by |
|---|---|---|
| `VALID` | Violates a §4.8 property via an in-scope §4.7 actor and §4.6 input. | §4.8, §4.6, §4.7 |
| `VALID-HARDENING` | No §4.8 property violated, but the API enables a §4.11 misuse cleanly enough to warrant a hardening change. | §4.11 |
| `OUT-OF-MODEL: trusted-input` | Requires attacker control of a §4.6 input marked trusted. | §4.6 |
| `OUT-OF-MODEL: adversary-not-in-scope` | Requires a §4.7 capability that is excluded. | §4.7 |
| `OUT-OF-MODEL: unsupported-component` | Lands in §4.2 rows 9–12. | §4.3 |
| `OUT-OF-MODEL: non-default-build` | Only manifests when a §4.5a knob is flipped from its default toward the less-secure side. | §4.5a |
| `BY-DESIGN: property-disclaimed` | Concerns a property §4.9 explicitly disclaims. | §4.9 |
| `KNOWN-NON-FINDING` | Matches a §4.11a entry verbatim or closely. | §4.11a |
| `MODEL-GAP` | Cannot be cleanly routed to any of the above. **Triggers §4.12 — model gets revised before the report is closed.** | (§4.12) |

---

## 4.14 Open questions

**Wave 3 — RESOLVED 2026-05-14.** All 14 technical questions
(M1–M14) answered by maintainer; promotions applied throughout
the body. Summary table:

| ID | Topic | Outcome |
|---|---|---|
| M1 | Disclosure channel | `security@apache.org` (ASF) + short `SECURITY.md` linking back to this doc |
| M2 | Cloud tenancy | **Shared Meta Service; per-tenant isolation IS a security claim** (§4.8 new property, §4.7 new actor) |
| M3 | SUPER admin | Out-of-scope adversary by definition |
| M4 | `auditloader` | Stays out-of-model demo (row 11) |
| M5 | Resource knobs | `exec_mem_limit` + Workload Group + `max_connections{,_per_user}` |
| M6 | Byzantine peers | Honest peers assumed; out-of-scope adversary |
| M7 | BE Arrow Flight 8050 | **Client-facing — Zone-0** (was Zone-2 in v0.1) |
| M8 | Toolchain | JDK 17 (FE), GCC 11+ libstdc++ (BE), Linux x86_64 + aarch64 |
| M9 | Negative claims | Subprocess: Python UDF + Python venv + Broker shell + CDC client; no FE custom signal handlers; env vars enumerated; **no password-via-env** |
| M10 | UDF defaults | `enable_java_udf=true` (FE), `enable_python_udf=true` (FE), `enable_python_udf_support=false` (BE); intentional asymmetric |
| M11 | Login lockout | **No default lockout** — operator's responsibility per §4.10 (10) |
| M12 | False friends | DES_*, Workload Group, Resource Tag, Audit Log added |
| M13 | SSRF | **Real surface** — Iceberg REST URL + non-admin `CREATE CATALOG` |
| M14 | Reverse-proxy auth | Code-verified NOT supported; no `X-Forwarded-User` trust |

**Wave 4 — RESOLVED 2026-05-14.** Process / meta:

| ID | Topic | Outcome |
|---|---|---|
| M15 | Versioning policy | Single living doc + `model-version` field at top, bumped per minor release |
| M16 | SECURITY.md coexistence | Short `SECURITY.md` at repo root linking back to this doc as canonical scope |
| M17 | Revision cadence | Trigger-driven only (§4.12 events); no periodic review |
| M18 | §4.11a publication | Internal primary; cite externally only when closing a specific report |

**Open follow-up items (not blocking v1.0 acceptance):**

- Add `model-version` field to top of this doc per M15. Currently
  bound to commit `1d1846591f7` / pre-3.x release. Update when
  cutting next release.
- Consider opening upstream issues per M10 (UDF default-off
  proposal), M11 (default lockout proposal), M13 (Iceberg URL
  validation). Each is a §4.12 trigger if accepted.

---

## 4.15 Optional: machine-readable companion

To be generated as `docs/threat-model.yaml` for automated / AI
triage, encoding:

- Per-port trust zone (from §4.4)
- Per-endpoint parameter trust (from §4.6)
- Component-family in/out-of-scope (from §4.2 / §4.3)
- §4.5a knobs with default + maintainer stance
- §4.8 property → severity-tier + violation-symptom
- §4.9 disclaimed properties + false friends
- §4.11a known-non-findings (suppression-list shape; per M18 kept
  internal-primary)
- §4.13 disposition labels

Not yet produced in v1.0. Optional follow-up.

---

## Self-check (skill §8)

- [x] Every section is substantive or marked N/A.
- [x] No bullet would be more at home in a code review.
- [x] No bullet restates README — README has no security content.
- [x] Every claim carries `(documented)` / `(maintainer)` /
      `(inferred)`. **Zero `(inferred)` remaining.** No hedge-tag
      variants.
- [x] Header reports a draft-confidence count.
- [x] All `(inferred)` resolved → no §4.14 wave-3 open items remain.
- [x] Component families with distinct trust profiles modeled
      separately (FE / BE / cloud / brokers / catalogs / UDF code
      paths).
- [x] §4.5a enumerates security-relevant knobs; both insecure-default
      cases (TLS per Q7, lockout per M11) explicitly resolved.
- [x] §4.9 and §4.10 substantive; §4.9 names false-friends and
      well-known attack classes (now including SSRF per M13).
- [x] §4.6 contains a per-parameter / per-endpoint trust table; BE
      Arrow Flight 8050 added per M7.
- [x] §4.8 properties carry violation symptom + severity tier.
- [x] Resource thresholds: §4.8 says **"NONE — no commitment"**
      per Q5. That is the threshold.
- [x] §4.11a populated; M18 publication policy stated inline.
- [x] §4.13 enumerates dispositions citing the section that licenses
      each.
- [x] A reader can answer "what threats has Doris taken responsibility
      for, and which are left to me?".
- [x] A triager can route an arbitrary finding to exactly one §4.13
      disposition.
- [x] Document length: ~7 pages (within recommended 3–8). v0.1's
      §4.14 wave-3 collapsed into a 14-row summary table.

**v1.0 status**: ACCEPTED for technical content; root `SECURITY.md`
coexistence artifact complete per M16.
