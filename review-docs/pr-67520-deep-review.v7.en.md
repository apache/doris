# Independent deep review — round 1

Reviewed the uncommitted protocol fixes over HEAD `12e48c403e0`. Read `.claude/skills/code-review/SKILL.md` Part 1 and `fe/fe-core/AGENTS.md`. Scope: new regressions in MySQL EOF/cursor forwarding, capabilities, Arrow forwarding and ordinary authentication compatibility. No security review; no production or test files modified by this reviewer. Test results below are attributed to the primary agent; this reviewer performed source inspection independently.

## Finding R1 — Major: negotiated capabilities newly reject server-side LOAD DATA INFILE

Anchor: `fe/fe-core/src/main/java/org/apache/doris/mysql/MysqlProto.java:207`:

```java
context.setCapability(new MysqlCapability(context.getServerCapability().getFlags()
        & authPacket.getCapability().getFlags()));
```

The capability intersection is correct for the wire serializer, but a downstream consumer treats CLIENT_LOCAL_FILES as a requirement for every MySQL load, including files read by FE. Concrete case: configure `mysql_load_server_secure_path` to a valid FE directory, use a MySQL client whose handshake omits CLIENT_LOCAL_FILES (for example a client with local-infile disabled), and execute existing server-side `LOAD DATA INFILE '/configured/path/data.csv' INTO TABLE ...` without LOCAL. Previously the context copied server capabilities, which always included CLIENT_LOCAL_FILES, so this path reached the server-file loader. After this change it returns ERR_NOT_ALLOWED_COMMAND before loading.

Evidence at `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/load/MysqlLoadCommand.java:222`:

```java
if (!ctx.getCapability().supportClientLocalFile()) {
    ctx.getState().setError(ErrorCode.ERR_NOT_ALLOWED_COMMAND, "This client is not support"
            + " to load client local file.");
    return;
}
```

Parallel implementation: `fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/LoadCommand.java:499` has the same unconditional check in the LOCAL_FILE job branch. Actual server-file I/O at `fe/fe-core/src/main/java/org/apache/doris/load/loadv2/MysqlLoadManager.java:420`:

```java
} else {
    // server side file had already check after analyze.
    inputStream = Files.newInputStream(Paths.get(file));
}
```

Minimal fix: require CLIENT_LOCAL_FILES only when the data description is client-local, in both command paths. Keep negotiated capabilities. Verify server-side load succeeds with CLIENT_LOCAL_FILES absent, client-local succeeds with it present, and client-local returns a prompt error when absent. This finding is a new consequence of the intersection, not a request to add a historical capability.

## Small cleanup and test feedback

- `StmtExecutor` methods `isForwardedClientDeprecatedEofApplied`, `hasForwardedQueryResultPackets`, and `getForwardedAffectedRows` have no production callers after deleting the lossy guard. Remove these unused wrappers and their obsolete Mockito stubs. `FEOpExecutor.getAffectedRows` is then test-only and also removable if no other caller appears. This simplifies the patch; no extra abstraction needed.
- Initially `testOkAndErrorAreNotRebuilt` did not enable deprecated EOF and therefore exited through the first guard. The primary agent corrected this during review; the current test now reaches the intended no-query-buffers guard.

## Part 1.3 checkpoint conclusions

1. **Goal and proof:** The forwarding and serializer changes address the requested new regressions; one concrete capability consumer regression (R1) must be fixed before sign-off. Source and targeted protocol UT cover empty/nonempty cursor results, old-master packet formats, OK payload preservation and Arrow request building. Primary-agent real-driver matrices supplement these tests.
2. **Small and focused:** Packet normalization is limited to unconfirmed old-master result buffers. It copies packet references once, replaces/removes one metadata boundary, and preserves rows and DML/DDL packets. Remove the obsolete wrappers above; no broader framework is justified.
3. **Concurrency:** No new threads/locks. Normalization occurs synchronously after the forwarding RPC returns and before packets are sent. Per-query result mutation is not shared with the master. Existing cancellation may replace the executor result as before; this patch introduces no separate asynchronous owner or lock ordering.
4. **Lifecycle:** Forwarding constructs a fresh `ConnectContext(null, true, sessionId)` for each request. Restoring capabilities before constructing `StmtExecutor` ensures its serializer observes the restored flags. Cursor intent is request-local; absent flags default false. No static-init/lifetime changes.
5. **Configuration:** No product configuration added. Test cluster configuration is excluded from the review/commit scope.
6. **Compatibility:** Optional Thrift field 1009 can be ignored by old peers. New master accepts missing capability/cursor fields. New follower normalizes buffered old-master EOF and metadata boundaries. Old follower cannot reconstruct a lost cursor flag and can still exhibit its historical cursor hang; this is not fixed and must not be described as fixed. Anonymous modern 9.x cursor clients remain outside the explicitly accepted compatibility target.
7. **Parallel paths:** Local and forwarded MySQL result paths were traced; SHOW structured results are serialized locally. Arrow request building now avoids the unsupported MySQL-channel getter; master-side forwarding still creates its existing MySQL proxy context. R1 identifies the two parallel load-command paths needing a follow-up fix.
8. **Conditional checks:** EOF packet classification uses the reserved short 0xFE header; text row fields beginning with a length-encoded 0xFE require at least nine bytes and cannot collide. Binary rows begin with 0x00. The short/incomplete metadata branch asserts an error final packet instead of silently inventing a successful result. Compatibility conditions are bounded by actual peer/client states.
9. **Coverage:** Targeted tests are meaningful, including 24 combinations of master metadata format/cursor/version/empty result and normalization idempotence. Real-driver matrices reported by the primary agent cover repeated prepared/text queries and prompt wrong-password errors. LOAD paths from R1 need additional regression verification. Full OIDC, Arrow and TLS execution results remain the primary agent's ongoing verification, not asserted by this reviewer.
10. **Expected results:** Regression additions contain constant ordered single-column results and no handwritten result update was performed by this reviewer. Primary agent must retain generation evidence. Existing JUnit 4 conventions are followed in adjacent legacy test classes.
11. **Observability:** No new metrics needed for deterministic local packet rewriting. Existing error/forwarding logs suffice; raw credentials must not be included. No authentication flow or logging changes reviewed here.
12. **Transactions/persistence:** No EditLog, transaction lifecycle, or persistence format changes. Retaining raw DML/DDL OK removes the loss of warning/load metadata and avoids inventing a post-success error/retry.
13. **Data writes/atomicity:** SQL side effects are unchanged except R1's early refusal of previously supported loads. Packet conversion runs after execution and does not repeat statements. Fix R1 and validate real loading before completion.
14. **FE/BE variables:** No FE/BE variable added. Capability is FE-to-FE only; build and restore sites are paired, while Arrow omits MySQL-only fields.
15. **Performance:** O(number of buffered packets) reference copy only on old-master adaptation. Payloads and rows are not copied. New-master confirmed results and DML/DDL bypass normalization. No problematic new hot-loop work or allocations found.
16. **Other issues:** Capability intersection leaves authentication using the original `MysqlAuthPacket`, preserving plugin and SSL auth decisions. Multi-statement channel handling remains in its established path. No additional confirmed production issue found after upstream/downstream tracing.

Round 1 status: **changes requested — R1**. Re-review the minimal R1 fix and completed validation before final PASS.

# Independent deep review — round 2

Incremental review of the R1 fix, dead-code removal and amended tests completed. **R1 is resolved in source; no remaining confirmed Blocker/Major or other production correctness finding.** Build/new load-unit-test execution was still in progress at this review point, so source sign-off does not claim those tests have passed.

- `MysqlLoadCommand.handleMysqlLoadCommand` now uses `mysqlDataDescription.isClientLocal() && !ctx.getCapability().supportClientLocalFile()`.
- `LoadCommand.handleLoadCommand` applies the same condition to its first data description, matching the same description passed to the existing loader. Server-side reads no longer depend on the client's LOCAL_FILES bit; client uploads without that bit still fail before opening an upload stream.
- `MysqlLoadCommandTest.testLocalFilesCapabilityOnlyRequiredForClientUploads` covers all four combinations of client/server file source and present/absent capability in both handlers. It verifies both final status and whether the real command dispatches to the loader (mocked I/O boundary), so it catches the exact R1 regression without requiring new production abstractions.
- The three obsolete `StmtExecutor` wrappers and `FEOpExecutor.getAffectedRows` were removed. Repository Java source search finds no remaining references to the deleted wrappers. Existing forwarding bookkeeping still reads affected rows directly from `TMasterOpResult`; it is unchanged.
- The OK/ERR preservation unit test now enables deprecated EOF as requested. The correction exercises the intended no-query-buffers branch.

Part 1.3 conclusions are unchanged except checkpoints 1, 7 and 13: the identified load regression has now been fixed in both parallel paths. Checkpoint 9 now includes the new load test matrix, with execution pending. No new lifecycle, locking, persistence, observability or FE/BE-passing concerns are introduced by the two-condition fix.

Additional execution evidence reported by the primary agent (not rerun by this reviewer): Arrow follower DDL passed; TLS 1.2/1.3 with Connector/J 8.2/9.5 across four endpoints passed 16 groups; follower/mixed-follower driver matrix passed 60 groups; actual INSERT preserved label/status/txnId. The older BE's failure on an ORDER BY plan also reproduces with the baseline FE, while ordinary table reading succeeds; this is not attributed to the patch. The OSS checkout lacks the complete product OIDC/TLS extension implementation, so do not claim a real end-to-end OIDC provider login was tested. Existing OIDC authentication-routing/encoding/TLS-rejection unit tests and ordinary authentication/TLS execution are distinct validation claims.

`git diff --check` has no production/test Java formatting complaint. Its sole current complaint is an extra blank line at the end of the regression `.out` file; regression output must remain generated by the prescribed script, not manually corrected by this reviewer.

Round 2 source-review status: **PASS — 0 unresolved findings**. The primary agent must attach the final build and test results, and retain the explicit old-follower cursor/anonymous-modern-client and product-OIDC validation limits in delivery.

# Final verification addendum

The final incremental source change only initializes the test's mocked `InternalCatalog` before `ConnectContext.setEnv`; it does not change production behavior. Inspected this setup correction and found no issue.

Verified local evidence directly:

- `test-load-recheck.log`: `BUILD SUCCESS`; `MysqlLoadCommandTest` completed 2 tests with 0 failures and 0 errors. The primary agent separately reports all other 111 targeted tests passed.
- `output/protocol-validation/load-30500-server.log`, `load-30500-client.log`, `load-35886-server.log`, and `load-35886-client.log`: both baseline and candidate successfully executed server `LOAD DATA INFILE` and client `LOAD DATA LOCAL INFILE`; each loaded 2 rows with 0 warnings. The primary agent confirms LOCAL_FILES was disabled for the server-file runs. This closes the real-execution validation gap for R1.
- `output/protocol-validation/error-reuse-results.json`: all 8 Connector/J 8.2/9.5 × four-FE-endpoint error/reuse probes exited successfully. These exercise a subsequent command on the same connection after an error, supplementing the result packet and repeated-query matrices.

Remaining environment/coverage limits are explicit: a new BE ASAN build is blocked by 11 missing third-party dependencies, including Arrow, ADBC, Lance and AWS components, not merely crc32c. Metadata regression cases requiring that new BE were therefore not completed. The earlier old-BE plan-node failure also exists with the baseline FE and is not evidence of a change-induced failure. Full product OIDC provider login was not verified in this OSS checkout. Historical old-follower cursor-flag loss and the agreed anonymous-modern-client exclusion remain as previously documented.

**Final independent review conclusion: PASS — 0 unresolved code findings.** R1 is fixed and now has passing targeted tests plus baseline/candidate real-load evidence. This conclusion applies to the inspected patch and stated validation, and does not turn the explicit environment/coverage limits into claims of completed testing. No additional abstraction or compatibility feature is requested.
