# SPM DDL command package (Phase 1)

This package holds the DDL command classes of SPM (SQL Plan Management), corresponding
to design doc sections 6.8 / 6.9 / 6.13 / 6.17.

## Command list

| Command class | Syntax | Delegates to |
| --- | --- | --- |
| `CreateBaselinePlanCommand` | `CREATE [GLOBAL \| SESSION] BASELINE PLAN 'bindSql' WITH 'planSql'` | `SPMPlanner.buildBaselineFromSql` -> `SessionBaselineStore.createBaseline` (SESSION) / `BaselineManager.createBaseline` (GLOBAL) |
| `AlterBaselinePlanCommand` | `ALTER BASELINE PLAN <id> [ENABLE \| DISABLE]` | id range (`BaselineScope.ofId`) -> `SessionBaselineStore.updateStatus` (SESSION) / `BaselineManager.updateStatus` (GLOBAL) |
| `DropBaselinePlanCommand` | `DROP BASELINE PLAN [IF EXISTS] <id>` | id range (`BaselineScope.ofId`) -> `SessionBaselineStore.dropBaseline` (SESSION) / `BaselineManager.dropBaseline` (GLOBAL) |
| `ShowBaselinePlansCommand` | `SHOW BASELINE PLANS [LIKE 'pattern' \| WHERE expr]` | `SessionBaselineStore.getAllBaselines` + `BaselineManager.getAllBaselines` |

## Grammar wiring (Phase 1, done)

1. Lexer/grammar: `BASELINE` / `DISABLE` tokens were added to `DorisLexer.g4` (and to the
   `nonReserved` keyword list in `DorisParser.g4`). The `supportedSpmStatement` rule was
   added to `statementBase` in `DorisParser.g4` with the four alternatives
   `createBaselinePlan` / `showBaselinePlans` / `alterBaselinePlan` / `dropBaselinePlan`.
2. AST mapping: the four `visit*` methods in `LogicalPlanBuilder` map the parse trees to
   the command classes.
3. Command execution: `run()` / `doRun()` are implemented, delegating to
   `BaselineManager.getInstance()` / the connection's `SessionBaselineStore` (routed by
   `BaselineScope.ofId(id)`) / `SPMPlanner` / `SPMOptimizer`.
4. Privileges: all SPM management commands require the ADMIN global privilege and throw
   `DdlException` in cloud mode (`checkSupportedInCloudMode`).

## PlanType correspondence

The four command types are registered in PlanType:
CREATE_BASELINE_PLAN_COMMAND, ALTER_BASELINE_PLAN_COMMAND,
DROP_BASELINE_PLAN_COMMAND, SHOW_BASELINE_PLANS_COMMAND.

## CREATE BASELINE PLAN flow (design doc 6.13)

1. `SPMPlanner.buildBaselineFromSql(ctx, bindSql, planSql)`:
   - parses `bindSql` / `planSql` into unbound plans and parameterizes the WHOLE trees
     with one shared `SPMPlaceholderBuilder` (aligned placeholder ids);
   - runs `SPMOptimizer.optimize` on the parameterized plan tree (state-sensitive Nereids
     rules disabled) and decompiles the best physical plan with `SPMPlan2SQLBuilder`;
   - assembles the `BaselinePlan`, including `queryId` = the audit_log query id of this
     CREATE statement (`DebugUtil.printId(ctx.queryId())`, "NaN" when absent).
2. GLOBAL: `BaselineManager.createBaseline` persists it (duplicate detection returns the
   existing id, giving natural "IF NOT EXISTS" semantics; the id generator is aligned
   with the persistence watermark `MAX(id)` before every allocation). SESSION: kept in
   the connection's `SessionBaselineStore`, ids from the session range [2^62, 2^63).
3. The result message carries the created baseline id.

## Result set

The first 12 columns of `ShowBaselinePlansCommand.META_DATA` mirror
`InternalSchema.SPM_BASELINES_SCHEMA` one-to-one (the spm_baselines internal table, see
design doc 6.14.1; `query_id` is the audit_log correlation id of the statement that
produced the baseline); the trailing `scope` column (GLOBAL / SESSION) is synthesized.
The table is created by `InternalSchemaInitializer` at FE startup; adding a column to an
existing table requires the `ALTER TABLE ... ADD COLUMN ... AFTER <col>` migration noted
in design doc 4.3 (column order = `InternalSchema` order = positional INSERT value order).
