// Clean-room adversarial RE-REVIEW of all MaxCompute functional paths (cutover vs legacy).
//
// HOW TO RUN (next session):
//   Workflow({ scriptPath: "plan-doc/reviews/maxcompute-full-rereview.workflow.js" })
//   optional tuning:  args: { verifyVotes: 3, lensesPerDomain: 2, includeBe: true }
//
// DISCIPLINE (see plan-doc/HANDOFF.md "Clean-room 铁律"):
//   - Phase A (Review) + Phase B (Verify) agents are CODE-ONLY. Their prompts contain ONLY source
//     pointers (fe/ be/ gensrc/) and "compare cutover vs legacy". They are told NOT to read any
//     plan-doc/ design/review/decisions/deviations/HANDOFF/memory — to keep judgment uncontaminated.
//   - Phase C (CrossCheck) is the ONLY phase allowed to read the development history (the QUARANTINE),
//     and only to classify already-independently-confirmed findings.
//   - The P4-T06d fixes themselves are IN SCOPE and judged fresh; "it was fixed / mutation-proven"
//     is a prior that never enters Phase A/B.
//
// The script returns structured data; the orchestrator writes
//   reviews/P4-maxcompute-full-rereview-<date>.md   from it (stamp the date when writing).

export const meta = {
  name: 'maxcompute-full-rereview',
  description: 'Clean-room adversarial re-review of MaxCompute read/write/DDL/replay/cache/fallback (cutover vs legacy)',
  phases: [
    { title: 'Review', detail: 'per-domain x lens clean-room reviewers (code-only, no plan-doc)' },
    { title: 'Verify', detail: '3 refute-by-default skeptics per finding (code-only)' },
    { title: 'CrossCheck', detail: 'classify survivors vs quarantined history (Phase C only)' },
  ],
}

const REPO = '/mnt/disk1/yy/git/wt-catalog-spi'
const verifyVotes = (args && args.verifyVotes) || 3
const lensesPerDomain = (args && args.lensesPerDomain) || 2          // 1 = parity only; 2 = + delivery/fallback
const includeBe = !args || args.includeBe !== false                  // default: include BE C++ paths

// ---- shared clean-room contract (NO conclusions, NO plan-doc) ----
const CLEANROOM = `You are a CLEAN-ROOM code reviewer. Repo root: ${REPO}.
CONTEXT: MaxCompute's functional paths were re-implemented during a connector-SPI "cutover". After the
cutover a max_compute catalog is instantiated as PluginDrivenExternalCatalog and its tables are
TableType.PLUGIN_EXTERNAL_TABLE. The pre-cutover ("legacy") implementation still exists in the tree
(mainly under fe/fe-core/src/main/java/org/apache/doris/datasource/maxcompute/ and sibling legacy
classes). Your job: judge the CURRENT cutover implementation INDEPENDENTLY and compare it against the
legacy implementation.
STRICT DISCIPLINE:
  - Read ONLY source code: fe/, be/, gensrc/. Use git/grep/file reads.
  - DO NOT read anything under plan-doc/ (designs, reviews, decisions-log, deviations-log, HANDOFF,
    PROGRESS) and DO NOT rely on any remembered project conclusions. Form your opinion from code alone.
  - Make NO assumption that anything "was fixed", "is correct", or "was verified". Treat the current
    code as unaudited.
  - Every finding MUST cite file:line and state the CUTOVER vs LEGACY behavioral difference and whether
    it is a regression (yes/no/unsure). "The code intends X" is not evidence — verify X actually holds.
  - Report only real, evidence-backed issues OR genuine cutover-vs-legacy divergences. No speculative
    style nits. If a path is correct and matches legacy, say so (zero findings is a valid result).`

// ---- the 6 domains: neutral scope + entry points + open questions (NO verdicts) ----
const DOMAINS = [
  {
    key: 'read',
    title: 'Read / SELECT',
    scope: `CUTOVER: datasource/PluginDrivenExternalTable (toThrift / initSchema / getFullSchema);
datasource/PluginDrivenScanNode; fe-connector-maxcompute/.../MaxComputeScanPlanProvider,
MaxComputeScanRange, MaxComputeConnectorMetadata (buildTableDescriptor / getTableSchema / split);
be-java-extensions/max-compute-connector/.../MaxComputeJniScanner.
${includeBe ? 'BE: be/src/exec/scan/file_scanner.cpp; be/src/runtime/descriptors.cpp; be/src/format/table/max_compute_jni_reader.cpp; gensrc/thrift/Descriptors.thrift (TMCTable).\n' : ''}LEGACY BASELINE: datasource/maxcompute/MaxComputeExternalTable (toThrift); maxcompute/source/MaxComputeScanNode, MaxComputeSplit.`,
    questions: `What table descriptor TYPE and FIELDS does the cutover toThrift produce, and how does BE consume it
(${includeBe ? 'descriptors.cpp factory + file_scanner.cpp cast + max_compute_jni_reader.cpp' : 'BE side'})? Same as legacy?
Split size/offset semantics (byte_size vs row_offset sentinel)? Predicate pushdown incl. CAST / datetime / source
time-zone? Partition pruning (does cutover prune or full-scan)? Column properties (e.g. isKey) as surfaced in
DESCRIBE / information_schema? limit-split optimization trigger conditions vs config default? How do
endpoint/project/quota/credentials reach BE?`,
  },
  {
    key: 'write',
    title: 'Write / INSERT',
    scope: `CUTOVER: nereids/.../insert/PluginDrivenInsertExecutor; planner/PluginDrivenTableSink;
transaction/PluginDrivenTransactionManager; fe-connector-maxcompute/.../MaxComputeConnectorTransaction + write-plan/sink.
${includeBe ? 'BE: MaxCompute writer + block-allocation RPC (FrontendServiceImpl.getMaxComputeBlockIdRange, TMaxComputeBlockId*).\n' : ''}LEGACY BASELINE: nereids/.../insert/MCInsertExecutor; transaction/.../MCTransaction; legacy MC sink.`,
    questions: `Transaction lifecycle (begin / finalizeSink / beforeExec / commit / abort / rollback) vs legacy — equivalent?
Where do reported affected-rows come from? Is the block-count limit honored (Config.max_compute_write_max_block_count)?
Commit protocol (TBinaryProtocol / TMCCommitData)? How are post-commit cache-refresh failures handled vs legacy?
Parallel vs single-writer distribution?`,
  },
  {
    key: 'ddl',
    title: 'DDL (CREATE/DROP TABLE, CREATE/DROP DB)',
    scope: `CUTOVER: datasource/PluginDrivenExternalCatalog (createTable / createDb / dropDb / dropTable);
nereids/.../info/CreateTableInfo (paddingEngineName / checkEngineWithCatalog / analyzeEngine / CTAS path);
connector/ddl/CreateTableInfoToConnectorRequestConverter; fe-connector-maxcompute/.../MaxComputeConnectorMetadata (DDL).
LEGACY BASELINE: datasource/maxcompute/MaxComputeMetadataOps.
NOTE: createTable/dropTable/initSchema on the PluginDriven classes are SHARED by jdbc/es/trino + max_compute.`,
    questions: `Local-name -> remote-name resolution for create & drop (with name-mapping on AND off)? Engine inference and
catalog-engine consistency check? Column-constraint / partition-desc / distribution-desc validation vs legacy?
ifExists / ifNotExists semantics? CREATE-time existence precheck? DROP DATABASE FORCE cascade? Edit-log content and
the cache-invalidation it pairs with (local vs remote names)? Any behavior change for the shared jdbc/es/trino path?`,
  },
  {
    key: 'replay',
    title: 'Metadata replay / editlog / image',
    scope: `CUTOVER: datasource/ExternalCatalog (replayCreateTable / replayDropTable / replayCreateDb / replayDropDb,
incl. the metadataOps==null branch); persist/CreateTableInfo, DropInfo, CreateDbInfo, DropDbInfo;
PluginDrivenExternalCatalog.gsonPostProcess, PluginDrivenExternalTable.gsonPostProcess;
CatalogFactory / GsonUtils registerCompatibleSubtype; InitCatalogLog.Type.
LEGACY BASELINE: MaxComputeExternalCatalog + MaxComputeMetadataOps.afterCreateDb/afterDropDb/afterCreateTable/afterDropTable; legacy gson registration.`,
    questions: `Does the replay path (no metadataOps) correctly rebuild the FE cache? Follower-FE behavior on replay? Image
deserialization of old resource-backed / migrated catalogs (ES/JDBC -> PluginDriven)? The execution ORDER of edit-log
write vs cache invalidation on the master vs legacy? Is the replay key the local or remote name? Are the GSON
catalog/db/table compat registrations all present and consistent?`,
  },
  {
    key: 'cache',
    title: 'Metadata cache',
    scope: `CUTOVER: datasource/ExternalMetaCacheMgr; SchemaCache, SchemaCacheValue, PluginDrivenSchemaCacheValue;
ExternalCatalog / ExternalDatabase metaCache + makeSureInitialized / resetMetaCacheNames / unregister* / invalidate;
partition-value sourcing in PluginDrivenExternalTable.
LEGACY BASELINE: maxcompute/MaxComputeExternalMetaCache; maxcompute/MaxComputeSchemaCacheValue.`,
    questions: `What schema-cache-value type and fields (partition columns / values / types)? Does legacy keep a second-level
partition-VALUE cache, and does the cutover (per-query connector list vs cached)? Invalidation / refresh / TTL timing vs
legacy? Cast safety of any (PluginDrivenSchemaCacheValue) downcast — can a plain SchemaCacheValue ever be cached for a
PluginDriven table? Row-count / statistics cache? Cache key (NameMapping; local vs remote)?`,
  },
  {
    key: 'fallback',
    title: 'Residual / fallback to legacy logic',
    scope: `Cross-cutting. Self-drive with grep + reads across fe/ (and be/ if relevant). Look at EVERY dispatch keyed on
legacy MaxCompute types and any silent fallback:
  - grep: "instanceof MaxComputeExternalCatalog", "instanceof MaxComputeExternalTable", "MAX_COMPUTE_EXTERNAL_TABLE",
    "registerCompatibleSubtype", and any post-cutover-reachable construction/call of legacy datasource.maxcompute.* classes.
  - TableType-driven routing; PluginDrivenExternalTable.toThrift null / SCHEMA_TABLE fallback branch;
    BindRelation / getEngine / getEngineTableTypeName routing; the keep-set (image/plan/thrift compat).`,
    questions: `After cutover (catalog = PluginDrivenExternalCatalog), which code paths STILL hit legacy MaxCompute logic, or
SILENTLY fall back to a generic/legacy path instead of failing loud? Which keep-set items are necessary compat vs true
residue? Any half-wired dispatch (a BE handler wired but its FE analyze gate not, or vice-versa)? For each, cutover-vs-legacy
diff + regression judgment.`,
  },
]

// lens angles applied within each domain (clean-room, code-only)
const LENS_ANGLES = [
  { key: 'parity', focus: `LEGACY-PARITY & CORRECTNESS: does the cutover preserve the legacy observable behavior on this path?
Enumerate concrete cutover-vs-legacy differences and classify each as regression / intentional-divergence / none. Verify the
actual data/control flow, not the apparent intent.` },
  { key: 'delivery', focus: `IMPLEMENTATION DELIVERY & EDGE/FALLBACK: does the implementation fully realize what the code
structure implies, or are there gaps, half-wired seams, silent fallbacks, missing fail-loud, untested invariants, or edge
cases (empty/null/zero, name-mapping on, follower/replay, concurrency) that diverge from legacy? Cite file:line.` },
]

const FINDINGS_SCHEMA = {
  type: 'object', additionalProperties: false,
  properties: {
    parity_assessment: { type: 'string', description: 'one-paragraph independent verdict: does this path reach legacy parity? design vs implementation gap?' },
    findings: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        properties: {
          title: { type: 'string' },
          severity: { type: 'string', enum: ['blocker', 'major', 'minor', 'nit'] },
          category: { type: 'string', enum: ['correctness', 'parity', 'regression', 'design-impl-gap', 'fallback', 'cache', 'replay', 'other'] },
          location: { type: 'string', description: 'file:line' },
          description: { type: 'string' },
          cutover_vs_legacy: { type: 'string', description: 'the concrete behavioral difference' },
          regression: { type: 'string', enum: ['yes', 'no', 'unsure'] },
          why_it_matters: { type: 'string' },
        },
        required: ['title', 'severity', 'category', 'location', 'description', 'cutover_vs_legacy', 'regression', 'why_it_matters'],
      },
    },
  },
  required: ['parity_assessment', 'findings'],
}
const VERDICT_SCHEMA = {
  type: 'object', additionalProperties: false,
  properties: { refuted: { type: 'boolean' }, confidence: { type: 'string', enum: ['low', 'medium', 'high'] }, reasoning: { type: 'string' } },
  required: ['refuted', 'confidence', 'reasoning'],
}
const CROSSCHECK_SCHEMA = {
  type: 'object', additionalProperties: false,
  properties: {
    status: { type: 'string', enum: ['new-gap', 'known-degradation', 'already-handled', 'disagreement-with-history', 'false-positive'] },
    evidence: { type: 'string', description: 'cite the plan-doc section/commit and/or code' },
    recommended_action: { type: 'string' },
  },
  required: ['status', 'evidence', 'recommended_action'],
}

// ===================== Phase A — clean-room review (per domain x lens) =====================
phase('Review')
const lenses = LENS_ANGLES.slice(0, Math.max(1, Math.min(LENS_ANGLES.length, lensesPerDomain)))
const reviewJobs = []
for (const d of DOMAINS) {
  for (const lens of lenses) {
    reviewJobs.push({ domain: d, lens })
  }
}
const reviewResults = await parallel(reviewJobs.map(job => () =>
  agent(
    `${CLEANROOM}\n\n==== DOMAIN: ${job.domain.title} ====\nSCOPE / ENTRY POINTS:\n${job.domain.scope}\n\nOPEN QUESTIONS (neutral; investigate, do not assume answers):\n${job.domain.questions}\n\nLENS: ${job.lens.focus}\n\nReturn an independent parity_assessment for this domain plus concrete findings (each with file:line, cutover-vs-legacy diff, regression judgment).`,
    { label: `review:${job.domain.key}:${job.lens.key}`, phase: 'Review', schema: FINDINGS_SCHEMA }
  ).then(r => ({ domain: job.domain.key, lens: job.lens.key, parity_assessment: r && r.parity_assessment, findings: (r && r.findings) || [] }))
))

const parityAssessments = reviewResults.filter(Boolean).map(r => ({ domain: r.domain, lens: r.lens, assessment: r.parity_assessment }))
const allFindings = reviewResults.filter(Boolean)
  .flatMap(r => r.findings.map(f => ({ ...f, domain: r.domain, lens: r.lens })))
  .map((f, i) => ({ ...f, id: `F${i + 1}` }))
log(`Phase A: ${allFindings.length} raw findings across ${reviewJobs.length} domain x lens reviewers`)

if (allFindings.length === 0) {
  return { verdict: 'clean', parityAssessments, confirmed: [], note: 'No findings surfaced by any clean-room lens.' }
}

// ===================== Phase B — adversarial verify (code-only) =====================
phase('Verify')
const verified = await parallel(allFindings.map(f => () =>
  parallel(Array.from({ length: verifyVotes }, (_, k) => () =>
    agent(
      `${CLEANROOM}\n\nADVERSARIAL VERIFY (skeptic #${k + 1}). Try to REFUTE this finding from code. Default refuted=true unless the code clearly proves a real defect or a real cutover-vs-legacy regression in the CURRENT implementation. Cite file:line.\nDOMAIN: ${f.domain}\nFINDING [${f.severity}/${f.category}] ${f.title}\nLocation: ${f.location}\n${f.description}\nClaimed cutover-vs-legacy: ${f.cutover_vs_legacy}\nWhy: ${f.why_it_matters}`,
      { label: `verify:${f.id}.${k + 1}`, phase: 'Verify', schema: VERDICT_SCHEMA }
    )
  )).then(votes => {
    const v = votes.filter(Boolean)
    const confirms = v.filter(x => !x.refuted).length
    return { ...f, confirms, votes: v.length, survives: confirms * 2 >= v.length && confirms >= 2 }
  })
))
const survivors = verified.filter(Boolean).filter(f => f.survives)
log(`Phase B: ${survivors.length}/${allFindings.length} findings survived (majority & >=2 confirm)`)

if (survivors.length === 0) {
  return {
    verdict: 'clean',
    parityAssessments,
    confirmed: [],
    allFindings: verified.filter(Boolean).map(f => ({ id: f.id, domain: f.domain, title: f.title, confirms: f.confirms })),
  }
}

// ===================== Phase C — cross-check vs quarantined history (priors UNLOCKED here only) =====================
phase('CrossCheck')
const QUARANTINE = `Now (and ONLY now) you MAY consult the development history to classify an already-independently-confirmed
finding. Repo root: ${REPO}. Relevant priors:
  - plan-doc/tasks/designs/P4-T06d-*-design.md, plan-doc/reviews/P4-T06d-*-review-rounds.md
  - plan-doc/tasks/designs/P4-cutover-fix-design.md, plan-doc/reviews/P4-cutover-review-findings.md
  - plan-doc/tasks/designs/P4-T05-T06-cutover-design.md, P4-T06c-fe-dispatch-wiring-design.md, P4-batchD-maxcompute-removal-design.md
  - plan-doc/decisions-log.md, plan-doc/deviations-log.md, plan-doc/task-list.md
Classify the finding:
  - new-gap: a genuine defect/divergence NOT addressed in code and NOT registered anywhere (development missed it).
  - known-degradation: explicitly registered as a known/accepted deviation or non-goal.
  - already-handled: the code already handles it correctly (the finding is mistaken).
  - disagreement-with-history: the history claims this is fixed/correct/non-issue, but the code says otherwise (SURFACE loudly).
  - false-positive: not actually true.`
const crossed = await parallel(survivors.map(f => () =>
  agent(
    `${QUARANTINE}\n\nFINDING [${f.severity}/${f.category}] (domain: ${f.domain}, confirms ${f.confirms}/${f.votes})\n${f.title}\nLocation: ${f.location}\n${f.description}\nCutover-vs-legacy: ${f.cutover_vs_legacy}  | regression: ${f.regression}`,
    { label: `crosscheck:${f.id}`, phase: 'CrossCheck', schema: CROSSCHECK_SCHEMA }
  ).then(c => ({ ...f, crosscheck: c }))
))

const confirmed = crossed.filter(Boolean)
const newGaps = confirmed.filter(f => f.crosscheck && f.crosscheck.status === 'new-gap')
const disagreements = confirmed.filter(f => f.crosscheck && f.crosscheck.status === 'disagreement-with-history')

return {
  verdict: (newGaps.length === 0 && disagreements.length === 0) ? 'no-new-gaps' : 'attention-needed',
  stats: {
    domains: DOMAINS.length, reviewers: reviewJobs.length, verifyVotes,
    rawFindings: allFindings.length, survived: survivors.length,
    newGaps: newGaps.length, disagreements: disagreements.length,
  },
  parityAssessments,
  newGaps: newGaps.map(f => ({ id: f.id, domain: f.domain, severity: f.severity, title: f.title, location: f.location, description: f.description, cutover_vs_legacy: f.cutover_vs_legacy, regression: f.regression, action: f.crosscheck.recommended_action })),
  disagreements: disagreements.map(f => ({ id: f.id, domain: f.domain, severity: f.severity, title: f.title, location: f.location, description: f.description, evidence: f.crosscheck.evidence, action: f.crosscheck.recommended_action })),
  confirmed: confirmed.map(f => ({ id: f.id, domain: f.domain, severity: f.severity, category: f.category, title: f.title, location: f.location, regression: f.regression, status: f.crosscheck && f.crosscheck.status, confirms: f.confirms })),
}
