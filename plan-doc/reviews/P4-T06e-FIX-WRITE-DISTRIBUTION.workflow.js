// Clean-room adversarial review of the FIX-WRITE-DISTRIBUTION change (P4-T06e, P0-2 / NG-2 / NG-4).
//
// HOW TO RUN:
//   Workflow({ scriptPath: "plan-doc/reviews/P4-T06e-FIX-WRITE-DISTRIBUTION.workflow.js" })
//   optional: args: { verifyVotes: 3, lenses: 2 }
//
// DISCIPLINE (clean-room, per HANDOFF):
//   - Phase A (Review) + Phase B (Verify) are CODE-ONLY. Prompts carry ONLY source pointers and the
//     "cutover vs legacy" framing. Reviewers must NOT read plan-doc/ (design/review/decisions/HANDOFF)
//     and must NOT assume "it was fixed / mutation-proven". The change is treated as unaudited.
//   - Phase C (CrossCheck) is the ONLY phase that may read the design doc + history, and only to
//     classify already-independently-confirmed findings (matches-design-intent / contradicts-history /
//     test-vacuous-risk / batch-D red-line).
//
// Returns structured data; the orchestrator writes the round into
//   plan-doc/reviews/P4-T06e-FIX-WRITE-DISTRIBUTION-review-rounds.md

export const meta = {
  name: 'fix-write-distribution-review',
  description: 'Clean-room adversarial review of FIX-WRITE-DISTRIBUTION (connector sink distribution + local-sort)',
  phases: [
    { title: 'Review', detail: 'parity + delivery clean-room lenses over the change (code-only)' },
    { title: 'Verify', detail: 'refute-by-default skeptics per finding (code-only)' },
    { title: 'CrossCheck', detail: 'classify survivors vs design/history (Phase C only)' },
  ],
}

const REPO = '/mnt/disk1/yy/git/wt-catalog-spi'
const verifyVotes = (args && args.verifyVotes) || 3
const lensCount = (args && args.lenses) || 2

const CLEANROOM = `You are a CLEAN-ROOM code reviewer. Repo root: ${REPO}.
CONTEXT: MaxCompute was migrated to a connector-SPI. After the cutover a max_compute catalog is a
PluginDrivenExternalCatalog and its tables are TableType.PLUGIN_EXTERNAL_TABLE, so a MaxCompute write
flows through the GENERIC nereids sink PhysicalConnectorTableSink instead of the legacy
PhysicalMaxComputeTableSink. A change ("FIX-WRITE-DISTRIBUTION") just modified the generic sink's
required-physical-properties (write distribution + sort) and added connector capabilities so MaxCompute
writes get the right distribution. Your job: judge this change INDEPENDENTLY from code, comparing the
CUTOVER behavior to the LEGACY behavior.

THE CHANGE (read these):
  - fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/physical/PhysicalConnectorTableSink.java
      -> getRequirePhysicalProperties() (the new 3-branch logic)
  - fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalTable.java
      -> requirePartitionLocalSortOnWrite() and supportsParallelWrite()
  - fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorCapability.java
      -> SINK_REQUIRE_PARTITION_LOCAL_SORT
  - fe/fe-connector/fe-connector-maxcompute/src/main/java/org/apache/doris/connector/maxcompute/MaxComputeDorisConnector.java
      -> getCapabilities()

LEGACY BASELINE to compare against:
  - fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/physical/PhysicalMaxComputeTableSink.java
      -> getRequirePhysicalProperties():111-155 (the 3-branch this generalizes)

DOWNSTREAM CONSUMERS of getRequirePhysicalProperties() (verify the change composes correctly):
  - fe/fe-core/src/main/java/org/apache/doris/nereids/properties/RequestPropertyDeriver.java
      -> visitPhysicalConnectorTableSink():212-227 vs visitPhysicalMaxComputeTableSink():180-188
  - fe/fe-core/src/main/java/org/apache/doris/nereids/processor/post/ShuffleKeyPruner.java
      -> visitPhysicalConnectorTableSink() vs visitPhysicalMaxComputeTableSink()
  - bind-time child-output alignment: fe/fe-core/.../nereids/rules/analysis/BindSink.java
      -> bindConnectorTableSink() (projects child to cols order) vs bindMaxComputeTableSink() (projects to full schema)
  - SessionVariable.enableStrictConsistencyDml default

STRICT DISCIPLINE:
  - Read ONLY source code (fe/, be/, gensrc/). Use git/grep/file reads.
  - DO NOT read anything under plan-doc/ and do NOT rely on remembered project conclusions.
  - Make NO assumption that the change "is correct" or "was tested". Treat it as unaudited.
  - Every finding MUST cite file:line and state the concrete CUTOVER vs LEGACY behavioral difference and
    whether it is a regression (yes/no/unsure). "The code intends X" is not evidence — verify X holds.
  - Zero findings is a valid result if the change faithfully generalizes legacy and composes correctly.`

const LENSES = [
  { key: 'parity', focus: `LEGACY-PARITY & CORRECTNESS. Does the new generic sink reproduce, for a MaxCompute table, the
EXACT distribution/sort legacy PhysicalMaxComputeTableSink produced in all three cases (dynamic partition,
all-static partition, non-partitioned)? Pay special attention to the partition-column -> child-output INDEX
mapping: legacy indexes child().getOutput() by FULL-SCHEMA position (its child is projected to full schema);
the generic connector sink's child is projected to COLS order. Is the new code's indexing correct for the
generic sink (no wrong slot, no off-by-one, no IndexOutOfBounds)? Are the hash exprIds and local-sort order
keys the right slots? Does the enableStrictConsistencyDml interaction in RequestPropertyDeriver match legacy?` },
  { key: 'delivery', focus: `DELIVERY, EDGE CASES & BLAST RADIUS. Does declaring SUPPORTS_PARALLEL_WRITE +
SINK_REQUIRE_PARTITION_LOCAL_SORT for MaxCompute change anything beyond getRequirePhysicalProperties()? Find
ALL readers of these capabilities. Could the new branch fire for the wrong connector (jdbc/es/trino) or wrong
write shape? Edge cases: empty cols, partition col absent from cols, multi-level (mixed static+dynamic)
partitions, ShuffleKeyPruner divergence between the connector branch and the MC branch (is it a real
regression?), and interaction with the not-yet-fixed static-partition bind (NG-3). Cite file:line.` },
]

const FINDINGS_SCHEMA = {
  type: 'object', additionalProperties: false,
  properties: {
    assessment: { type: 'string', description: 'one-paragraph independent verdict: does the change reach legacy parity and compose correctly?' },
    findings: {
      type: 'array',
      items: {
        type: 'object', additionalProperties: false,
        properties: {
          title: { type: 'string' },
          severity: { type: 'string', enum: ['blocker', 'major', 'minor', 'nit'] },
          category: { type: 'string', enum: ['correctness', 'parity', 'regression', 'design-impl-gap', 'blast-radius', 'test-quality', 'other'] },
          location: { type: 'string', description: 'file:line' },
          description: { type: 'string' },
          cutover_vs_legacy: { type: 'string' },
          regression: { type: 'string', enum: ['yes', 'no', 'unsure'] },
          why_it_matters: { type: 'string' },
        },
        required: ['title', 'severity', 'category', 'location', 'description', 'cutover_vs_legacy', 'regression', 'why_it_matters'],
      },
    },
  },
  required: ['assessment', 'findings'],
}
const VERDICT_SCHEMA = {
  type: 'object', additionalProperties: false,
  properties: { refuted: { type: 'boolean' }, confidence: { type: 'string', enum: ['low', 'medium', 'high'] }, reasoning: { type: 'string' } },
  required: ['refuted', 'confidence', 'reasoning'],
}
const CROSSCHECK_SCHEMA = {
  type: 'object', additionalProperties: false,
  properties: {
    status: { type: 'string', enum: ['new-gap', 'known-degradation', 'already-handled', 'disagreement-with-design', 'false-positive'] },
    matchesDesignIntent: { type: 'boolean' },
    evidence: { type: 'string', description: 'cite the design doc section/commit and/or code' },
    recommended_action: { type: 'string' },
  },
  required: ['status', 'matchesDesignIntent', 'evidence', 'recommended_action'],
}

// ===================== Phase A — clean-room review =====================
phase('Review')
const lenses = LENSES.slice(0, Math.max(1, Math.min(LENSES.length, lensCount)))
const reviewResults = await parallel(lenses.map(lens => () =>
  agent(
    `${CLEANROOM}\n\nLENS: ${lens.focus}\n\nReturn an independent assessment of the change plus concrete findings (each with file:line, cutover-vs-legacy diff, regression judgment).`,
    { label: `review:${lens.key}`, phase: 'Review', schema: FINDINGS_SCHEMA }
  ).then(r => ({ lens: lens.key, assessment: r && r.assessment, findings: (r && r.findings) || [] }))
))

const assessments = reviewResults.filter(Boolean).map(r => ({ lens: r.lens, assessment: r.assessment }))
const allFindings = reviewResults.filter(Boolean)
  .flatMap(r => r.findings.map(f => ({ ...f, lens: r.lens })))
  .map((f, i) => ({ ...f, id: `F${i + 1}` }))
log(`Phase A: ${allFindings.length} raw findings across ${lenses.length} lenses`)

if (allFindings.length === 0) {
  return { verdict: 'clean', assessments, survivors: [], note: 'No findings surfaced by any clean-room lens.' }
}

// ===================== Phase B — adversarial verify =====================
phase('Verify')
const verified = await parallel(allFindings.map(f => () =>
  parallel(Array.from({ length: verifyVotes }, (_, k) => () =>
    agent(
      `${CLEANROOM}\n\nADVERSARIAL VERIFY (skeptic #${k + 1}). Try to REFUTE this finding from code. Default refuted=true unless the code clearly proves a real defect or a real cutover-vs-legacy regression in the CURRENT change. Cite file:line.\nFINDING [${f.severity}/${f.category}] ${f.title}\nLocation: ${f.location}\n${f.description}\nClaimed cutover-vs-legacy: ${f.cutover_vs_legacy}\nWhy: ${f.why_it_matters}`,
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
    assessments,
    survivors: [],
    allFindings: verified.filter(Boolean).map(f => ({ id: f.id, lens: f.lens, title: f.title, confirms: f.confirms, votes: f.votes })),
  }
}

// ===================== Phase C — cross-check vs design/history =====================
phase('CrossCheck')
const QUARANTINE = `Now (and ONLY now) you MAY consult the design + history to classify an already-confirmed
finding. Repo root: ${REPO}. Relevant:
  - plan-doc/tasks/designs/P4-T06e-FIX-WRITE-DISTRIBUTION-design.md (the design for this change)
  - plan-doc/reviews/P4-maxcompute-full-rereview-2026-06-07.md (§A.NG-2/NG-4 the source findings)
  - plan-doc/tasks/designs/P4-batchD-maxcompute-removal-design.md (Batch-D red-line: deleting PhysicalMaxComputeTableSink)
  - plan-doc/decisions-log.md, plan-doc/deviations-log.md
Classify:
  - new-gap: a genuine defect/divergence the change introduced or left, NOT covered by the design.
  - known-degradation: the design explicitly registers it as accepted/known (e.g. ShuffleKeyPruner non-strict
    divergence, enable_strict_consistency_dml=false parity, P0-3 coupling).
  - already-handled: the code already handles it; the finding is mistaken.
  - disagreement-with-design: the design CLAIMS something the code does not do (surface loudly).
  - false-positive: not actually true.
Also judge matchesDesignIntent (does the change match its own design doc?) and whether the Batch-D red-line
(delete PhysicalMaxComputeTableSink only after this lands) is satisfied.`
const crossed = await parallel(survivors.map(f => () =>
  agent(
    `${QUARANTINE}\n\nFINDING [${f.severity}/${f.category}] (confirms ${f.confirms}/${f.votes})\n${f.title}\nLocation: ${f.location}\n${f.description}\nCutover-vs-legacy: ${f.cutover_vs_legacy} | regression: ${f.regression}`,
    { label: `crosscheck:${f.id}`, phase: 'CrossCheck', schema: CROSSCHECK_SCHEMA }
  ).then(c => ({ ...f, crosscheck: c }))
))

const confirmed = crossed.filter(Boolean)
const newGaps = confirmed.filter(f => f.crosscheck && f.crosscheck.status === 'new-gap')
const disagreements = confirmed.filter(f => f.crosscheck && f.crosscheck.status === 'disagreement-with-design')
const mustFix = confirmed.filter(f => f.crosscheck
  && (f.crosscheck.status === 'new-gap' || f.crosscheck.status === 'disagreement-with-design'))

return {
  verdict: mustFix.length === 0 ? 'converged-or-known' : 'needs-revision',
  stats: { lenses: lenses.length, verifyVotes, rawFindings: allFindings.length, survived: survivors.length, newGaps: newGaps.length, disagreements: disagreements.length },
  assessments,
  mustFix: mustFix.map(f => ({ id: f.id, severity: f.severity, category: f.category, title: f.title, location: f.location, description: f.description, cutover_vs_legacy: f.cutover_vs_legacy, status: f.crosscheck.status, matchesDesignIntent: f.crosscheck.matchesDesignIntent, action: f.crosscheck.recommended_action })),
  knownOrHandled: confirmed.filter(f => !mustFix.includes(f)).map(f => ({ id: f.id, severity: f.severity, title: f.title, location: f.location, status: f.crosscheck && f.crosscheck.status, action: f.crosscheck && f.crosscheck.recommended_action })),
}
