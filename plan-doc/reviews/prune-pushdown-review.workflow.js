export const meta = {
  name: 'prune-pushdown-review',
  description: 'Clean-room adversarial review of FIX-PRUNE-PUSHDOWN (P4-T06e/DG-1) diff: parity, blast-radius, correctness, test-quality',
  phases: [
    { title: 'Review', detail: 'independent reviewers, each a distinct lens, over the diff' },
    { title: 'Verify', detail: 'adversarially verify each surfaced finding before reporting' },
  ],
}

const REPO = '/mnt/disk1/yy/git/wt-catalog-spi'

// The diff under review (files changed by FIX-PRUNE-PUSHDOWN):
const FILES = [
  'fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/scan/ConnectorScanPlanProvider.java',
  'fe/fe-connector/fe-connector-maxcompute/src/main/java/org/apache/doris/connector/maxcompute/MaxComputeScanPlanProvider.java',
  'fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java',
  'fe/fe-core/src/main/java/org/apache/doris/nereids/glue/translator/PhysicalPlanTranslator.java',
  'fe/fe-core/src/test/java/org/apache/doris/datasource/PluginDrivenScanNodePartitionPruningTest.java',
  'fe/fe-connector/fe-connector-maxcompute/src/test/java/org/apache/doris/connector/maxcompute/MaxComputeScanPlanProviderTest.java',
]

const CONTEXT = `FIX-PRUNE-PUSHDOWN (P4-T06e / DG-1). Background: in the plugin-driven MaxCompute read path the Nereids partition-pruning result (SelectedPartitions) was computed but dropped at the translator, so the ODPS read session was built over ALL partitions (perf/memory regression; rows still correct). The fix threads it through an additive 6-arg planScan SPI overload.

Design doc: ${REPO}/plan-doc/tasks/designs/P4-T06e-FIX-PRUNE-PUSHDOWN-design.md
Legacy parity reference: ${REPO}/fe/fe-core/src/main/java/org/apache/doris/datasource/maxcompute/source/MaxComputeScanNode.java (getSplits():~700-754, three-state requiredPartitionSpecs; startSplit():~236-250).
Inspect the actual diff with: git -C ${REPO} diff HEAD -- <file>  (and read full files for context).`

const FINDINGS_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['findings'],
  properties: {
    findings: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        required: ['title', 'severity', 'category', 'fileLine', 'detail', 'suggestedFix'],
        properties: {
          title: { type: 'string' },
          severity: { type: 'string', enum: ['blocker', 'major', 'minor', 'nit'] },
          category: { type: 'string', enum: ['parity', 'correctness', 'blast-radius', 'test-quality', 'style', 'doc'] },
          fileLine: { type: 'string' },
          detail: { type: 'string', description: 'what is wrong and why it matters' },
          suggestedFix: { type: 'string' },
        },
      },
    },
  },
}

const VERDICT_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  required: ['title', 'isReal', 'mustFix', 'reasoning', 'evidence'],
  properties: {
    title: { type: 'string' },
    isReal: { type: 'boolean', description: 'true if the finding is a genuine defect after independent code re-check' },
    mustFix: { type: 'boolean', description: 'true if it must be fixed before commit (blocker/major real defect)' },
    reasoning: { type: 'string' },
    evidence: { type: 'array', items: { type: 'string', description: 'file:line' } },
  },
}

phase('Review')

const LENSES = [
  { key: 'parity', prompt: `You are a SKEPTICAL reviewer. Lens: LEGACY PARITY. Does the fix faithfully mirror legacy MaxComputeScanNode partition pushdown? Check: (1) three-state mapping (NOT_PRUNED/not-pruned -> scan all; pruned non-empty -> subset; pruned empty -> short-circuit no splits) vs legacy getSplits():718-731; (2) name->PartitionSpec conversion matches legacy new PartitionSpec(key); (3) BOTH read-session paths (standard + limit-opt) receive requiredPartitions, matching legacy getSplits + getSplitsWithLimitOptimization; (4) the limit-opt eligibility / behavior is unchanged. Report any divergence from legacy semantics.` },
  { key: 'correctness', prompt: `You are a SKEPTICAL reviewer. Lens: CORRECTNESS. Could the change drop or duplicate rows, NPE, or mis-handle edge cases? Check: (1) null vs empty-list semantics of requiredPartitions end-to-end (resolveRequiredPartitions -> getSplits short-circuit -> planScan -> toPartitionSpecs); (2) the short-circuit returns no splits ONLY when genuinely pruned-to-zero, never when not-pruned; (3) SelectedPartitions.isPruned is the right gate (vs legacy != NOT_PRUNED); (4) default field value NOT_PRUNED keeps non-MaxCompute / non-pruned behavior identical; (5) thread-safety / shared-state concerns. Report real correctness risks.` },
  { key: 'blast-radius', prompt: `You are a SKEPTICAL reviewer. Lens: BLAST RADIUS / SPI. The fix adds a 6-arg planScan default-method overload. Verify: (1) the other 6 connector providers (es/jdbc/hive/paimon/hudi/trino) are genuinely unaffected (inherit the default that delegates to their existing planScan); (2) no existing caller of the 4/5-arg planScan breaks; (3) the new default method correctly delegates; (4) the MaxCompute 5-arg now delegates to 6-arg(null) without behavior change for existing callers (e.g. passthrough/TVF); (5) the SPI javadoc contract is accurate. Also: is the Hudi-SPI plugin branch (visitPhysicalHudiScan) being left unwired a real gap or acceptable scope? Report issues.` },
  { key: 'test-quality', prompt: `You are a SKEPTICAL reviewer. Lens: TEST QUALITY (Rule 9: tests must fail when business logic changes). For PluginDrivenScanNodePartitionPruningTest and MaxComputeScanPlanProviderTest: (1) would each test actually go RED if the pruning logic were reverted/mutated (e.g. resolveRequiredPartitions always returns null, or toPartitionSpecs always returns empty)? (2) are the null-vs-empty distinctions actually asserted? (3) is anything important UNtested (the getSplits short-circuit branch, the translator wiring, the limit-opt path threading)? (4) any vacuous assertions? Report weak/missing coverage and whether the untested seams are acceptable (documented as live-e2e gate) or a gap.` },
]

const reviews = await pipeline(
  LENSES,
  l => agent(`Clean-room review in ${REPO}.\n${CONTEXT}\n\n${l.prompt}\n\nFiles in the diff:\n${FILES.map(f => '- ' + f).join('\n')}\n\nRead the ACTUAL code (and git diff). Return only genuine findings; empty list is fine if the lens is clean.`,
    { label: `review:${l.key}`, phase: 'Review', schema: FINDINGS_SCHEMA, agentType: 'Explore' }),
  (review, l) => parallel((review.findings || []).map(f => () =>
    agent(`Clean-room adversarial verification in ${REPO}.\n${CONTEXT}\n\nA reviewer (${l.key} lens) raised this finding. Independently re-check the code and decide if it is REAL and MUST-FIX. Be adversarial toward the finding — try to show it is wrong/non-issue. Default mustFix=false unless it is a genuine blocker/major defect.\n\nFINDING: ${f.title}\nSEVERITY(claimed): ${f.severity}\nCATEGORY: ${f.category}\nLOCATION: ${f.fileLine}\nDETAIL: ${f.detail}\nSUGGESTED FIX: ${f.suggestedFix}\n\nRead the actual code and return your verdict with file:line evidence.`,
      { label: `verify:${(f.fileLine || l.key).slice(0, 40)}`, phase: 'Verify', schema: VERDICT_SCHEMA })
      .then(v => ({ lens: l.key, claimedSeverity: f.severity, category: f.category, fileLine: f.fileLine, ...v }))
  ))
)

const allVerdicts = reviews.flat().filter(Boolean)
return {
  total: allVerdicts.length,
  real: allVerdicts.filter(v => v.isReal),
  mustFix: allVerdicts.filter(v => v.mustFix),
  allVerdicts,
}
