export const meta = {
  name: 'p5-paimon-fullpath-cleanroom-review',
  description: 'Clean-room adversarial review of all paimon connector functional paths vs legacy',
  phases: [
    { title: 'Review', detail: '13 fresh reviewers, one per functional path, neutral prompts only' },
    { title: 'Verify', detail: '3 adversarial refuters per BLOCKER/MAJOR finding (3 lenses)' },
    { title: 'Completeness', detail: 'critic identifies uncovered paths/aspects' },
    { title: 'Supplemental', detail: 'targeted review of each gap the critic flags' },
    { title: 'Synthesis', detail: 'faithful markdown report from all findings' },
  ],
}

// ----------------------------------------------------------------------------
// Clean-room guardrails — injected into every reviewer/refuter prompt.
// NO decision logs, NO prior review conclusions, NO memory content is ever
// forwarded. Reviewers judge purely from firsthand source code.
// ----------------------------------------------------------------------------
const BASELINE = '1872ea05310'

const CLEANROOM = [
  'You are an independent, adversarial code reviewer performing a CLEAN-ROOM review.',
  'Judge ONLY from source code you read firsthand in this repository.',
  'Do NOT read, search, or rely on any of: files under plan-doc/, decision logs, prior review files (plan-doc/reviews/*), task docs, .claude/ memory files, or commit messages. Ignore code comments that merely assert intent — verify behavior from the code itself.',
  'Form your own conclusions purely by tracing the actual code. Do NOT assume prior reviews were correct or incorrect; there is no trusted prior verdict.',
].join('\n')

const ARCH = [
  'Architecture context (factual, not a conclusion):',
  '- This repo is migrating the Apache Doris "paimon" external-catalog integration from a LEGACY design (all logic in fe-core under datasource/paimon/) to a NEW design with two parts:',
  '  (a) a standalone connector module fe/fe-connector/fe-connector-paimon/ implementing a connector SPI defined in fe/fe-connector/fe-connector-api/. This module MUST NOT import fe-core classes.',
  '  (b) a generic bridge layer in fe-core: fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDriven*.java, adapting Doris catalog/scan/MTMV/MVCC framework to the connector SPI.',
  '- The legacy paimon code still exists in the working tree (not yet deleted), so you can read both sides directly.',
].join('\n')

const LEGACY_NOTE = [
  'Accessing legacy behavior:',
  '- Paimon-specific legacy files under fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/ are present in the working tree — read them directly.',
  '- Some SHARED/dispatch fe-core files were modified by this migration (e.g. Env.java, CatalogFactory.java, ShowPartitionsCommand.java, GsonUtils.java, UserAuthentication.java, PhysicalPlanTranslator.java, CreateTableInfo.java). To see their PRE-migration behavior run: `git show ' + BASELINE + ':<path>` (' + BASELINE + ' is the legacy baseline commit; its production code == pre-migration).',
].join('\n')

const TASK = [
  'Your task:',
  '1. Trace the NEW implementation\'s complete flow for this functional path (entry point -> end), reading the real code.',
  '2. Trace the LEGACY implementation\'s complete flow for the same path.',
  '3. Compare precisely: behavior, semantics, boundary/edge cases, error surfaces (exception types/messages), default values, ordering, time-zone handling, null handling, pushdown correctness, etc.',
  '4. For every new-vs-legacy difference decide: intentional/benign reduction, or unintentional regression / bug?',
  '',
  'Report ONLY real problems demonstrable from actual code: design flaws, implementation bugs, or parity regressions vs legacy. For each finding give: title; severity; newLocation (file:line); legacyLocation (file:line or "N/A"); difference (concrete new-vs-legacy behavioral difference); failureScenario (concrete trigger: input/query -> observed wrong output or exception); suggestion (concrete fix direction).',
  '',
  'If an area is correct/clean, say so in coverageSummary and cleanAreas. Do NOT invent problems — only report what real code demonstrates. Prefer a few solid findings over many speculative ones.',
  '',
  'Discipline: review-only. Do NOT modify any production code. Do NOT run git checkout/restore/stash/reset. Do NOT commit.',
].join('\n')

const SEVERITY = [
  'Severity rubric:',
  '- BLOCKER: wrong query results, data loss / missing rows, crash on a reachable path, or credential/security leak.',
  '- MAJOR: a real correctness bug on some inputs, or a significant parity regression vs legacy.',
  '- MINOR: limited-impact bug or behavioral divergence unlikely to cause wrong results.',
  '- NIT: cosmetic / style only.',
].join('\n')

const LENSES = [
  { name: 'new-code-correctness', instruction: 'Focus on the NEW code path. Does it actually behave the way the finding claims (does the claimed wrong behavior really happen)? Trace the new code precisely, including guards, callers, and preconditions.' },
  { name: 'legacy-parity', instruction: 'Focus on the LEGACY code path. Does legacy actually behave differently from the new code as claimed? Trace legacy precisely and confirm the divergence is real (or whether legacy did the same thing).' },
  { name: 'reproducibility', instruction: 'Focus on end-to-end reachability. Can the claimed failure scenario actually be triggered through real entry points? Check whether guards, validation, config defaults, or callers prevent it (which would make it non-reproducible).' },
]

function ptr(arr) { return arr.map(p => '  - ' + p).join('\n') }

function reviewPrompt(spec) {
  return [
    CLEANROOM, '', ARCH, '',
    '# Review unit: ' + spec.name, '',
    '## Functional path to review', spec.description, '',
    '## NEW implementation — starting file pointers (navigation only; explore further as needed)',
    ptr(spec.newPointers), '',
    '## LEGACY implementation — starting file pointers',
    ptr(spec.legacyPointers),
    spec.extra ? ('\n## Additional navigation hints\n' + spec.extra) : '',
    '', LEGACY_NOTE, '', TASK, '', SEVERITY,
    '', 'Set pathName in your output to: ' + spec.name,
  ].join('\n')
}

function refutePrompt(f, contextName, lens) {
  return [
    CLEANROOM, '', ARCH, '',
    '# Adversarial verification',
    'An independent reviewer raised the following finding about the paimon connector migration (functional area: ' + contextName + '). Verify it FIRSTHAND from the actual code and decide whether it is a REAL defect.',
    '',
    '## Finding under review',
    '- title: ' + f.title,
    '- severity (claimed): ' + f.severity,
    '- newLocation: ' + f.newLocation,
    '- legacyLocation: ' + (f.legacyLocation || 'N/A'),
    '- difference (claimed): ' + f.difference,
    '- failureScenario (claimed): ' + f.failureScenario,
    '',
    '## Your verification lens: ' + lens.name,
    lens.instruction,
    '',
    LEGACY_NOTE,
    '',
    'Read the real code at the cited (and surrounding) locations and trace actual behavior. Do NOT trust the finding\'s claims — independently confirm or refute each. If you cannot demonstrate the defect firsthand from the code, answer REFUTED. If partially right (real divergence but wrong severity/impact, or only under conditions that do not hold), answer PARTIAL and explain.',
    '',
    'Output: verdict (CONFIRMED = real defect as described; REFUTED = not a real defect / claim wrong; PARTIAL = partly real), reasoning, evidence (firsthand file:line references).',
    '',
    'Discipline: review-only; do NOT modify code, do NOT run git checkout/restore/stash/reset, do NOT commit.',
  ].join('\n')
}

function completenessPrompt(digest) {
  return [
    CLEANROOM, '', ARCH, '',
    '# Completeness critic',
    'A clean-room review covered the paimon connector migration across functional paths. Below is a digest of what each reviewer reported it covered, plus the titles of findings raised.',
    '',
    '## Coverage digest',
    JSON.stringify(digest, null, 2),
    '',
    'Your job: identify GAPS — functional paths, code paths, inputs, edge cases, or new-vs-legacy comparisons NOT covered or covered shallowly, that could hide a real defect or parity regression. For each gap give: area; why (what defect could hide there); suggestedReview (concretely what to trace, with file pointers).',
    'Be specific and code-grounded. Do not restate already-covered findings. If coverage is genuinely complete, return an empty gaps list and say so in assessment.',
  ].join('\n')
}

function supplementPrompt(gap) {
  return [
    CLEANROOM, '', ARCH, '',
    '# Supplemental targeted review',
    'A completeness critic flagged this as an under-covered area in the paimon connector migration. Investigate it firsthand.',
    '',
    '## Area', gap.area,
    '## Why it matters', gap.why,
    '## What to trace', gap.suggestedReview,
    '',
    'New connector code: fe/fe-connector/fe-connector-paimon/ and fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDriven*.java. Legacy code: fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/.',
    LEGACY_NOTE, '', TASK, '', SEVERITY,
    '', 'Set pathName in your output to: ' + gap.area,
  ].join('\n')
}

function synthesisPrompt(payload) {
  return [
    '# Final report synthesis (faithful formatting task)',
    'You are given the complete structured output of a clean-room adversarial review of the paimon connector migration: per-path findings, adversarial verification verdicts for each BLOCKER/MAJOR finding (3 lenses each), a completeness critic result, and supplemental findings.',
    '',
    'Produce a faithful, well-organized Markdown report (Chinese section headers are fine; this team works in Chinese). Requirements:',
    '- Title: "P5 paimon 全功能路径 clean-room 对抗 review — findings (2026-06-11)".',
    '- Executive summary: counts of findings by severity; how many BLOCKER/MAJOR were CONFIRMED vs majority-REFUTED (downgraded) by the verify phase; the single highest-priority real defect.',
    '- A per-path section for each review unit AND each supplemental area: list EVERY finding with severity, new file:line, legacy file:line, difference, failureScenario, suggestion, and (for BLOCKER/MAJOR) the verify verdict summary "CONFIRMED x / REFUTED y / PARTIAL z" across the 3 lenses, with a clear **DOWNGRADED** tag when majority-refuted.',
    '- A consolidated "new <-> legacy 差异表" markdown table: path | difference | severity | verdict.',
    '- A "仍走旧逻辑 / fallback 清单" section built from the cross-cutting fallback-sweep path.',
    '- A "completeness / gaps" section summarizing the critic assessment.',
    'Be faithful: include ALL findings (do not drop or soften). Do not add new analysis or conclusions beyond what the data states. Clearly separate CONFIRMED real defects from DOWNGRADED (likely-not-real) ones.',
    '',
    '## Structured data (JSON)',
    JSON.stringify(payload, null, 2),
    '',
    'Output ONLY the Markdown report body, nothing else.',
  ].join('\n')
}

// ----------------------------------------------------------------------------
// Schemas
// ----------------------------------------------------------------------------
const FINDING_ITEM = {
  type: 'object',
  properties: {
    title: { type: 'string' },
    severity: { type: 'string', enum: ['BLOCKER', 'MAJOR', 'MINOR', 'NIT'] },
    newLocation: { type: 'string' },
    legacyLocation: { type: 'string' },
    difference: { type: 'string' },
    failureScenario: { type: 'string' },
    suggestion: { type: 'string' },
  },
  required: ['title', 'severity', 'newLocation', 'difference', 'failureScenario', 'suggestion'],
}
const REVIEW_SCHEMA = {
  type: 'object',
  properties: {
    pathName: { type: 'string' },
    coverageSummary: { type: 'string' },
    findings: { type: 'array', items: FINDING_ITEM },
    cleanAreas: { type: 'string' },
  },
  required: ['pathName', 'coverageSummary', 'findings', 'cleanAreas'],
}
const VERDICT_SCHEMA = {
  type: 'object',
  properties: {
    verdict: { type: 'string', enum: ['CONFIRMED', 'REFUTED', 'PARTIAL'] },
    reasoning: { type: 'string' },
    evidence: { type: 'string' },
  },
  required: ['verdict', 'reasoning', 'evidence'],
}
const COMPLETENESS_SCHEMA = {
  type: 'object',
  properties: {
    gaps: {
      type: 'array',
      items: {
        type: 'object',
        properties: {
          area: { type: 'string' },
          why: { type: 'string' },
          suggestedReview: { type: 'string' },
        },
        required: ['area', 'why', 'suggestedReview'],
      },
    },
    assessment: { type: 'string' },
  },
  required: ['gaps', 'assessment'],
}

// ----------------------------------------------------------------------------
// The 13 review units — neutral descriptions + bare file pointers only.
// ----------------------------------------------------------------------------
const PATHS = [
  {
    id: 'p01-normal-scan',
    name: '1. 基础读取 (normal scan)',
    description: 'Reading a normal paimon table: split generation, predicate/projection/limit/partition-prune pushdown, scan-node properties, JNI-vs-native execution selection, and row-count/value correctness.',
    newPointers: [
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java (planScan, getScanNodeProperties, getTableHandle, getTableSchema)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanPlanProvider.java (splits, ReadBuilder, predicate/projection/limit pushdown)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonPredicateConverter.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanRange.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonTableResolver.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonTableHandle.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenScanNode.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenExternalTable.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenSplit.java',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonScanNode.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonSource.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonSplit.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonValueConverter.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalTable.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonUtil.java',
    ],
    extra: 'Compare: split generation, predicate/projection/limit/partition-prune pushdown, scan-node properties, JNI-vs-native selection, row-count/value correctness. Note PluginDrivenScanNode getNodeExplainString and partition-count display vs the legacy FileScanNode behavior.',
  },
  {
    id: 'p02-incremental',
    name: '2. 批式增量读取 (@incr)',
    description: 'Batch incremental read via the @incr table-suffix syntax: incremental-between / -timestamp / -scan-mode keys, mutual exclusion with time-travel, parameter parsing/validation.',
    newPointers: [
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonIncrementalScanParams.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java (incremental keys / scan options)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonTableHandle.java (withScanOptions)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanPlanProvider.java',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalTable.java (validateIncrementalReadParams and incremental handling)',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonScanNode.java (incremental scan)',
    ],
    extra: 'Compare every supported incremental key, defaults, validation/error messages, and interaction with time-travel (AS OF).',
  },
  {
    id: 'p03-time-travel',
    name: '3. Time Travel (AS OF)',
    description: 'AS OF time-travel reads: TIMESTAMP vs VERSION, numeric-vs-tag resolution, time-zone handling of TIMESTAMP, schema-at-snapshot, and not-found error surfaces.',
    newPointers: [
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java (resolveTimeTravel / toTimeTravelSpec)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonCatalogOps.java (schemaAt / snapshot resolution)',
      'fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/mvcc/ConnectorTimeTravelSpec.java',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalTable.java (getSnapshotAt / getSnapshotById and time-travel)',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonSnapshot.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonMvccSnapshot.java',
    ],
    extra: 'TIME-ZONE is a key risk: confirm TIMESTAMP literal -> epoch conversion uses the same zone as legacy (a wrong zone yields the wrong snapshot = silently wrong rows). Compare numeric-vs-tag disambiguation and not-found error messages.',
  },
  {
    id: 'p04-branch-tag',
    name: '4. Branch / Tag 读取',
    description: 'Reading a specific branch or tag: branch-as-handle identity, tag pinning, and whether a branch carries its own schema/snapshot.',
    newPointers: [
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java (tag/branch resolution via Identifier)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonTableHandle.java (branchName)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonCatalogOps.java (branchExists / tagManager)',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalTable.java (branch/tag handling)',
    ],
    extra: 'Compare how a branch identifier is threaded into table loading and split planning, tag pinning to a snapshot id, and per-branch schema resolution.',
  },
  {
    id: 'p05-system-tables',
    name: '5. 系统表查询 ($snapshots/$schemas/$partitions...)',
    description: 'Querying paimon system tables ($snapshots, $schemas, $partitions, $files, binlog, audit_log, ...): enumeration/routing, JNI forcing for binlog/audit_log, auth unwrap, and TTableType selection for normal-table vs sys-table descriptors.',
    newPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenSysExternalTable.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java (isSystemTable / system-table loading / sys handle)',
      'search fe-core for: SysTableResolver, NativeSysTable, buildTableDescriptor',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonSysExternalTable.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalTable.java (system-table paths)',
    ],
    extra: 'Compare sys-table enumeration/routing, the JNI-force for binlog/audit_log, auth unwrap of the sys-table handle, and the TTableType chosen by buildTableDescriptor for normal tables vs sys tables.',
  },
  {
    id: 'p06-metadata-cache',
    name: '6. 元数据缓存',
    description: 'Metadata caching: schema / snapshot / table caches, whether REFRESH CATALOG / REFRESH TABLE reach the connector (i.e. invalidation is not silently dropped), cache-key correctness, and cache granularity/consistency.',
    newPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenSchemaCacheValue.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonCatalogOps.java',
      'search fe-core for: ExternalSchemaCache, ExternalMetaCacheMgr, and any invalidateTable / invalidate SPI hook',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalMetaCache.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonSchemaCacheKey.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonSchemaCacheValue.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonSnapshotCacheValue.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonTableCacheValue.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonMetadataOps.java',
    ],
    extra: 'Key risk: does REFRESH CATALOG/TABLE actually invalidate the connector-side caches, or is stale schema/snapshot served after a refresh? Compare cache-key composition and what gets cached at which layer.',
  },
  {
    id: 'p07-deletion-vector',
    name: '7. Deletion Vector 读取',
    description: 'Deletion-vector reads: whether DV is correctly enabled and applied on the new scan path, row-deletion semantics, and BE/JNI coordination.',
    newPointers: [
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanPlanProvider.java (ReadBuilder / scan options / DV enable/read — locate yourself)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonScanRange.java',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonScanNode.java (DV handling)',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/source/PaimonSplit.java',
    ],
    extra: 'Grep both new and legacy for "deletionVector" / "DeletionFile" / "dropDelete" / "withDeletion" etc. Confirm DV files are propagated to BE the same way (a dropped DV = wrong results: deleted rows reappear).',
  },
  {
    id: 'p08-metastore-flavors',
    name: '8. 多元数据服务接入 (HMS/DLF/REST/Filesystem/JDBC)',
    description: 'Per-metastore catalog creation across HMS / DLF / REST / Filesystem / JDBC: option assembly, authentication (Kerberos for HMS, DLF creds, REST vended credentials), and cross-classloader metastore-client use.',
    newPointers: [
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonCatalogFactory.java (buildCatalogOptions / validate)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorProperties.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/AbstractPaimonProperties.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/PaimonHMSMetaStoreProperties.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/PaimonAliyunDLFMetaStoreProperties.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/PaimonRestMetaStoreProperties.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/PaimonFileSystemMetaStoreProperties.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/PaimonJdbcMetaStoreProperties.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/PaimonPropertiesFactory.java',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalCatalogFactory.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalCatalog.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonHMSExternalCatalog.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonDLFExternalCatalog.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonRestExternalCatalog.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonFileExternalCatalog.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonVendedCredentialsProvider.java',
    ],
    extra: 'For EACH flavor compare catalog-option assembly and the auth path. REST vended-credentials and DLF/HMS Kerberos are high-risk for silent breakage. Note that the property/metastore/Paimon* classes are still live in fe-core — confirm who calls them in the new path.',
  },
  {
    id: 'p09-storage-systems',
    name: '9. 多存储系统接入 (S3/OSS/HDFS...)',
    description: 'Storage backends (S3 / OSS / HDFS / ...): how storage options/credentials are propagated into the paimon Configuration/FileIO, and per-cloud differences.',
    newPointers: [
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorProperties.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonCatalogFactory.java (Configuration / catalog options / FileIO)',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalCatalog.java (fs / storage configuration)',
    ],
    extra: 'Compare storage-option propagation and credential flow per cloud. The connector module cannot import fe-core StorageProperties — check how the Configuration is rebuilt and whether any option is dropped vs legacy.',
  },
  {
    id: 'p10-type-mapping',
    name: '10. 列类型映射',
    description: 'Column type mapping in both directions: paimon type -> Doris type (read) and Doris type -> paimon type (DDL). Nullability, precision/scale, and complex types (array/map/row/struct), byte-for-byte parity.',
    newPointers: [
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonTypeMapping.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java (mapFields)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonSchemaBuilder.java (DDL toPaimonType)',
      'search fe-core for: ConnectorColumnConverter (connector column -> Doris Column)',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonUtil.java (type mapping)',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/DorisToPaimonTypeVisitor.java',
    ],
    extra: 'Go type-by-type. Check nullable propagation, decimal precision/scale, timestamp/time precision, char/varchar length, and nested complex types. Any mismatch can mean wrong values or DDL that creates a wrong schema.',
  },
  {
    id: 'p11-mtmv',
    name: '11. mtmv',
    description: 'MTMV (materialized view) base-table support: partition tracking, incremental-refresh snapshot feed, and partition-spec exposure.',
    newPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenMvccExternalTable.java (MTMVRelatedTableIf / MTMVBaseTableIf)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java (partition / snapshot feed)',
      'fe/fe-connector/fe-connector-api/src/main/java/org/apache/doris/connector/api/ConnectorPartitionInfo.java',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalTable.java (MTMV methods)',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonPartitionInfo.java',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonPartition.java',
    ],
    extra: 'Compare MTMV base-table refresh: partition tracking, how snapshot is fed for incremental refresh, and partition-spec/partition-name rendering (a wrong partition name = missing/duplicated refresh rows).',
  },
  {
    id: 'p12-mvcc',
    name: '12. mvcc',
    description: 'MVCC snapshot isolation: query-begin snapshot pinning, consistency, and crucially whether the pinned snapshot actually reaches split planning.',
    newPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenMvccExternalTable.java (MvccTable)',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenMvccSnapshot.java',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonConnectorMetadata.java (beginQuerySnapshot / snapshot pin)',
      'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonTableHandle.java',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonExternalTable.java (MvccTable)',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/PaimonMvccSnapshot.java',
    ],
    extra: 'Key risk: trace the pinned query-begin snapshot all the way into planScan/split planning. If split planning re-resolves "latest" instead of using the pinned snapshot, concurrent writes cause inconsistent reads.',
  },
  {
    id: 'p13-fallback-sweep',
    name: '13. cross-cutting: 旧逻辑/fallback sweep',
    description: 'Whole-tree sweep for residual legacy paimon logic still reachable after the cutover (potential bugs) vs dead code (safe to delete later).',
    newPointers: [
      'grep the WHOLE fe/ tree for: "instanceof Paimon", "PAIMON_EXTERNAL_TABLE", "case PAIMON", "MetastoreProperties.Type.PAIMON", "PaimonExternalCatalog", "PaimonExternalTable"',
      'fe/fe-core/src/main/java/org/apache/doris/datasource/CatalogFactory.java',
      'fe/fe-core/src/main/java/org/apache/doris/catalog/Env.java',
      'fe/fe-core/src/main/java/org/apache/doris/nereids/rules/analysis/UserAuthentication.java',
      'fe/fe-core/src/main/java/org/apache/doris/nereids/trees/plans/commands/ShowPartitionsCommand.java',
    ],
    legacyPointers: [
      'fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/ (still-live Paimon* classes)',
      'use `git show ' + BASELINE + ':<path>` to compare pre-cutover dispatch',
    ],
    extra: 'For EACH match decide: (a) post-cutover DEAD code (the catalog/table is never the legacy type anymore -> safe to delete) or (b) STILL-REACHABLE legacy logic / fallback (a real path can still hit it -> potential bug). Pay special attention to scan / cache / auth / DDL dispatch and the property/metastore/Paimon* classes that remain live. Report any reachable fallback that would behave differently from the new connector.',
  },
]

// ----------------------------------------------------------------------------
// Verify helper: 3 adversarial refuters (3 lenses) per BLOCKER/MAJOR finding.
// ----------------------------------------------------------------------------
async function verifyFindings(review, contextName, idPrefix) {
  if (!review || !Array.isArray(review.findings)) return []
  const toVerify = review.findings.filter(f => f.severity === 'BLOCKER' || f.severity === 'MAJOR')
  if (!toVerify.length) return []
  const flat = await parallel(toVerify.flatMap((f, fi) =>
    LENSES.map(lens => () =>
      agent(refutePrompt(f, contextName, lens), { label: 'verify:' + idPrefix + ':f' + fi + ':' + lens.name, phase: 'Verify', schema: VERDICT_SCHEMA })
        .then(v => v ? { fi, lens: lens.name, verdict: v.verdict, reasoning: v.reasoning, evidence: v.evidence } : null)
    )
  ))
  const byFinding = {}
  for (const v of flat.filter(Boolean)) (byFinding[v.fi] = byFinding[v.fi] || []).push(v)
  return toVerify.map((f, fi) => {
    const vs = byFinding[fi] || []
    const refuted = vs.filter(v => v.verdict === 'REFUTED').length
    const confirmed = vs.filter(v => v.verdict === 'CONFIRMED').length
    const partial = vs.filter(v => v.verdict === 'PARTIAL').length
    return { finding: f, verdicts: vs, confirmedCount: confirmed, refutedCount: refuted, partialCount: partial, majorityRefuted: vs.length > 0 && refuted > vs.length / 2 }
  })
}

// ----------------------------------------------------------------------------
// Phase Review + Verify (pipeline: each path verifies as soon as it is reviewed)
// ----------------------------------------------------------------------------
phase('Review')
log('Clean-room review: ' + PATHS.length + ' functional paths, fresh subagent each, neutral prompts only.')
const reviewed = await pipeline(
  PATHS,
  (spec) => agent(reviewPrompt(spec), { label: 'review:' + spec.id, phase: 'Review', schema: REVIEW_SCHEMA }),
  async (review, spec) => ({
    id: spec.id,
    pathName: spec.name,
    review,
    verdicts: await verifyFindings(review, spec.name, spec.id),
  })
)

// ----------------------------------------------------------------------------
// Phase Completeness (critic over the coverage digest)
// ----------------------------------------------------------------------------
phase('Completeness')
const digest = reviewed.filter(Boolean).map(r => ({
  path: r.pathName,
  coverage: r.review ? r.review.coverageSummary : '(reviewer failed)',
  cleanAreas: r.review ? r.review.cleanAreas : '',
  findings: r.review && Array.isArray(r.review.findings) ? r.review.findings.map(f => '[' + f.severity + '] ' + f.title) : [],
}))
const completeness = await agent(completenessPrompt(digest), { label: 'completeness-critic', phase: 'Completeness', schema: COMPLETENESS_SCHEMA })

// ----------------------------------------------------------------------------
// Phase Supplemental (targeted review of each gap, then verify)
// ----------------------------------------------------------------------------
phase('Supplemental')
const gaps = (completeness && Array.isArray(completeness.gaps)) ? completeness.gaps.slice(0, 8) : []
log('Completeness critic flagged ' + gaps.length + ' gap(s) for supplemental review.')
const supplemental = await pipeline(
  gaps,
  (gap) => agent(supplementPrompt(gap), { label: 'supp:' + gap.area.slice(0, 24), phase: 'Supplemental', schema: REVIEW_SCHEMA }),
  async (review, gap, i) => ({
    area: gap.area,
    review,
    verdicts: await verifyFindings(review, gap.area, 'supp' + i),
  })
)

// ----------------------------------------------------------------------------
// Phase Synthesis (faithful markdown report)
// ----------------------------------------------------------------------------
phase('Synthesis')
const payload = {
  reviewed: reviewed.filter(Boolean),
  completeness,
  supplemental: supplemental.filter(Boolean),
}
const markdown = await agent(synthesisPrompt(payload), { label: 'synthesis', phase: 'Synthesis' })

return { markdown, raw: payload }
