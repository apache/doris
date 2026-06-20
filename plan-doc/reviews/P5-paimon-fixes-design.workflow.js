export const meta = {
  name: 'p5-paimon-fixes-design',
  description: 'Design docs for the 8 paimon fullpath-review fixes, grounded in current code',
  phases: [{ title: 'Design', detail: 'one design subagent per fix, confirms root cause in current code + patch/test plan' }],
}

const REPORT = 'plan-doc/reviews/P5-paimon-fullpath-review-2026-06-11.md'
const CONNDIR = 'fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/'
const CONNTEST = 'fe/fe-connector/fe-connector-paimon/src/test/java/org/apache/doris/connector/paimon/'
const LEGACY = 'fe/fe-core/src/main/java/org/apache/doris/datasource/paimon/'

const COMMON = [
  'You are a DESIGN subagent. Produce an implementation design for ONE confirmed defect in the Apache Doris paimon connector.',
  '',
  'Context: a clean-room review already confirmed this defect. Your job is NOT to re-judge it but to design the fix, grounded in the CURRENT code (read it firsthand — line numbers in the report may have drifted).',
  'The confirmed findings live in: ' + REPORT + ' (read the relevant section).',
  'New connector code: ' + CONNDIR + '  (this module MUST NOT import fe-core — verify your design respects that).',
  'New connector tests: ' + CONNTEST + ' (harness: FakePaimonTable, RecordingPaimonCatalogOps, RecordingConnectorContext, PaimonCatalogFactoryTest, PaimonScanPlanProviderTest, etc.).',
  'Legacy reference (still in tree, port behavior from here for parity): ' + LEGACY,
  '',
  'Deliver a design doc (markdown) with EXACTLY these sections:',
  '# Problem',
  '# Root Cause (confirmed in current code, cite file:line you actually read)',
  '# Design (the fix approach; respect connector no-fe-core-import rule; match existing style; minimal change)',
  '# Implementation Plan (concrete: which files, which methods, what exact change — pseudocode/snippets ok)',
  '# Risk Analysis (parity vs legacy, shared-code blast radius, edge cases)',
  '# Test Plan (## Unit Tests — concrete new/extended UT in the connector test dir that FAIL before and PASS after, designed to verify INTENT not just behavior; ## E2E Tests — note if live-only/CI-skipped and why)',
  '',
  'Be concrete and correct. Read the actual current code AND the legacy reference before writing. Do NOT modify any code — design only.',
].join('\n')

const FIXES = [
  {
    id: 'FIX-STORAGE-CREDS',
    title: 'Storage credentials: canonical s3/oss keys dropped by applyStorageConfig; DLF gate passes but no OSS creds',
    detail: [
      'Read report path 9 (多存储系统接入) findings "s3/oss credentials dropped from Paimon FileIO" and "DLF gate ok but no OSS creds", and path 8 DLF.',
      'Current code: ' + CONNDIR + 'PaimonCatalogFactory.java applyStorageConfig (~:328) + the prefix allow-list (~:75) + DLF assembly. Trace what storage keys Doris users actually pass (canonical s3.access_key / s3.secret_key / s3.endpoint, oss.*) vs the paimon.s3./paimon.fs.oss. prefixes the connector recognizes.',
      'Legacy: ' + LEGACY + 'PaimonExternalCatalog (fs/storage config) — see how legacy propagated storage credentials into the catalog/FileIO Configuration.',
      'Design how to map the canonical Doris storage keys into the paimon Configuration for BOTH the s3/oss filesystem flavor and the DLF flavor, without importing fe-core StorageProperties.',
    ].join('\n'),
  },
  {
    id: 'FIX-REST-VENDED',
    title: 'REST vended credentials never delivered to BE (data files unreadable)',
    detail: [
      'Read report path 8 finding "REST vended credentials are never delivered to BE".',
      'Current code: ' + CONNDIR + 'PaimonCatalogFactory.java (REST flavor), PaimonConnectorMetadata.java (getScanNodeProperties / planScan — where per-scan storage properties are emitted to BE), PaimonScanPlanProvider.java, PaimonScanRange.java.',
      'Legacy: ' + LEGACY + 'PaimonVendedCredentialsProvider.java and source/PaimonScanNode.java (where vended creds are fetched per-snapshot and pushed to BE scan ranges).',
      'Design how the connector obtains paimon REST vended credentials and threads them into the scan-node/BE-facing storage properties, matching legacy timing/scope.',
    ].join('\n'),
  },
  {
    id: 'FIX-NATIVE-PARTVAL',
    title: 'Native partition-value rendering: port whole serializePartitionValue switch incl. session TZ',
    detail: [
      'Read report path 1 Finding 1.1 + the supplemental "Native-path partition-value rendering" findings (TIME, BINARY/VARBINARY, and the fix-scope finding).',
      'Current code: ' + CONNDIR + 'PaimonScanPlanProvider.java getPartitionInfoMap (~:383-400, the raw values[i].toString()). Also PaimonScanRange.populateRangeParams.',
      'Legacy to port: ' + LEGACY + 'PaimonUtil.java serializePartitionValue (~:566-627) and getPartitionInfoMap (~:545-629) — port the ENTIRE type switch: DATE (LocalDate.ofEpochDay), TIMESTAMP_WITHOUT_TZ, TIMESTAMP_WITH_LOCAL_TIME_ZONE (UTC->session TZ), TIME, FLOAT/DOUBLE; map key Locale.ROOT lowercase; unsupported types (binary) -> skip (omit from map) like legacy returns null.',
      'Determine where the session TimeZone is available to the connector at this point (ConnectorSession) and how legacy obtained it. Respect no-fe-core-import (cannot use fe-core TimeUtils — inline as needed).',
    ].join('\n'),
  },
  {
    id: 'FIX-CPP-READER',
    title: 'enable_paimon_cpp_reader ignored + Java-serialized split breaks BE paimon-cpp deserialize',
    detail: [
      'Read the supplemental finding "Connector ignores enable_paimon_cpp_reader and Java-serializes the split, breaking BE paimon-cpp deserialize".',
      'Current code: ' + CONNDIR + 'PaimonScanPlanProvider.java (split building / serialization), PaimonScanRange.java (populateRangeParams / what is sent to BE), PaimonTableHandle.java.',
      'Legacy: ' + LEGACY + 'source/PaimonScanNode.java and source/PaimonSplit.java — find how legacy honored enable_paimon_cpp_reader (session var) and chose the split serialization format (java-serialized vs cpp/native) accordingly.',
      'Design how the connector reads the enable_paimon_cpp_reader session flag and selects the matching split serialization so BE cpp reader can deserialize.',
    ].join('\n'),
  },
  {
    id: 'FIX-TZ-ALIAS',
    title: 'FOR TIME AS OF fails under CST(default)/PST/EST: inline 4-entry tz alias map',
    detail: [
      'Read report path 3 finding "FOR TIME AS OF datetime-string fails under session time_zone CST/PST/EST".',
      'Current code: ' + CONNDIR + 'PaimonConnectorMetadata.java parseTimestampMillis (~:538-547) where ZoneId.of(session.getTimeZone()) is called with no alias map.',
      'Legacy: fe/fe-core/src/main/java/org/apache/doris/common/util/TimeUtils.java timeZoneAliasMap (~:58-116) — it has exactly 4 entries (confirm them). Legacy resolves via ZoneId.of(tz, timeZoneAliasMap).',
      'Design a tiny inline alias constant in the connector (cannot import fe-core TimeUtils) that maps those 4 aliases before ZoneId.of, still failing loud on truly-unknown ids. Confirm the exact 4 entries from current TimeUtils.',
    ].join('\n'),
  },
  {
    id: 'FIX-HMS-CONFRES',
    title: 'HMS hive.conf.resources (external hive-site.xml) silently dropped at catalog creation',
    detail: [
      'Read report path 8 finding "HMS hive.conf.resources (external hive-site.xml) is silently dropped at catalog creation".',
      'Current code: ' + CONNDIR + 'PaimonCatalogFactory.java (HMS flavor assembly) and fe/fe-core/src/main/java/org/apache/doris/datasource/property/metastore/PaimonHMSMetaStoreProperties.java (still live).',
      'Legacy: ' + LEGACY + 'PaimonHMSExternalCatalog.java / PaimonExternalCatalog.java — how legacy loaded hive.conf.resources (paths to hive-site.xml etc.) into the HiveConf.',
      'Design how to honor hive.conf.resources in the new HMS catalog assembly. Note which side (connector vs fe-core property class) is the right place given the no-import rule.',
    ].join('\n'),
  },
  {
    id: 'FIX-TABLE-STATS',
    title: 'getTableStatistics not overridden -> base-table row count always -1',
    detail: [
      'Read the supplemental finding "Paimon connector never overrides getTableStatistics so base-table row count is always UNKNOWN minus 1".',
      'Current code: ' + CONNDIR + 'PaimonConnectorMetadata.java (does it override getTableStatistics from the ConnectorMetadata SPI?), PaimonCatalogOps.java. Check fe/fe-connector/fe-connector-api/.../ConnectorMetadata.java and ConnectorTableStatistics.java for the SPI shape.',
      'Legacy: ' + LEGACY + 'PaimonExternalTable.java (fetchRowCount / getRowCount) and PaimonUtil — how legacy computed the base-table row count (sum of snapshot record counts).',
      'Design the getTableStatistics override returning the paimon snapshot row count.',
    ].join('\n'),
  },
  {
    id: 'FIX-READ-NOTNULL',
    title: 'Read path propagates paimon NOT NULL; legacy always forced columns nullable',
    detail: [
      'Read report path 10 finding "Read path propagates paimon NOT NULL to Doris column; legacy always forced columns nullable".',
      'Current code: ' + CONNDIR + 'PaimonConnectorMetadata.java mapFields + PaimonTypeMapping.java (where nullability is set on the Doris column).',
      'Legacy: ' + LEGACY + 'PaimonUtil.java type mapping — confirm legacy forced isAllowNull=true on every column regardless of paimon nullability, and WHY (BE read-path expectation).',
      'Design restoring the legacy nullable behavior on the read path. Flag clearly whether this is a pure parity restore or whether propagating NOT NULL is actually desirable (give the tradeoff for the user to confirm).',
    ].join('\n'),
  },
]

const SCHEMA = {
  type: 'object',
  properties: {
    id: { type: 'string' },
    designMarkdown: { type: 'string' },
    filesTouched: { type: 'array', items: { type: 'string' } },
    rootCauseConfirmed: { type: 'boolean' },
    notes: { type: 'string' },
  },
  required: ['id', 'designMarkdown', 'filesTouched', 'rootCauseConfirmed', 'notes'],
}

phase('Design')
log('Designing ' + FIXES.length + ' fixes, grounded in current code.')
const designs = await parallel(FIXES.map(fx => () =>
  agent(COMMON + '\n\n# FIX: ' + fx.id + '\n## ' + fx.title + '\n\n' + fx.detail,
    { label: 'design:' + fx.id, phase: 'Design', schema: SCHEMA })
))

return { designs: designs.filter(Boolean) }
