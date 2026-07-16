export const meta = {
  name: 'd2-cache-recon',
  description: 'HEAD-grounded recon for D2 connector-owned scan-side cache (retire fe-core Hive/Hudi/Iceberg caches + 4 routing gates + max-partition-time)',
  phases: [
    { title: 'Recon', detail: '8 dimension readers over the cache subsystem, consumers, connector templates, max-time/invalidation, Trino' },
    { title: 'Critique', detail: '3 adversarial critics: completeness, flip-safety, scope-boundary' },
  ],
}

const READER_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    dimension: { type: 'string' },
    summary: { type: 'string', description: '4-8 sentence executive summary of what you found, in plain terms' },
    facts: {
      type: 'array',
      description: 'Code-grounded facts. Every one MUST cite file:line evidence verified at HEAD.',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          claim: { type: 'string' },
          evidence: { type: 'string', description: 'file:line (repo-relative). Multiple allowed.' },
          implication: { type: 'string', description: 'why it matters for D2 / the flip' },
        },
        required: ['claim', 'evidence'],
      },
    },
    flipImpact: {
      type: 'array',
      description: 'For this dimension: what silently breaks / changes / must be replaced at the atomic flip (when a type=hms catalog becomes a PluginDriven catalog and cache routing collapses to ENGINE_DEFAULT).',
      items: { type: 'string' },
    },
    designOptions: {
      type: 'array',
      description: 'Concrete options/tradeoffs you surfaced for the design (do NOT pick one; list them with pros/cons).',
      items: { type: 'string' },
    },
    openQuestions: { type: 'array', items: { type: 'string' } },
    filesRead: { type: 'array', items: { type: 'string' } },
  },
  required: ['dimension', 'summary', 'facts', 'flipImpact'],
}

const CRITIC_SCHEMA = {
  type: 'object',
  additionalProperties: false,
  properties: {
    verdict: { type: 'string' },
    gaps: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          area: { type: 'string' },
          whatsMissing: { type: 'string' },
          whyItMatters: { type: 'string' },
          severity: { type: 'string', enum: ['blocker', 'major', 'minor', 'nit'] },
          suggestedResolution: { type: 'string' },
          evidence: { type: 'string' },
        },
        required: ['area', 'whatsMissing', 'severity'],
      },
    },
    confirmedRisks: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          risk: { type: 'string' },
          severity: { type: 'string', enum: ['blocker', 'major', 'minor', 'nit'] },
          evidence: { type: 'string' },
        },
        required: ['risk', 'severity'],
      },
    },
    refuted: { type: 'array', description: 'Worries you checked against HEAD and dismissed, with why.', items: { type: 'string' } },
  },
  required: ['verdict', 'gaps'],
}

const CTX = `
CONTEXT — you are reconnoitering for a DESIGN DOC (do not write code, do not implement).

Project: Apache Doris catalog-SPI migration. Legacy \`type=hms\` catalogs (HMSExternalCatalog/HMSExternalTable, in fe-core \`datasource/hive|hudi|iceberg\`) are being migrated to a plugin-connector model (PluginDrivenExternalCatalog/PluginDrivenExternalTable holding an fe-connector-hive connector loaded in a child-first classloader). The migration ends in ONE ATOMIC FLIP that adds "hms" to SPI_READY_TYPES and remaps GSON subtypes so every hms catalog/db/table instantiates as PluginDriven* classes instead of the legacy classes.

THE D2 TASK (what this recon informs): "connector-owned scan-side cache." Today fe-core owns three engine metadata caches — HiveExternalMetaCache, HudiExternalMetaCache, IcebergExternalMetaCache — routed by ExternalMetaCacheRouteResolver.addBuiltinRoutes(): a type=hms catalog routes to HIVE+HUDI+ICEBERG caches (ExternalMetaCacheRouteResolver.java:66-70). At the flip, the catalog is no longer \`instanceof HMSExternalCatalog\`, so routing falls through to ENGINE_DEFAULT (:72-73) and these three caches SILENTLY stop being fed/invalidated for hms — no crash, no log. Decision D2 (LOCKED): the hive connector must OWN its scan-side cache (mirroring how the paimon & iceberg-native connectors own theirs today), so that routing-collapse is harmless. Then retire the three fe-core caches + the four instanceof routing gates WITH the flip set. Coupled sub-item §2.6: flipped plain-hive \`getNewestUpdateVersionOrTime\` (PluginDrivenMvccExternalTable.java:713-714) returns constant 0 (names-only listPartitions → all lastModifiedMillis -1 → filtered → orElse(0)), so hive-backed SQL-dictionary / MV auto-refresh silently never sees a newer version. D2 must let the connector surface a real max-partition modify time cheaply.

HARD ARCHITECTURE RULES (from project memory — respect them in what you flag):
- fe-core does NOT parse connector properties; storage parsing → fe-filesystem, meta parsing → fe-connector (both plugin-side).
- fe-core depends only on fe-connector-api/-spi (maven). Plugin connector classes load in a SEPARATE child-first classloader. A connector CANNOT import fe-core datasource classes. If you see "connector references HiveExternalMetaCache", determine whether it's a real import, a comment/string, or a VENDORED same-simple-name copy in a different package — this matters a lot.
- The generic fe-core SPI layer must stay connector-agnostic (no source-specific branches).
- Cross-classloader calls into plugins must pin TCCL to the connector classloader.

DELIVERABLE: return the READER_SCHEMA object. Every fact needs file:line evidence you actually opened and verified at HEAD (branch catalog-spi-11-hive, HEAD commit d43ba31f3b3). Distinguish legacy deletion-unit code (dies at flip, needs NO replacement) from flip-survivor code (needs a replacement path). Surface design OPTIONS with tradeoffs; do not decide.
`

phase('Recon')

const readers = [
  {
    label: 'dim-cache-machinery',
    prompt: `${CTX}

YOUR DIMENSION = the shared fe-core cache MACHINERY (not the engine-specific caches themselves). Read and map:
- fe/fe-core/src/main/java/org/apache/doris/datasource/ExternalMetaCacheMgr.java (the manager; public entry points prepareCatalog/invalidate*/removeCatalog; the CacheKey inner class; invalidateTableCache)
- fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/ExternalMetaCache.java (interface — already known: engine()/aliases()/initCatalog/entry/invalidate*/stats/close)
- fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/AbstractExternalMetaCache.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/ExternalMetaCacheRegistry.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/ExternalMetaCacheRouteResolver.java (the routing — ENGINE_DEFAULT fallback at :72-73)
- fe/fe-core/src/main/java/org/apache/doris/connector/ExternalMetaCacheInvalidator.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/metacache/MetaCacheEntry.java / MetaCacheEntryStats.java
- fe/fe-core/src/main/java/org/apache/doris/datasource/doris/DorisExternalMetaCache.java (a peer engine cache — how a non-hive engine registers, for contrast)

Answer: What is the engine→catalog→entry model? How do caches register with the registry and get resolved by engine name? What EXACTLY is ENGINE_DEFAULT — is there a registered "default" cache, or does registry.resolve("default") return null / throw? Trace what happens to every ExternalMetaCacheMgr public method for a flipped hms catalog (routing → ENGINE_DEFAULT). Which manager methods are called on the scan hot-path vs on REFRESH/DDL/event paths? Does the machinery itself survive the flip (it serves paimon/jdbc/doris too) or get retired? Note the CacheKey/registry stats surface.`,
  },
  {
    label: 'dim-hive-cache',
    prompt: `${CTX}

YOUR DIMENSION = HiveExternalMetaCache internals (the primary cache to relocate into the hive connector).
Read fe/fe-core/src/main/java/org/apache/doris/datasource/hive/HiveExternalMetaCache.java IN FULL. Enumerate EVERY distinct cache it holds (schema cache, partition-value cache, partition cache, file cache, and any others — note the inner key classes PartitionValueCacheKey:849 / PartitionCacheKey:882 / FileCacheKey:942). For each cache: what is the key, what is the value, which loader populates it, and what remote calls (HMS thrift / filesystem listing) does the loader make. Document the four instanceof gates (:203, :209, :248, :274) — what does each gate protect and what does it do for a non-HMS catalog/table. Find how max-partition modify time / lastModifiedTime is computed here (relevant to §2.6 getNewestUpdateVersionOrTime). Note file-listing / partition-listing caching that is on the SCAN hot-path (this is the core of what the hive connector must own). Note the eager-init behavior (initCatalog).

Then classify: which of these caches are SCAN-side (must be owned by the connector post-flip) vs which serve only legacy HMSExternalTable/Catalog paths that die at the flip. Surface options for WHERE the relocated cache lives (connector-internal vs the shared fe-connector-cache framework) with tradeoffs.`,
  },
  {
    label: 'dim-hudi-iceberg-cache',
    prompt: `${CTX}

YOUR DIMENSION = HudiExternalMetaCache + IcebergExternalMetaCache (the other two fe-core caches routed for hms), and whether they even need a replacement.
Read:
- fe/fe-core/src/main/java/org/apache/doris/datasource/hudi/HudiExternalMetaCache.java IN FULL (gate at :221 = \`!(dorisTable instanceof HMSExternalTable)\`).
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/IcebergExternalMetaCache.java IN FULL (gates at :213 = \`!(dorisTable instanceof MTMVRelatedTableIf)\`, :234 = \`catalog instanceof HMSExternalCatalog\`).
- fe/fe-core/src/main/java/org/apache/doris/datasource/iceberg/cache/IcebergManifestCacheLoader.java

CRUCIAL NUANCE to resolve with evidence: post-flip, an iceberg-on-HMS table is served by DELEGATION to the sibling iceberg connector (which has its OWN native caches IcebergLatestSnapshotCache / IcebergManifestCache), and a hudi-on-HMS table is served by the hudi sibling connector. So does the fe-core IcebergExternalMetaCache / HudiExternalMetaCache still serve ANYTHING for a flipped hms catalog, or does it become fully dead (served by siblings)? Enumerate exactly what each caches, who feeds it, and whether the flip's sibling-delegation already covers it. Determine: can these two caches simply be RETIRED at the flip (no connector-owned replacement needed) because the sibling connectors' native caches cover it, or is there a residual fe-core-served path? Cite evidence. Note the execution plan's claim that IcebergExternalMetaCache:234 is "unreached post-flip" — verify.`,
  },
  {
    label: 'dim-consumers',
    prompt: `${CTX}

YOUR DIMENSION = classify EVERY consumer of the three fe-core caches into (a) LEGACY deletion-unit → dies at flip, needs no replacement; (b) FLIP-SURVIVOR → needs a replacement path post-flip; (c) fe-core plumbing that just reroutes. For each survivor, say what it needs and where it should get it after the flip.

Known consumer files (grep of HiveExternalMetaCache|HudiExternalMetaCache|IcebergExternalMetaCache, non-test):
- fe-core LEGACY-suspect: datasource/hive/HMSExternalCatalog.java, HMSExternalTable.java, HiveDlaTable.java, source/HiveScanNode.java, datasource/hudi/HudiUtils.java, datasource/iceberg/source/IcebergScanNode.java, datasource/iceberg/IcebergUtils.java, statistics/HMSAnalysisTask.java, nereids/.../insert/HiveInsertExecutor.java, planner/HiveTableSink.java, datasource/hive/AcidUtil.java
- fe-core SURVIVOR-suspect: catalog/RefreshManager.java, datasource/CatalogMgr.java, datasource/FilePartitionUtils.java, datasource/hive/event/MetastoreEventsProcessor.java, tablefunction/MetadataGenerator.java, nereids/rules/expression/rules/SortedPartitionRanges.java, datasource/ExternalMetaCacheMgr.java
- CONNECTOR references (RESOLVE whether real import / comment / vendored copy): fe-connector-hive/.../HiveConnectorMetadata.java, fe-connector-hudi/.../HudiConnectorMetadata.java, fe-connector-iceberg/.../IcebergConnector.java + IcebergConnectorMetadata.java + IcebergLatestSnapshotCache.java + IcebergManifestCache.java

For each file: open the actual reference line(s), state which cache/method it touches, and classify. Pay special attention to RefreshManager, CatalogMgr, FilePartitionUtils, MetadataGenerator, SortedPartitionRanges — these likely SURVIVE and drive REFRESH/invalidation/TVF; describe exactly how they invoke the cache and what the post-flip replacement must provide. For the CONNECTOR references: this is critical — a connector importing fe-core would break layering; determine the truth (package + import statement) and report it.`,
  },
  {
    label: 'dim-connector-template',
    prompt: `${CTX}

YOUR DIMENSION = the CONNECTOR-OWNED cache TEMPLATE (how paimon & iceberg-native own scan-side caches today — the pattern the hive connector must copy).
Read:
- fe/fe-connector/fe-connector-cache/src/main/java/org/apache/doris/connector/cache/CacheFactory.java + CacheSpec.java + MetaCacheEntry.java + MetaCacheEntryStats.java + package-info.java (the SHARED cache framework — note the memory: fe-core does NOT depend on this; it's a copied framework compiled into each plugin; Caffeine pinned to 2.9.3 lowest-common version).
- fe/fe-connector/fe-connector-iceberg/src/main/java/org/apache/doris/connector/iceberg/IcebergLatestSnapshotCache.java + IcebergManifestCache.java + ManifestCacheValue.java + dlf/client/DLFCachedClientPool.java
- fe/fe-connector/fe-connector-paimon/src/main/java/org/apache/doris/connector/paimon/PaimonLatestSnapshotCache.java
- The IcebergConnector / PaimonConnector class to see where these caches are instantiated, held, and invalidated.

Answer: How does a connector INSTANTIATE and HOLD a scan-side cache (per-connector field? via CacheFactory?)? How is it keyed/scoped (per-table, per-snapshot)? How is it INVALIDATED — is there an SPI the connector implements that fe-core's REFRESH TABLE / REFRESH CATALOG calls, or is it event/TTL only? CRITICALLY: how does fe-core's ExternalMetaCacheRouteResolver end up NOT routing to these connectors (paimon/iceberg-native catalogs get ENGINE_DEFAULT and it's harmless) — trace WHY it's harmless for them today, because that is exactly the state hive must reach. Note config (TTL, size, expire) — where does the connector read cache config from (catalog properties? Config?). This is the reference model for the hive connector cache; describe it precisely enough to copy.`,
  },
  {
    label: 'dim-hive-connector-state',
    prompt: `${CTX}

YOUR DIMENSION = the CURRENT state of caching (or absence) inside fe-connector-hive, and what scan-side metadata the hive connector fetches per query.
Read the fe-connector-hive metadata + scan path:
- fe/fe-connector/fe-connector-hive/src/main/java/org/apache/doris/connector/hive/HiveConnectorMetadata.java (does it cache anything? where does it get schema/partitions/files?)
- The HiveConnector class, the scan-plan provider, and the HMS client wrappers (HmsClient / ThriftHmsClient) in the hive/hms connector modules.
- fe/fe-connector/fe-connector-metastore-hms/** and fe-connector-metastore-spi/** if present (the shared HMS resolver).
Search the hive connector module for any existing Caffeine/cache usage, and for per-query getTableSchema / listPartitions / listPartitionNames / file-listing calls.

Answer: Today (dormant, hms still legacy) does the hive connector cache ANY metadata, or does every getTableSchema/listPartitions/getTableStatistics call hit HMS thrift fresh? Enumerate the scan-side metadata reads the connector performs (schema, partition names, partition objects, file splits, column stats, table stats) and which are hot-path-per-query. Which of these does legacy HiveExternalMetaCache cache today that the connector currently re-fetches uncached? Note whether fe-connector-hive already depends on fe-connector-cache (check its pom.xml). Surface: what is the MINIMUM set of connector-owned caches needed at the flip for scan performance + correctness parity (schema? partition-name? partition-object? file-listing?), and note the TCCL/classloader considerations (filesystem listing runs plugin-side).`,
  },
  {
    label: 'dim-maxtime-invalidation',
    prompt: `${CTX}

YOUR DIMENSION = (§2.6) max-partition-time / getNewestUpdateVersionOrTime, PLUS the invalidation coupling between D2 and event-pipeline Model B.
Read:
- fe/fe-core/src/main/java/org/apache/doris/datasource/PluginDrivenMvccExternalTable.java around :713-714 (getNewestUpdateVersionOrTime returning 0 for flipped plain-hive) AND the getTableSnapshot / beginQuerySnapshot / freshness path (freshness-kind-aware, landed 3d784673ca4).
- How legacy computes newest-update-time: fe/fe-core/src/main/java/org/apache/doris/datasource/hive/HMSExternalTable.java getNewestUpdateVersionOrTime + any max-partition-time logic, and how HiveExternalMetaCache serves it.
- fe/fe-core/src/main/java/org/apache/doris/dictionary/Dictionary.java + mtmv/MTMVRelatedTableIf.java (the CONSUMERS of getNewestUpdateVersionOrTime — SQL dictionary refresh + MV freshness).
- The invalidation path: ExternalMetaCacheInvalidator, ExternalMetaCacheMgr.invalidateTableCache / invalidateDb / invalidatePartitions, RefreshManager, and how REFRESH TABLE/CATALOG + the event pipeline currently invalidate the fe-core caches.
- The event pipeline: datasource/hive/event/MetastoreEventsProcessor.java (:116 instanceof HMSExternalCatalog) — note the planned "Connector.invalidatePartition(s)" SPI (§2.1 Model B) that pairs with D2.

Answer: (1) Exactly how must the hive connector surface a cheap real max-partition modify time so flipped getNewestUpdateVersionOrTime matches legacy? What SPI shape is needed (ConnectorMvccPartitionView already exists — read fe-connector-api/.../mvcc/ConnectorMvccPartitionView.java and IcebergPartitionUtils.getNewestUpdateVersionOrTime for the iceberg precedent). (2) How is the connector-owned cache invalidated by REFRESH TABLE / REFRESH CATALOG and by metastore events — what invalidation SPI verbs does D2 need to expose, and how does that couple to event Model B (which is the NEXT task after D2)? Keep D2 and event-Model-B boundaries distinct but note the shared SPI. Surface options + tradeoffs for the max-time SPI and the invalidation SPI.`,
  },
  {
    label: 'dim-trino-reference',
    prompt: `${CTX}

YOUR DIMENSION = the Trino reference architecture for connector-owned Hive metadata caching, mapped onto this design. The user explicitly wants Trino consulted for architecture-level decisions.
From your knowledge of Trino (and any Trino-referencing comments in this codebase — grep for "Trino"/"trino" in fe-connector-hive and the metastore modules):
- How does Trino structure Hive metastore metadata caching? (CachingHiveMetastore: the per-plugin caching decorator wrapping the raw metastore client; cache config hive.metastore-cache-ttl / metastore-refresh-interval / metastore-cache-maximum-size; per-transaction vs shared caching; the SharedHiveMetastoreCache.) Where does caching live — engine or connector? How is it invalidated (flushMetadataCache procedure, per-table/per-partition flush)?
- How does Trino cache file listings / directory listings separately (the DirectoryLister / CachingDirectoryLister, hive.file-status-cache)?
- How does Trino avoid a central engine-side metadata cache (the SPI is stateless Metadata calls; the connector owns all caching)?

Answer: Map each Trino mechanism to a concrete recommendation for the Doris hive connector D2 design: (1) a CachingHiveMetastore-style decorator around the HMS client inside fe-connector-hive; (2) a directory/file-listing cache; (3) invalidation verbs the connector exposes to fe-core REFRESH/events. Note where Doris ALREADY diverges (Doris fe-core historically centralized the cache in HiveExternalMetaCache; the D2 move re-aligns to Trino's connector-owned model). Flag any Trino mechanism that does NOT map cleanly (e.g. Doris's GSON edit-log replay, the SPI_READY flip model, the split file-listing across classloaders). Surface options where Trino offers more than one approach.`,
  },
]

const readerResults = await parallel(
  readers.map(r => () => agent(r.prompt, { label: r.label, phase: 'Recon', schema: READER_SCHEMA, effort: 'high' }))
)

const valid = readerResults.filter(Boolean)
log(`Recon done: ${valid.length}/${readers.length} dimensions returned.`)

// Build a compact digest of all reader findings for the critics.
function digest(list) {
  return list.map(r => {
    const facts = (r.facts || []).map(f => `    - ${f.claim} [${f.evidence}]${f.implication ? ' → ' + f.implication : ''}`).join('\n')
    const flip = (r.flipImpact || []).map(x => `    - ${x}`).join('\n')
    const opts = (r.designOptions || []).map(x => `    - ${x}`).join('\n')
    const oq = (r.openQuestions || []).map(x => `    - ${x}`).join('\n')
    return `### ${r.dimension}\nSUMMARY: ${r.summary}\nFACTS:\n${facts}\nFLIP-IMPACT:\n${flip}\nDESIGN-OPTIONS:\n${opts}\nOPEN-QUESTIONS:\n${oq}`
  }).join('\n\n')
}
const allFindings = digest(valid)

phase('Critique')

const critics = [
  {
    label: 'critic-completeness',
    prompt: `${CTX}

You are a COMPLETENESS critic. Below are the combined recon findings for the D2 connector-owned-cache design. Your job: find what is MISSING or unverified that would silently break post-flip if the connector-owned cache design omits it. Adversarially hunt for: (a) a consumer of the three fe-core caches not classified (legacy-die vs survivor); (b) a cached entry (schema/partition/file/stats/max-time) with no post-flip owner; (c) a REFRESH / DDL / event / TVF / dictionary / MV path that reads the cache and would collapse to ENGINE_DEFAULT silently; (d) a scan hot-path that becomes uncached (perf regression) at the flip; (e) an invalidation verb the connector cache won't receive. For each gap, cite the evidence file:line if you can and rank severity. Also RE-VERIFY any load-bearing claim you doubt by reasoning about the cited evidence. Return CRITIC_SCHEMA.

COMBINED FINDINGS:
${allFindings}`,
  },
  {
    label: 'critic-flip-safety',
    prompt: `${CTX}

You are a FLIP-SAFETY & DORMANCY critic. Below are the combined recon findings. Your job: stress-test the core D2 premise — "the hive connector owns its scan-side cache, so at the flip, cache routing collapsing to ENGINE_DEFAULT is HARMLESS, and the 3 fe-core caches + 4 gates retire cleanly." Adversarially check: (1) Can the connector-owned cache land DORMANT (inert while hms is still legacy, exactly like every prior connector step) — or does anything force it to be co-committed with the flip? What must be dormant vs must ride the flip commit? (2) Is routing-collapse actually harmless, or is there a path where ENGINE_DEFAULT routing does something WRONG (not just no-op) — e.g. registry.resolve("default") throwing, or double-caching? (3) The iceberg/hudi fe-core caches: are they truly covered by sibling delegation post-flip, or is there a residual? (4) Ordering: does D2 have hidden dependencies on event Model B (the NEXT task) or vice versa — can D2 truly land first? (5) Any TCCL/classloader hazard when the connector-owned cache does filesystem listing or HMS calls. Rank severity, cite evidence, and list worries you REFUTED. Return CRITIC_SCHEMA.

COMBINED FINDINGS:
${allFindings}`,
  },
  {
    label: 'critic-scope',
    prompt: `${CTX}

You are a SCOPE-BOUNDARY critic. Below are the combined recon findings. Your job: pin down the RIGHT scope for D2 so the design doc is neither bloated nor missing a flip-blocker. Adversarially decide, with evidence: (1) What is genuinely REQUIRED for a correct+performant flip (minimum viable connector cache set: which of schema/partition-name/partition-object/file-listing/stats/max-time) vs what can be DEFERRED post-flip as a perf item. (2) Does hudi/iceberg fe-core cache RETIREMENT belong inside the D2 commit-set or does it just ride the flip's deletion (since siblings cover them)? (3) Where is the clean boundary between D2 (cache) and event Model B (invalidation source) and §2.6 (max-time) — what shared SPI sits on the seam, and should max-time + invalidation SPI land in D2 or their own steps? (4) Is the shared fe-connector-cache framework the right home, or should the hive cache be connector-internal like PaimonLatestSnapshotCache? (5) How should D2 be decomposed into independent dormant commits (propose an ordered sub-step list, mirroring how the iceberg/hudi lines were decomposed). Cite evidence, rank, and note anything the plan §2.2/§2.6 got wrong vs HEAD. Return CRITIC_SCHEMA.

COMBINED FINDINGS:
${allFindings}`,
  },
]

const criticResults = await parallel(
  critics.map(c => () => agent(c.prompt, { label: c.label, phase: 'Critique', schema: CRITIC_SCHEMA, effort: 'high' }))
)

return {
  readers: valid,
  critics: criticResults.filter(Boolean),
}
