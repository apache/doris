// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.datasource.paimon.source;

import org.apache.doris.datasource.paimon.PaimonScanParams;
import org.apache.doris.system.Backend;

import org.apache.commons.lang3.StringUtils;
import org.apache.paimon.CoreOptions;
import org.apache.paimon.rest.RESTTokenFileIO;
import org.apache.paimon.schema.TableSchema;
import org.apache.paimon.table.FallbackReadFileStoreTable;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;

import java.util.Collection;
import java.util.Locale;
import java.util.Map;

/**
 * The table-, storage- and backend-level gates of the paimon-rust reader,
 * computed once from the table handle and shared by every call site that must
 * agree on them: {@code PaimonScanNode}'s per-split gate (both the batch
 * reader and the PK-vector search) and the {@code PushDownVectorTopNIntoPaimonScan}
 * rewrite's pre-flight via {@code PaimonVectorSearch.vectorScanEligible}. The
 * rewrite is one-way (a rewritten PK-vector split has no JNI fallback), so
 * both sites must evaluate the same conditions; this class is that single
 * definition.
 *
 * <p>Covers only what is a property of the table handle, the storage shape or
 * the cluster's backends. Everything that depends on the individual split
 * (native DataSplit shape, unmaterialized deletion vectors, external-path
 * files, the PaimonRustReaderCapabilities whitelist) or on the final scan
 * tuple (ORC TIMESTAMP_LTZ, projected VARIANT) stays with the scan node.
 */
final class PaimonRustEligibility {

    // BE opens the table via paimon_table_from_schema_json, which needs the resolved
    // TableSchema that only FileStoreTable exposes via schema(). A table that is not a
    // FileStoreTable (e.g. a sys table backed by DataSplit) cannot ship a schema JSON,
    // so it falls back to JNI rather than sending an incomplete PAIMON_RUST request
    // that BE would reject.
    final boolean isFileStoreTable;

    // Both sides of a FallbackReadFileStoreTable wrap their splits, and the pinned
    // rust decoder rejects a FallbackDataSplit outright ("trailing bytes after
    // DataSplit" — it requires full-buffer consumption); the wrapper is gated as a
    // whole (any split from it routes to JNI) until the rust ABI represents both
    // sides. The per-split FallbackSplit shape is judged separately in PaimonScanNode.
    final boolean fallbackReadTable;

    // A renewable REST token reached the shipped properties as a plain value; only
    // the table's FileIO type reveals it expires: doInitialize snapshots
    // RESTTokenFileIO.validToken().token() into the backend storage properties,
    // discarding expireAtMillis and the REST refresh context, so the shipped
    // credentials look static — but the pinned rust table reuses one option map with
    // no refresh callback, while the JNI RESTTokenFileIO checks expiry before each
    // file operation and obtains a replacement token. A queued or long scan that
    // crosses the token TTL would start on rust and later fail authentication.
    // Null-safe: a table handle whose FileIO is not resolved stays rust-eligible,
    // mirroring the CoreOptions null-safety below.
    final boolean restTokenTable;

    // query-auth.enabled tables stay on JNI: when catalog authorization succeeds with
    // no row filter or column mask, Paimon still leaves an ordinary DataSplit
    // (restricted results use QueryAuthSplit and are already handled by the
    // nativeSplit gate), so this table shape passes the compound gate — but the
    // shipped schema keeps query-auth.enabled=true and the pinned rust ReadBuilder
    // rejects every such table (its CoreOptions::ensure_read_authorized fails closed
    // because the client cannot enforce the row filter / column masking), turning a
    // valid authorized scan into a BE-open failure. Until the authorization result
    // can be transported and enforced by the rust ABI, these tables route to JNI.
    final boolean queryAuthTable;

    // Partial-update / aggregation tables with deletion vectors: the pinned rust
    // read_pk rejects merge-engine=partial-update/aggregation with
    // deletion-vectors.merge-on-read=true outright, so that table option routes the
    // whole table to JNI. merge-on-read has no Java accessor in the pinned paimon, so
    // it is read raw from the schema options map — the one the BE rust reader
    // deserializes from the shipped schema JSON, so it cannot diverge from what BE
    // sees — with the rust parsing semantics (any case-insensitive "true" is on,
    // default false). For the remaining PU/AGG DV tables, per-split materialization
    // is judged in PaimonScanNode (splitDvNotMaterialized); Deduplicate stays
    // rust-eligible (its read_pk routes uncompacted splits to the KV reader, which
    // applies the attached per-file DVs).
    final boolean puAggDeletionVectors;
    final boolean dvMergeOnRead;

    // deduplicate.ignore-delete=true tables stay on JNI: Java's
    // DeduplicateMergeFunction skips retract records when the option is set —
    // including old, uncompacted files that still contain them — but the pinned rust
    // read_pk does not pass table options into its deduplicate merge: it picks the
    // latest row and omits the key when that row is DELETE/UPDATE_BEFORE. An
    // uncompacted insert followed by a delete therefore returns the insert through
    // JNI but silently disappears through rust. Gate the option until the rust
    // merge implements it.
    final boolean deduplicateIgnoreDelete;

    // Non-DV merge options the pinned rust read rejects: Java supports
    // partial-update.remove-record-on-delete / aggregation.remove-record-on-delete
    // and the wider per-field retract matrix, but the rust PartialUpdateConfig /
    // AggregationConfig validations return Unsupported for them — and the DV-derived
    // gates above only cover deletion-vector tables, so an ordinary non-DV DataSplit
    // with one of these options would pass the compound gate and fail during the
    // rust merge construction. Mirrors the exact rust key matrix (presence, not
    // values) against the same schema options map BE deserializes.
    final boolean rustUnsupportedMergeOption;

    // Branch tables can retain persisted modes without a selector: the shipped
    // schema options carry scan.mode, and the pinned Rust builders only accept the
    // default.
    final boolean scanModeNotDefault;

    // The credential provider shape is evaluated by providerModeTranslatable below.
    final boolean providerModeTranslatable;

    // Scheme capability gate: the pinned paimon-rust storage dispatcher (io/storage.rs)
    // selects the FileIO parser from the table location's URI scheme, and
    // libpaimon_c.a compiles in separate COS, OBS, GCS and Azdls parsers besides the
    // OSS and S3 ones. Doris normalizes every object store's credentials into the
    // AWS_* / use_path_style aliases, which the BE rust bridge translates only into
    // the fs.oss.* and s3.* key families — a cosn:// / obs:// / gs:// / abfs://
    // warehouse would reach its scheme's parser without the key family it reads and
    // fail the open instead of using JNI. Only the schemes whose property translation
    // is implemented and open-tested (s3 / s3a / oss) plus the credential-free hdfs
    // and local-filesystem parsers stay rust-eligible. A null location also routes to
    // JNI: the rust path needs the paimon_table that only a real location can provide.
    final boolean schemeCapabilityVerified;

    // An hdfs:// location is scheme-verified only together with the credential-free
    // backend shape: the backend storage properties that ship to BE also carry an
    // HDFS catalog's authentication (kerberos principal / keytab, proxy user, HA
    // nameservice config), none of which the pinned rust HDFS parser reads — the
    // scan would open as the BE process's ambient identity instead of the catalog's
    // configured one and fail the access JNI honors. See isRustVerifiedHdfsBackend.
    final boolean hdfsBackendVerified;

    // The BE capability negotiation requires every candidate backend to report
    // supports_paimon_rust_reader.
    final boolean allBackendsRustCapable;

    private PaimonRustEligibility(boolean isFileStoreTable, boolean fallbackReadTable,
            boolean restTokenTable, boolean queryAuthTable, boolean puAggDeletionVectors,
            boolean dvMergeOnRead, boolean deduplicateIgnoreDelete, boolean rustUnsupportedMergeOption,
            boolean scanModeNotDefault, boolean providerModeTranslatable, boolean schemeCapabilityVerified,
            boolean hdfsBackendVerified, boolean allBackendsRustCapable) {
        this.isFileStoreTable = isFileStoreTable;
        this.fallbackReadTable = fallbackReadTable;
        this.restTokenTable = restTokenTable;
        this.queryAuthTable = queryAuthTable;
        this.puAggDeletionVectors = puAggDeletionVectors;
        this.dvMergeOnRead = dvMergeOnRead;
        this.deduplicateIgnoreDelete = deduplicateIgnoreDelete;
        this.rustUnsupportedMergeOption = rustUnsupportedMergeOption;
        this.scanModeNotDefault = scanModeNotDefault;
        this.providerModeTranslatable = providerModeTranslatable;
        this.schemeCapabilityVerified = schemeCapabilityVerified;
        this.hdfsBackendVerified = hdfsBackendVerified;
        this.allBackendsRustCapable = allBackendsRustCapable;
    }

    /**
     * Whether the credential provider shape in the backend storage properties can be
     * translated to the pinned paimon-rust reader without changing the identity the
     * catalog configured. The rust S3 bridge maps static credentials, anonymous access
     * (AWS_CREDENTIALS_PROVIDER_TYPE=ANONYMOUS -> s3.anonymous) and assume-role, but
     * the remaining provider modes are ambient JVM provider chains (ENV,
     * SYSTEM_PROPERTIES, WEB_IDENTITY, CONTAINER, INSTANCE_PROFILE) with no paimon-rust
     * equivalent — rust would silently sign with whatever its own ambient chain
     * resolves to. Lives here as one of the shared eligibility gates; also called
     * directly for the JNI fallback-reason logging.
     */
    static boolean providerModeTranslatable(String location,
            Map<String, String> backendStorageProperties) {
        String providerType = backendStorageProperties == null
                ? null : backendStorageProperties.get("AWS_CREDENTIALS_PROVIDER_TYPE");
        String mode = providerType == null ? "DEFAULT" : providerType.trim().toUpperCase(Locale.ROOT);
        if (mode.isEmpty()) {
            mode = "DEFAULT";
        }
        if (location != null && (location.startsWith("s3://") || location.startsWith("s3a://"))) {
            // Java DEFAULT may resolve JVM properties or anonymous credentials;
            // Rust's ambient chain is different. Only explicit keys or explicit
            // anonymous access can cross this boundary without changing identity.
            String accessKey = backendStorageProperties == null
                    ? null : backendStorageProperties.get("AWS_ACCESS_KEY");
            String secretKey = backendStorageProperties == null
                    ? null : backendStorageProperties.get("AWS_SECRET_KEY");
            boolean staticKeys = accessKey != null && !accessKey.trim().isEmpty()
                    && secretKey != null && !secretKey.trim().isEmpty();
            // Hadoop's Simple provider gives static keys precedence over anonymous,
            // role and token settings. Rust interprets those settings differently.
            boolean conflictingStaticSettings = staticKeys && (mode.equals("ANONYMOUS")
                    || StringUtils.isNotEmpty(backendStorageProperties.get("AWS_ROLE_ARN"))
                    || StringUtils.isNotEmpty(backendStorageProperties.get("AWS_TOKEN")));
            return !conflictingStaticSettings
                    && (mode.equals("ANONYMOUS") || (mode.equals("DEFAULT") && staticKeys));
        }
        if (providerType != null) {
            // OSS has no anonymous FileIO mode in the pinned Rust dependency.
            return mode.equals("DEFAULT")
                    || (mode.equals("ANONYMOUS") && (location == null
                            || !location.startsWith("oss://")));
        }
        return true;
    }

    static PaimonRustEligibility of(Table table, String tableLocation,
            Map<String, String> backendStorageProperties, Collection<Backend> backends) {
        boolean isFileStoreTable = table instanceof FileStoreTable;
        boolean fallbackReadTable = table instanceof FallbackReadFileStoreTable;
        boolean restTokenTable = false;
        boolean queryAuthTable = false;
        boolean puAggDeletionVectors = false;
        boolean dvMergeOnRead = false;
        boolean deduplicateIgnoreDelete = false;
        boolean rustUnsupportedMergeOption = false;
        if (isFileStoreTable) {
            FileStoreTable fileStoreTable = (FileStoreTable) table;
            restTokenTable = fileStoreTable.fileIO() instanceof RESTTokenFileIO;
            CoreOptions resolvedCoreOptions = fileStoreTable.coreOptions();
            // Null-safe: a table handle whose CoreOptions is not resolved (e.g. some
            // wrapper shapes) stays rust-eligible rather than failing here — the rust
            // open itself rejects such a table if the option is really set.
            if (resolvedCoreOptions != null) {
                queryAuthTable = resolvedCoreOptions.queryAuthEnabled();
                CoreOptions.MergeEngine mergeEngine = resolvedCoreOptions.mergeEngine();
                if (resolvedCoreOptions.deletionVectorsEnabled()
                        && (mergeEngine == CoreOptions.MergeEngine.PARTIAL_UPDATE
                                || mergeEngine == CoreOptions.MergeEngine.AGGREGATE)) {
                    puAggDeletionVectors = true;
                    TableSchema dvSchema = fileStoreTable.schema();
                    Map<String, String> dvOptions = dvSchema == null ? null : dvSchema.options();
                    String mergeOnRead = dvOptions == null
                            ? null : dvOptions.get(PaimonScanNode.DELETION_VECTORS_MERGE_ON_READ);
                    dvMergeOnRead = "true".equalsIgnoreCase(mergeOnRead);
                }
                if (mergeEngine == CoreOptions.MergeEngine.DEDUPLICATE
                        && resolvedCoreOptions.ignoreDelete()) {
                    deduplicateIgnoreDelete = true;
                }
                if (mergeEngine == CoreOptions.MergeEngine.PARTIAL_UPDATE
                        || mergeEngine == CoreOptions.MergeEngine.AGGREGATE) {
                    TableSchema mergeSchema = fileStoreTable.schema();
                    Map<String, String> mergeOptions =
                            mergeSchema == null ? null : mergeSchema.options();
                    rustUnsupportedMergeOption = mergeOptions != null
                            && PaimonScanNode.hasRustUnsupportedMergeOption(mergeOptions, mergeEngine);
                }
            }
        }
        // Null-safe on a null schema, mirroring the CoreOptions null-safety above:
        // a fallback-shaped table can carry a null schema.
        boolean scanModeNotDefault = false;
        if (isFileStoreTable && ((FileStoreTable) table).schema() != null) {
            TableSchema schema = ((FileStoreTable) table).schema();
            String scanMode = PaimonScanParams.withoutTimeTravelSelectors(schema)
                    .options().get(CoreOptions.SCAN_MODE.key());
            scanModeNotDefault = scanMode != null && !"default".equalsIgnoreCase(scanMode);
        }
        boolean schemeCapabilityVerified = PaimonScanNode.isRustVerifiedLocationScheme(tableLocation);
        boolean hdfsBackendVerified = !PaimonScanNode.isHdfsLocationScheme(tableLocation)
                || PaimonScanNode.isRustVerifiedHdfsBackend(backendStorageProperties);
        boolean providerModeTranslatable = providerModeTranslatable(
                tableLocation, backendStorageProperties);
        boolean allBackendsRustCapable = backends != null && !backends.isEmpty()
                && backends.stream().allMatch(Backend::isPaimonRustReaderSupported);
        return new PaimonRustEligibility(isFileStoreTable, fallbackReadTable, restTokenTable,
                queryAuthTable, puAggDeletionVectors, dvMergeOnRead, deduplicateIgnoreDelete,
                rustUnsupportedMergeOption, scanModeNotDefault, providerModeTranslatable,
                schemeCapabilityVerified, hdfsBackendVerified, allBackendsRustCapable);
    }

    /** Whether the table, storage and backend shapes all admit the paimon-rust reader. */
    boolean eligible() {
        return isFileStoreTable && !fallbackReadTable
                && !restTokenTable && !queryAuthTable
                && !dvMergeOnRead && !deduplicateIgnoreDelete && !rustUnsupportedMergeOption
                && !scanModeNotDefault
                && providerModeTranslatable && schemeCapabilityVerified && hdfsBackendVerified
                && allBackendsRustCapable;
    }
}
