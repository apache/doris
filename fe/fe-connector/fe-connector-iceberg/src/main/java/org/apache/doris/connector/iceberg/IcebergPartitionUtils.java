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

package org.apache.doris.connector.iceberg;

import org.apache.doris.connector.api.mvcc.ConnectorMvccPartition;
import org.apache.doris.connector.api.mvcc.ConnectorMvccPartitionView;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.base.Preconditions;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.PartitionData;
import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.Table;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Type.TypeID;
import org.apache.iceberg.types.Types.NestedField;
import org.apache.iceberg.types.Types.TimestampType;
import org.apache.iceberg.util.JsonUtil;
import org.apache.iceberg.util.StructProjection;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.Month;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Self-contained port of the legacy fe-core {@code IcebergUtils} partition helpers used by the scan path
 * (P6.2-T03). The connector cannot import fe-core, so {@code getIdentityPartitionColumns} /
 * {@code getIdentityPartitionInfoMap} / {@code getPartitionValues} / {@code getPartitionDataJson} /
 * {@code serializePartitionValue} are reproduced byte-faithfully against the iceberg SDK with two
 * deliberate, documented deltas:
 *
 * <ul>
 *   <li>The timezone argument is a resolved {@link ZoneId} (instead of legacy's raw {@code String} +
 *       {@code ZoneId.of(tz)}), so a non-canonical Doris session {@code time_zone} (e.g. {@code "CST"})
 *       cannot crash partition-timestamp rendering — consistent with the T02 alias-map fix
 *       ({@code IcebergScanPlanProvider.resolveSessionZone}).</li>
 *   <li>{@code getPartitionDataJson} renders the JSON array via iceberg's bundled Jackson
 *       ({@link JsonUtil#mapper()}) rather than fe-core {@code GsonUtils.GSON}. BE re-parses the JSON
 *       array back to {@code List<String>}, so the value content is identical; only the serializer differs.</li>
 * </ul>
 */
final class IcebergPartitionUtils {

    private static final Logger LOG = LogManager.getLogger(IcebergPartitionUtils.class);

    private IcebergPartitionUtils() {
    }

    /**
     * Ordered, lowercased, de-duplicated list of the identity partition column names across <b>all</b>
     * partition specs of the table (mirrors legacy {@code IcebergUtils.getIdentityPartitionColumns}). This
     * is the {@code path_partition_keys} payload: it tells FE which slots are partition columns so they are
     * excluded from the file-decode set (the CI #968880 double-fill guard). Non-identity transforms
     * (bucket/truncate/year/month/...) are excluded.
     */
    static List<String> getIdentityPartitionColumns(Table table) {
        LinkedHashSet<String> partitionColumns = new LinkedHashSet<>();
        for (PartitionSpec spec : table.specs().values()) {
            for (PartitionField partitionField : spec.fields()) {
                if (!partitionField.transform().isIdentity()) {
                    continue;
                }
                String columnName = table.schema().findColumnName(partitionField.sourceId());
                if (columnName != null) {
                    partitionColumns.add(columnName.toLowerCase(java.util.Locale.ROOT));
                }
            }
        }
        return new ArrayList<>(partitionColumns);
    }

    /**
     * Per-file map of identity partition column (lowercased) to serialized value, skipping non-identity
     * transforms and BINARY/FIXED columns (utf8 round-trip would corrupt those). Order-preserving
     * (LinkedHashMap, spec field order). Mirrors legacy {@code IcebergUtils.getIdentityPartitionInfoMap}.
     */
    static Map<String, String> getIdentityPartitionInfoMap(PartitionData partitionData,
            PartitionSpec partitionSpec, Table table, ZoneId zone) {
        Map<String, String> partitionInfoMap = new LinkedHashMap<>();
        List<NestedField> fields = partitionData.getPartitionType().asNestedType().fields();
        List<PartitionField> partitionFields = partitionSpec.fields();
        Preconditions.checkArgument(fields.size() == partitionFields.size(),
                "PartitionData fields size does not match PartitionSpec fields size");

        for (int i = 0; i < fields.size(); i++) {
            NestedField field = fields.get(i);
            PartitionField partitionField = partitionFields.get(i);
            if (!partitionField.transform().isIdentity()) {
                continue;
            }
            TypeID partitionTypeId = field.type().typeId();
            if (partitionTypeId == TypeID.BINARY || partitionTypeId == TypeID.FIXED) {
                continue;
            }

            String columnName = table.schema().findColumnName(partitionField.sourceId());
            if (columnName == null) {
                continue;
            }
            Object value = partitionData.get(i);
            try {
                partitionInfoMap.put(columnName.toLowerCase(java.util.Locale.ROOT),
                        serializePartitionValue(field.type(), value, zone));
            } catch (UnsupportedOperationException e) {
                LOG.warn("Failed to serialize Iceberg table partition value for field {}: {}", field.name(),
                        e.getMessage());
            }
        }
        return partitionInfoMap;
    }

    /**
     * The serialized value for <b>every</b> partition field (identity + transform), in spec order, used for
     * {@code partition_data_json}. Mirrors legacy {@code IcebergUtils.getPartitionValues}. A field whose
     * value cannot be serialized (BINARY/FIXED) yields a {@code null} entry (not dropped) to keep positional
     * alignment with the spec.
     */
    static List<String> getPartitionValues(PartitionData partitionData, PartitionSpec partitionSpec, ZoneId zone) {
        List<NestedField> fields = partitionData.getPartitionType().asNestedType().fields();
        Preconditions.checkArgument(fields.size() == partitionSpec.fields().size(),
                "PartitionData fields size does not match PartitionSpec fields size");

        List<String> partitionValues = new ArrayList<>(fields.size());
        for (int i = 0; i < fields.size(); i++) {
            NestedField field = fields.get(i);
            Object value = partitionData.get(i);
            try {
                partitionValues.add(serializePartitionValue(field.type(), value, zone));
            } catch (UnsupportedOperationException e) {
                LOG.warn("Failed to serialize Iceberg partition value for field {}: {}", field.name(),
                        e.getMessage());
                partitionValues.add(null);
            }
        }
        return partitionValues;
    }

    /**
     * The {@code partition_data_json} string: a JSON array of the serialized partition values. Rendered via
     * iceberg's bundled Jackson (see class javadoc) instead of fe-core Gson — BE re-parses it, so the value
     * content is identical.
     */
    static String getPartitionDataJson(PartitionData partitionData, PartitionSpec partitionSpec, ZoneId zone) {
        List<String> partitionValues = getPartitionValues(partitionData, partitionSpec, zone);
        try {
            return JsonUtil.mapper().writeValueAsString(partitionValues);
        } catch (com.fasterxml.jackson.core.JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize iceberg partition data to JSON, error message is:"
                    + e.getMessage(), e);
        }
    }

    /**
     * Faithful port of legacy {@code IcebergUtils.serializePartitionValue}: render a single partition value
     * to its string form, dispatching on the iceberg {@link Type}. {@code null} values pass through as
     * {@code null}; BINARY/FIXED throw {@link UnsupportedOperationException} (a utf8 round-trip would corrupt
     * the bytes). Package-private for direct parity testing.
     */
    static String serializePartitionValue(Type type, Object value, ZoneId zone) {
        switch (type.typeId()) {
            case BOOLEAN:
            case INTEGER:
            case LONG:
            case STRING:
            case UUID:
            case DECIMAL:
                if (value == null) {
                    return null;
                }
                return value.toString();
            case FLOAT:
                if (value == null) {
                    return null;
                }
                return Float.toString((Float) value);
            case DOUBLE:
                if (value == null) {
                    return null;
                }
                return Double.toString((Double) value);
            // BINARY / FIXED are intentionally unsupported: returning a utf8 string may corrupt the data.
            case DATE:
                if (value == null) {
                    return null;
                }
                // Iceberg date is stored as days since epoch (1970-01-01).
                return LocalDate.ofEpochDay((Integer) value).format(DateTimeFormatter.ISO_LOCAL_DATE);
            case TIME:
                if (value == null) {
                    return null;
                }
                // Iceberg time is stored as microseconds since midnight.
                long micros = (Long) value;
                return LocalTime.ofNanoOfDay(micros * 1000).format(DateTimeFormatter.ISO_LOCAL_TIME);
            case TIMESTAMP:
                if (value == null) {
                    return null;
                }
                // Iceberg timestamp is stored as microseconds since epoch (1970-01-01T00:00:00).
                long timestampMicros = (Long) value;
                LocalDateTime timestamp = LocalDateTime.ofEpochSecond(
                        timestampMicros / 1_000_000, (int) (timestampMicros % 1_000_000) * 1000, ZoneOffset.UTC);
                // timestamptz when shouldAdjustToUTC() — render the stored UTC instant in the session zone.
                if (((TimestampType) type).shouldAdjustToUTC()) {
                    timestamp = timestamp.atZone(ZoneOffset.UTC).withZoneSameInstant(zone).toLocalDateTime();
                }
                return timestamp.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
            default:
                throw new UnsupportedOperationException("Unsupported type for serializePartitionValue: " + type);
        }
    }

    // Canonical partition-timestamp format ("yyyy-MM-dd HH:mm:ss" with an optional micro/nano fraction),
    // the form BE renders human-readable partition values in. Legacy parsed via the nereids multi-format
    // DateLiteral.parseDateTime (connector-forbidden); the canonical form is the only one BE emits, so this
    // single formatter is equivalent in practice (DV-T04-c). Mirrors the scan-side IcebergTimeUtils tradeoff.
    private static final DateTimeFormatter TIMESTAMP_PARTITION_FORMAT = new DateTimeFormatterBuilder()
            .appendPattern("yyyy-MM-dd HH:mm:ss")
            .optionalStart()
            .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
            .optionalEnd()
            .toFormatter();

    /**
     * Faithful port of legacy {@code IcebergUtils.parsePartitionValueFromString}: the write-direction inverse
     * of {@link #serializePartitionValue} — BE sends a human-readable partition string, this converts it to
     * the iceberg internal partition object (DATE -&gt; epoch-day Integer, TIMESTAMP -&gt; epoch-micros Long,
     * etc.) for {@link PartitionData}. {@code null} passes through as {@code null}.
     *
     * <p>Two documented deltas vs legacy (DV-T04-c/-f): the TIMESTAMP case parses the canonical format with an
     * explicit resolved {@code zone} (legacy used the multi-format nereids parser + a thread-local zone), and
     * FLOAT/DOUBLE normalize Doris's {@code nan}/{@code inf}/{@code infinity} spellings before parsing.</p>
     */
    static Object parsePartitionValueFromString(String valueStr, Type icebergType, ZoneId zone) {
        if (valueStr == null) {
            return null;
        }
        try {
            switch (icebergType.typeId()) {
                case STRING:
                    return valueStr;
                case INTEGER:
                    return Integer.parseInt(valueStr);
                case LONG:
                    return Long.parseLong(valueStr);
                case FLOAT:
                    return Float.parseFloat(normalizeFloatingPointPartitionValue(valueStr));
                case DOUBLE:
                    return Double.parseDouble(normalizeFloatingPointPartitionValue(valueStr));
                case BOOLEAN:
                    return Boolean.parseBoolean(valueStr);
                case DATE:
                    // Iceberg date is days since epoch (1970-01-01).
                    return (int) LocalDate.parse(valueStr, DateTimeFormatter.ISO_LOCAL_DATE).toEpochDay();
                case TIMESTAMP:
                    return parseTimestampToMicros(valueStr, (TimestampType) icebergType, zone);
                case DECIMAL:
                    return new BigDecimal(valueStr);
                default:
                    throw new IllegalArgumentException("Unsupported partition value type: " + icebergType);
            }
        } catch (Exception e) {
            throw new IllegalArgumentException(String.format(
                    "Failed to convert partition value '%s' to type %s", valueStr, icebergType), e);
        }
    }

    private static String normalizeFloatingPointPartitionValue(String valueStr) {
        if ("nan".equalsIgnoreCase(valueStr)) {
            return "NaN";
        }
        if ("inf".equalsIgnoreCase(valueStr) || "+inf".equalsIgnoreCase(valueStr)
                || "infinity".equalsIgnoreCase(valueStr) || "+infinity".equalsIgnoreCase(valueStr)) {
            return "Infinity";
        }
        if ("-inf".equalsIgnoreCase(valueStr) || "-infinity".equalsIgnoreCase(valueStr)) {
            return "-Infinity";
        }
        return valueStr;
    }

    private static long parseTimestampToMicros(String valueStr, TimestampType timestampType, ZoneId sessionZone) {
        LocalDateTime ldt = LocalDateTime.parse(valueStr, TIMESTAMP_PARTITION_FORMAT);
        // timestamptz (shouldAdjustToUTC): interpret the wall-clock string in the session zone; plain timestamp:
        // interpret it in UTC. Mirrors legacy parseTimestampToMicros (DateUtils.getTimeZone vs ZoneId.of("UTC")).
        ZoneId zone = timestampType.shouldAdjustToUTC() ? sessionZone : ZoneOffset.UTC;
        Instant instant = ldt.atZone(zone).toInstant();
        return instant.getEpochSecond() * 1_000_000L + instant.getNano() / 1000L;
    }

    /**
     * Faithful port of legacy {@code IcebergUtils.parsePartitionValuesFromJson}: parse a
     * {@code partition_data_json} array (the inverse of {@link #getPartitionDataJson}) back to its list of
     * serialized partition value strings. Rendered/parsed via iceberg's bundled Jackson rather than fe-core
     * Gson (DV-T04-d) — the JSON array of strings is byte-identical either way. A blank input or a parse
     * failure yields an empty list (legacy parity).
     */
    static List<String> parsePartitionValuesFromJson(String partitionDataJson) {
        if (partitionDataJson == null || partitionDataJson.trim().isEmpty()) {
            return new ArrayList<>();
        }
        try {
            return JsonUtil.mapper().readValue(partitionDataJson, new TypeReference<List<String>>() {});
        } catch (Exception e) {
            LOG.warn("Failed to parse partition data JSON: {}", partitionDataJson, e);
            return new ArrayList<>();
        }
    }

    // ─────────────────────────── B-2: MTMV partition enumeration (RANGE view) ───────────────────────────
    // Self-contained port of the legacy fe-core iceberg MTMV partition logic — the connector cannot import
    // fe-core, so it emits a NEUTRAL ConnectorMvccPartitionView (pre-rendered string bounds + resolved
    // snapshot-id freshness) and the generic PluginDrivenMvccExternalTable assembles the RangePartitionItems.
    // Source: master IcebergExternalTable.isValidRelatedTable / getPartitionSnapshot + IcebergUtils
    // .loadPartitionInfo / loadIcebergPartition / generateIcebergPartition / getPartitionRange /
    // mergeOverlapPartitions + IcebergPartitionInfo.getLatestSnapshotId.

    private static final String YEAR = "year";
    private static final String MONTH = "month";
    private static final String DAY = "day";
    private static final String HOUR = "hour";

    // Iceberg partition field id starts at PARTITION_DATA_ID_START (org.apache.iceberg.PartitionSpec).
    private static final int PARTITION_DATA_ID_START = 1000;
    // Master IcebergUtils.UNKNOWN_SNAPSHOT_ID: an empty table / a null last_updated_snapshot_id row.
    private static final long UNKNOWN_SNAPSHOT_ID = -1;

    private static final DateTimeFormatter RANGE_DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter RANGE_DATETIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    // Sort by partition-range LOW ascending; ties broken by HIGH descending (larger range first), so an
    // enclosing partition precedes the ones it encloses. Parity with master IcebergUtils.RangeComparator.
    private static final Comparator<RangeBuild> RANGE_COMPARATOR = (p1, p2) -> {
        int cmpLow = p1.lower.compareTo(p2.lower);
        return cmpLow == 0 ? p2.upper.compareTo(p1.upper) : cmpLow;
    };

    /**
     * Builds the connector-supplied {@link ConnectorMvccPartitionView} for an iceberg table acting as an MTMV
     * related (base) table. Port of master {@code IcebergExternalTable.getPartitionType} +
     * {@code IcebergUtils.loadPartitionInfo}: the eligibility gate decides RANGE vs UNPARTITIONED; the PARTITIONS
     * metadata table is enumerated at {@code pinnedSnapshotId} (or the table's CURRENT snapshot when it is
     * {@code < 0}); transform math yields each partition's {@code [lower, upper)} range; overlapping
     * (partition-evolution) ranges are merged; per-partition freshness is the resolved iceberg snapshot id. Must
     * be invoked inside {@code context.executeAuthenticated} (the PARTITIONS scan is a remote read).
     *
     * @param pinnedSnapshotId the query's pinned snapshot id (so the partition set + freshness stay consistent
     *        with the data-scan pin — the caller threads {@code beginQuerySnapshot}'s snapshot through the
     *        handle), or {@code < 0} to enumerate at the table's current (latest) snapshot.
     */
    static ConnectorMvccPartitionView buildMvccPartitionView(Table table, long pinnedSnapshotId) {
        if (!isValidRelatedTable(table)) {
            return ConnectorMvccPartitionView.unpartitioned();
        }
        long snapshotId;
        if (pinnedSnapshotId >= 0) {
            snapshotId = pinnedSnapshotId;
        } else {
            Snapshot current = table.currentSnapshot();
            if (current == null) {
                // A valid related table that is still empty (no snapshot yet): RANGE on the spec alone, with no
                // partitions. Parity: master getPartitionType=RANGE (spec-only) + getIcebergPartitionItems empty.
                return new ConnectorMvccPartitionView(ConnectorMvccPartitionView.Style.RANGE,
                        ConnectorMvccPartitionView.Freshness.SNAPSHOT_ID, Collections.emptyList());
            }
            snapshotId = current.snapshotId();
        }
        // The freshness fallback base = the enumeration snapshot (master uses snapshotValue.getSnapshot()
        // .getSnapshotId(), which is the same snapshot the partition info is loaded at).
        long tableSnapshotId = snapshotId;
        // The gate guarantees a single, stable source column across all specs, so any spec's field-0 sourceId
        // resolves the same source column; its iceberg type drives DATE-vs-DATETIME bound rendering.
        int sourceId = table.spec().fields().get(0).sourceId();
        Type sourceType = table.schema().findField(sourceId).type();

        // Deduplicate by partition name (last-wins) BEFORE the merge, exactly like master, which keys both
        // nameToIcebergPartition and nameToPartitionItem by name (IcebergUtils.loadPartitionInfo) so a name can
        // never enclose its own twin. The overlap merge below MUST run over this deduped set (not the raw rows):
        // two rows rendering the same field=value name have byte-identical ranges, and feeding both to the merge
        // would make the name self-enclose and be dropped (0 partitions where master keeps 1).
        Map<String, RangeBuild> allByName = new LinkedHashMap<>();
        for (IcebergRawPartition raw : loadRawPartitions(table, snapshotId)) {
            RangeBuild rb = buildRange(raw.name, raw.values.get(0), raw.transforms.get(0), sourceType,
                    raw.lastUpdateTime, raw.lastSnapshotId);
            allByName.put(rb.name, rb);
        }

        Set<String> survivors = new LinkedHashSet<>(allByName.keySet());
        Map<String, Set<String>> mergeMap =
                mergeOverlapPartitions(new ArrayList<>(allByName.values()), survivors);

        List<ConnectorMvccPartition> partitions = new ArrayList<>(survivors.size());
        for (RangeBuild rb : allByName.values()) {
            if (!survivors.contains(rb.name)) {
                continue;   // enclosed by another partition -> merged away (master removes from originPartitions)
            }
            long latest = latestSnapshotId(rb.name, mergeMap, allByName);
            // Parity master getPartitionSnapshot: partition snapshot id <= 0 falls back to the table snapshot
            // id (always > 0 here — the empty-table case returned above), so no "table snapshot also invalid"
            // throw is reachable for a non-empty table.
            long freshness = latest > 0 ? latest : tableSnapshotId;
            partitions.add(new ConnectorMvccPartition(rb.name, rb.lowerBound, rb.upperBound, freshness));
        }
        // Deterministic order (the generic model re-keys by name; sorting only stabilizes tests/diagnostics).
        partitions.sort(Comparator.comparing(ConnectorMvccPartition::getName));
        return new ConnectorMvccPartitionView(ConnectorMvccPartitionView.Style.RANGE,
                ConnectorMvccPartitionView.Freshness.SNAPSHOT_ID, partitions);
    }

    /**
     * The raw iceberg partition display names ({@code "f1=v1/f2=v2"}) of {@code table} at its CURRENT snapshot,
     * for SHOW PARTITIONS (single-column form). Unlike {@link #buildMvccPartitionView} this is NOT gated on the
     * MTMV eligibility rules — it lists the physical partitions of ANY partitioned iceberg table. An
     * unpartitioned or empty table yields an empty list. Must run inside {@code context.executeAuthenticated}.
     */
    static List<String> listPartitionNames(Table table) {
        if (table.spec().isUnpartitioned()) {
            return Collections.emptyList();
        }
        Snapshot current = table.currentSnapshot();
        if (current == null) {
            return Collections.emptyList();
        }
        List<IcebergRawPartition> raws = loadRawPartitions(table, current.snapshotId());
        List<String> names = new ArrayList<>(raws.size());
        for (IcebergRawPartition raw : raws) {
            names.add(raw.name);
        }
        return names;
    }

    /**
     * Port of master {@code IcebergExternalTable.isValidRelatedTable}: an iceberg table is a valid MTMV related
     * table iff EVERY partition spec has exactly one field whose transform is {@code year}/{@code month}/{@code
     * day}/{@code hour}, and the partition source column is stable across partition evolution (a single distinct
     * source column over all specs). Failure -> the connector reports an UNPARTITIONED view.
     */
    static boolean isValidRelatedTable(Table table) {
        Set<String> allFields = new HashSet<>();
        for (PartitionSpec spec : table.specs().values()) {
            if (spec == null) {
                return false;
            }
            List<PartitionField> fields = spec.fields();
            if (fields.size() != 1) {
                return false;
            }
            PartitionField partitionField = fields.get(0);
            String transformName = partitionField.transform().toString();
            if (!YEAR.equals(transformName) && !MONTH.equals(transformName)
                    && !DAY.equals(transformName) && !HOUR.equals(transformName)) {
                return false;
            }
            allFields.add(table.schema().findColumnName(partitionField.sourceId()));
        }
        return allFields.size() == 1;
    }

    /**
     * Port of master {@code IcebergUtils.loadIcebergPartition} + {@code generateIcebergPartition}: scan the
     * PARTITIONS metadata table at {@code snapshotId} and reduce each row to an {@link IcebergRawPartition}.
     * {@code last_updated_at} (row 9) / {@code last_updated_snapshot_id} (row 10) are optional, so a missing
     * value (NPE on the typed getter) degrades to {@code 0} / {@code UNKNOWN_SNAPSHOT_ID}, exactly like master.
     */
    private static List<IcebergRawPartition> loadRawPartitions(Table table, long snapshotId) {
        Table partitionsTable = MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.PARTITIONS);
        List<IcebergRawPartition> partitions = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = partitionsTable.newScan().useSnapshot(snapshotId).planFiles()) {
            for (FileScanTask task : tasks) {
                CloseableIterable<StructLike> rows = task.asDataTask().rows();
                for (StructLike row : rows) {
                    partitions.add(generateRawPartition(table, row));
                }
            }
        } catch (IOException e) {
            LOG.warn("Failed to get Iceberg table {} partition info.", table.name(), e);
        }
        return partitions;
    }

    private static IcebergRawPartition generateRawPartition(Table table, StructLike row) {
        // PARTITIONS row layout: 0 partitionData, 1 spec_id, 2 record_count, 3 file_count,
        // 4 total_data_file_size_in_bytes, 5..8 position/equality delete stats, 9 last_updated_at,
        // 10 last_updated_snapshot_id. Only 0/1/9/10 are needed by the MTMV partition view.
        Preconditions.checkState(!table.spec().fields().isEmpty(), table.name() + " is not a partition table.");
        int specId = row.get(1, Integer.class);
        PartitionSpec partitionSpec = table.specs().get(specId);
        StructProjection partitionData = row.get(0, StructProjection.class);
        StringBuilder sb = new StringBuilder();
        List<String> partitionValues = new ArrayList<>();
        List<String> transforms = new ArrayList<>();
        for (int i = 0; i < partitionSpec.fields().size(); ++i) {
            PartitionField partitionField = partitionSpec.fields().get(i);
            Class<?> fieldClass = partitionSpec.javaClasses()[i];
            int fieldId = partitionField.fieldId();
            // Iceberg partition field id starts at PARTITION_DATA_ID_START, so the index into partitionData is
            // fieldId - PARTITION_DATA_ID_START.
            int index = fieldId - PARTITION_DATA_ID_START;
            Object o = partitionData.get(index, fieldClass);
            String fieldValue = o == null ? null : o.toString();
            sb.append(partitionField.name()).append("=").append(fieldValue).append("/");
            partitionValues.add(fieldValue);
            transforms.add(partitionField.transform().toString());
        }
        if (sb.length() > 0) {
            sb.delete(sb.length() - 1, sb.length());
        }
        long lastUpdateTime;
        long lastSnapshotId;
        try {
            lastUpdateTime = row.get(9, Long.class);
        } catch (NullPointerException e) {
            lastUpdateTime = 0;
        }
        try {
            lastSnapshotId = row.get(10, Long.class);
        } catch (NullPointerException e) {
            lastSnapshotId = UNKNOWN_SNAPSHOT_ID;
        }
        return new IcebergRawPartition(sb.toString(), partitionValues, transforms, lastUpdateTime, lastSnapshotId);
    }

    /**
     * Port of master {@code IcebergUtils.getPartitionRange}, but emits PRE-RENDERED string bounds (fe-core owns
     * {@code PartitionKey}). The {@code [lower, upper)} {@link LocalDateTime} interval is kept for the overlap
     * merge; the string bounds are rendered with the partition source column's date/datetime form. A NULL
     * partition value yields lower {@code "0000-01-01"} and an EMPTY upper bound — the signal for the generic
     * model to derive the exclusive upper as {@code lowerKey.successor()} (which is column-type/scale aware and
     * lives in fe-core), matching master's {@code nullLowKey.successor()}.
     */
    static RangeBuild buildRange(String name, String value, String transform, Type sourceType,
            long lastUpdateTime, long lastSnapshotId) {
        if (value == null) {
            LocalDateTime nullLower = LocalDateTime.of(0, 1, 1, 0, 0, 0);
            return new RangeBuild(name, nullLower, nullLower.plusDays(1),
                    Collections.singletonList("0000-01-01"), Collections.emptyList(),
                    lastUpdateTime, lastSnapshotId);
        }
        LocalDateTime epoch = Instant.EPOCH.atZone(ZoneId.of("UTC")).toLocalDateTime();
        long longValue = Long.parseLong(value);
        LocalDateTime target;
        LocalDateTime lower;
        LocalDateTime upper;
        switch (transform) {
            case HOUR:
                target = epoch.plusHours(longValue);
                lower = LocalDateTime.of(target.getYear(), target.getMonth(), target.getDayOfMonth(),
                        target.getHour(), 0, 0);
                upper = lower.plusHours(1);
                break;
            case DAY:
                target = epoch.plusDays(longValue);
                lower = LocalDateTime.of(target.getYear(), target.getMonth(), target.getDayOfMonth(), 0, 0, 0);
                upper = lower.plusDays(1);
                break;
            case MONTH:
                target = epoch.plusMonths(longValue);
                lower = LocalDateTime.of(target.getYear(), target.getMonth(), 1, 0, 0, 0);
                upper = lower.plusMonths(1);
                break;
            case YEAR:
                target = epoch.plusYears(longValue);
                lower = LocalDateTime.of(target.getYear(), Month.JANUARY, 1, 0, 0, 0);
                upper = lower.plusYears(1);
                break;
            default:
                throw new RuntimeException("Unsupported transform " + transform);
        }
        // Master renders the bound with the Doris partition-column type (the source column): iceberg DATE ->
        // "yyyy-MM-dd", iceberg TIMESTAMP/TIMESTAMPTZ -> "yyyy-MM-dd HH:mm:ss" (HOUR's source is always a
        // timestamp). Equivalent to master's c.getType().isDate()||isDateV2() formatter switch.
        DateTimeFormatter formatter = sourceType.typeId() == TypeID.DATE ? RANGE_DATE_FORMAT : RANGE_DATETIME_FORMAT;
        return new RangeBuild(name, lower, upper,
                Collections.singletonList(lower.format(formatter)),
                Collections.singletonList(upper.format(formatter)),
                lastUpdateTime, lastSnapshotId);
    }

    /**
     * Port of master {@code IcebergUtils.mergeOverlapPartitions}: merge an enclosed partition's range into the
     * enclosing one (a partition-evolution DAY range inside a MONTH range becomes one Doris partition). Removes
     * the enclosed names from {@code survivors} (mutated in place, mirroring master's {@code originPartitions
     * .remove}) and returns the enclosing-name -> {enclosing + enclosed names} map used to resolve the merged
     * partition's freshness. The merge is on aligned time {@link LocalDateTime} intervals, equivalent to
     * master's {@code Range<PartitionKey>.encloses} (year/month/day/hour ranges never partially intersect).
     */
    static Map<String, Set<String>> mergeOverlapPartitions(List<RangeBuild> builds, Set<String> survivors) {
        List<RangeBuild> entries = new ArrayList<>(builds);
        entries.sort(RANGE_COMPARATOR);
        Map<String, Set<String>> map = new HashMap<>();
        for (int i = 0; i < entries.size() - 1; i++) {
            RangeBuild first = entries.get(i);
            String firstKey = first.name;
            RangeBuild second = entries.get(i + 1);
            String secondKey = second.name;
            while (i < entries.size() && encloses(first, second)) {
                survivors.remove(secondKey);
                map.putIfAbsent(firstKey, new HashSet<>(Collections.singleton(firstKey)));
                final String finalSecondKey = secondKey;
                map.computeIfPresent(firstKey, (key, value) -> {
                    value.add(finalSecondKey);
                    return value;
                });
                i++;
                if (i >= entries.size() - 1) {
                    break;
                }
                second = entries.get(i + 1);
                secondKey = second.name;
            }
        }
        return map;
    }

    /** Closed-open {@code a} encloses {@code b} iff {@code a.lower <= b.lower && b.upper <= a.upper}. */
    private static boolean encloses(RangeBuild a, RangeBuild b) {
        return !a.lower.isAfter(b.lower) && !b.upper.isAfter(a.upper);
    }

    /**
     * Port of master {@code IcebergPartitionInfo.getLatestSnapshotId}: for a merged (enclosing) partition, the
     * snapshot id of the most-recently-updated iceberg partition in its merge set (skipping {@code <= 0} update
     * times); for a standalone partition, its own last snapshot id. The lookup uses {@code allByName} (the FULL
     * set), NOT the survivor set — master keeps {@code nameToIcebergPartition} complete and only prunes the item
     * map, so an enclosed partition's snapshot id is still resolvable here.
     */
    static long latestSnapshotId(String name, Map<String, Set<String>> mergeMap,
            Map<String, RangeBuild> allByName) {
        Set<String> mergedNames = mergeMap.get(name);
        if (mergedNames == null) {
            return allByName.get(name).lastSnapshotId;
        }
        long latestSnapshotId = -1;
        long latestUpdateTime = -1;
        for (String mergedName : mergedNames) {
            RangeBuild partition = allByName.get(mergedName);
            long lastUpdateTime = partition.lastUpdateTime;
            // Skip partitions with invalid update time (<= 0 means unknown/invalid).
            if (lastUpdateTime <= 0) {
                continue;
            }
            if (latestUpdateTime < lastUpdateTime) {
                latestUpdateTime = lastUpdateTime;
                latestSnapshotId = partition.lastSnapshotId;
            }
        }
        return latestSnapshotId;
    }

    /** One PARTITIONS-metadata-table row reduced to what the MTMV partition view needs (port of IcebergPartition). */
    private static final class IcebergRawPartition {
        private final String name;
        private final List<String> values;
        private final List<String> transforms;
        private final long lastUpdateTime;
        private final long lastSnapshotId;

        IcebergRawPartition(String name, List<String> values, List<String> transforms,
                long lastUpdateTime, long lastSnapshotId) {
            this.name = name;
            this.values = values;
            this.transforms = transforms;
            this.lastUpdateTime = lastUpdateTime;
            this.lastSnapshotId = lastSnapshotId;
        }
    }

    /** A single physical partition's computed range: time interval (for the overlap merge) + pre-rendered bounds. */
    static final class RangeBuild {
        private final String name;
        private final LocalDateTime lower;
        private final LocalDateTime upper;
        private final List<String> lowerBound;
        private final List<String> upperBound;   // empty => NULL-min partition; fe-core derives lower.successor()
        private final long lastUpdateTime;
        private final long lastSnapshotId;

        RangeBuild(String name, LocalDateTime lower, LocalDateTime upper, List<String> lowerBound,
                List<String> upperBound, long lastUpdateTime, long lastSnapshotId) {
            this.name = name;
            this.lower = lower;
            this.upper = upper;
            this.lowerBound = lowerBound;
            this.upperBound = upperBound;
            this.lastUpdateTime = lastUpdateTime;
            this.lastSnapshotId = lastSnapshotId;
        }

        List<String> getLowerBound() {
            return lowerBound;
        }

        List<String> getUpperBound() {
            return upperBound;
        }
    }
}
