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

package org.apache.doris.mtmv.ivm;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.MTMV;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.catalog.stream.BaseTableStream;
import org.apache.doris.catalog.stream.OlapTableStream;
import org.apache.doris.common.FeNameFormat;
import org.apache.doris.common.util.DebugPointUtil;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.info.TableNameInfoUtils;
import org.apache.doris.mtmv.MTMVPartitionUtil;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.functions.scalar.MurmurHash3128;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nvl;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.commands.info.ColumnDefinition;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.commons.codec.digest.DigestUtils;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * IVM (Incremental View Maintenance) utility class.
 * Centralizes IVM hidden column detection, naming, and ColumnDefinition factories.
 * Column name constants are defined in {@link Column}.
 */
public class IvmUtil {
    // Exceptional hidden-column defaults. Numeric and string-like hidden columns use zero and
    // empty string by default; add an entry here only when a hidden column requires a different
    // value.
    public static final Map<String, Expression> SPECIAL_HIDDEN_SLOT_DEFAULTS = ImmutableMap.of();
    // Max length (in characters) of the embedded key name in sink hidden key columns
    // (__DORIS_IVM_KEY_{n}_{name}_COL__). The name is sanitized (prefix/suffix stripped) and
    // truncated to this length so chained MVs never produce a column name exceeding the column
    // name length limit.
    public static final int IVM_KEY_EMBEDDED_NAME_MAX_LENGTH = 200;

    public static boolean isIvmHiddenColumn(String columnName) {
        return columnName != null && columnName.startsWith(Column.IVM_HIDDEN_COLUMN_PREFIX);
    }

    /**
     * A base scan's own storage column that must not be carried into an IVM plan.
     * <p>
     * The criterion is not "hidden column": it is a column whose name matches a column IVM owns and
     * that the delta rewrite reads back from the plan output by name. The base table's sequence
     * column ({@code Column.SEQUENCE_COL}) is the only such column today: IVM owns the MV's sequence
     * and several handlers select it with {@code findSlotByName}, so a base sequence surviving
     * normalization shadows the delta-allocated one. The two can even have different types (a base
     * DATETIME sequence against the MV's BIGINT), which turns the mistake into a rejected write.
     * <p>
     * The other hidden columns stay in place, deliberately. The base delete sign is referenced by the
     * {@code __DORIS_DELETE_SIGN__ = 0} filter directly above the scan, by ExprId; masking it would
     * leave that filter with a dangling slot. The MV's own delete sign is computed from the delta
     * factor rather than looked up, and for a merge-on-write base table a visible row's delete sign is
     * always 0. Version and commit tso are not read by name either -- the sink maps its hidden columns
     * positionally and defaults the missing ones -- and they are pruned when unused. The row lsn is
     * the row identity IVM reads for DUP detail tables.
     */
    public static boolean isBaseScanStorageColumnToMask(String columnName) {
        return Column.SEQUENCE_COL.equalsIgnoreCase(columnName);
    }

    /**
     * Sanitizes an identity key name before embedding it into a sink hidden key column
     * (__DORIS_IVM_KEY_{n}_{name}_COL__): strips leading IVM/Doris hidden prefixes and the
     * trailing _COL__ suffix, then truncates to {@link IvmUtil#IVM_KEY_EMBEDDED_NAME_MAX_LENGTH}.
     * Applied identically on the sink naming side and the agg apply-join matching side so
     * chained MVs never exceed the column name length limit.
     */
    public static String sanitizeIvmKeyName(String name) {
        String s = name;
        String ivmPrefix = Column.IVM_HIDDEN_COLUMN_PREFIX;
        if (s.regionMatches(true, 0, ivmPrefix, 0, ivmPrefix.length())) {
            s = s.substring(ivmPrefix.length());
        }
        String dorisPrefix = "__DORIS_";
        if (s.regionMatches(true, 0, dorisPrefix, 0, dorisPrefix.length())) {
            s = s.substring(dorisPrefix.length());
        }
        String colSuffix = "_COL__";
        if (s.regionMatches(true, s.length() - colSuffix.length(), colSuffix, 0, colSuffix.length())) {
            s = s.substring(0, s.length() - colSuffix.length());
        }
        return s.length() <= IVM_KEY_EMBEDDED_NAME_MAX_LENGTH
                ? s : s.substring(0, IVM_KEY_EMBEDDED_NAME_MAX_LENGTH);
    }

    public static boolean isCommonHiddenSlot(String columnName) {
        return columnName != null && (columnName.equals(Column.DELETE_SIGN)
                || (columnName.startsWith(Column.HIDDEN_COLUMN_PREFIX)
                && columnName.endsWith("_COL__") && !isIvmHiddenColumn(columnName)));
    }

    /**
     * The default a hidden slot takes when the plan writing an append-only detail MV does not produce it.
     * That model keeps no row identity in the row lsn column: the column is the MV's own binlog
     * bookkeeping, which the storage layer fills in as it writes the row, so a refresh appending a value
     * for it is not what makes the MV correct. The unique-key model is the opposite, which is why its
     * default is refused outright.
     */
    public static Expression getAppendOnlyDupHiddenSlotDefault(String columnName, DataType targetType) {
        return Column.ROW_LSN_COL.equals(columnName)
                ? new IntegerLiteral(0).castTo(targetType)
                : getCommonHiddenSlotDefault(columnName, targetType);
    }

    /**
     * The value a common hidden slot takes when the plan writing it does not produce it. The data
     * type owns that value ({@link DataType#defaultValue()}), so a hidden slot of any type -- a
     * base table's sequence column can be a DATE/DATETIME, for example -- gets a non-null
     * placeholder instead of failing the rewrite; a type without one still fails loudly.
     */
    public static Expression getCommonHiddenSlotDefault(String columnName, DataType targetType) {
        if (!isCommonHiddenSlot(columnName)) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "not an IVM common hidden slot: " + columnName);
        }
        if (Column.ROW_LSN_COL.equals(columnName)) {
            // The row lsn column is the stable row identity of DUP detail tables and must be
            // mapped from the corresponding scan output (base olap scan / RESET-SNAPSHOT
            // stream scan, or the stream lsn virtual column on the incremental side).
            // Defaulting it to 0 would silently break row-id based delete/update handling.
            // Missing it is a plan bug that must fail loudly.
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: row lsn column " + columnName
                            + " must be mapped from the scan output, not defaulted");
        }
        Expression defaultValue = SPECIAL_HIDDEN_SLOT_DEFAULTS.get(columnName);
        if (defaultValue != null) {
            return defaultValue.castTo(targetType);
        }
        return targetType.defaultValue().orElseThrow(() -> new IvmException(
                IvmFailureReason.PLAN_REWRITE_FAILED,
                "no default value for IVM hidden slot: " + columnName + ", type=" + targetType));
    }

    /**
     * Builds a null-safe deterministic row-id expression from key expressions:
     * <ul>
     *   <li>Empty list (scalar agg): returns {@code TinyIntLiteral(0)}</li>
     *   <li>Single key of an eligible MOW key type (see
     *       {@link ColumnDefinition#isEligibleKeyType}): returns the key itself directly
     *       instead of hashing it, which avoids hash collisions and makes the row-id
     *       NULL exactly when the key is NULL</li>
     *   <li>Otherwise: returns
     *       {@code murmur_hash3_128(ifnull(k1,''), isnull(k1), ifnull(k2,''), isnull(k2), ...)}</li>
     * </ul>
     *
     * <p>In the hash path, each key produces two hash arguments: {@code ifnull(cast(key AS VARCHAR), '')}
     * to prevent NULL propagation in the hash function, and {@code cast(isnull(key) AS VARCHAR)} to
     * distinguish groups that differ only in which positions are NULL (e.g. (NULL,'x') vs ('x',NULL)).
     *
     * <p>Because the single-key path may return NULL, callers must treat row-ids as nullable
     * (e.g. use null-safe equality when joining on row-id).
     *
     * <p>Used by both normalize (IvmNormalizeUniqKeysMTMV) and delta rewrite (IvmAggDeltaHandler)
     * to ensure row-id derivation is identical.
     */
    public static Expression buildRowIdHash(List<? extends Expression> keyExprs) {
        if (DebugPointUtil.isEnable("IvmUtil.buildRowIdHash.force_collision")) {
            return new LargeIntLiteral(BigInteger.ONE);
        }
        if (keyExprs.isEmpty()) {
            return new TinyIntLiteral((byte) 0);
        }
        // A single key of an eligible MOW key type becomes the row-id directly: no hash means
        // no collision, and the row-id is NULL exactly when the key is NULL. Other types (e.g.
        // string, float) are not legal MOW keys and must be hashed to stay deterministic.
        if (keyExprs.size() == 1 && ColumnDefinition.isEligibleKeyType(keyExprs.get(0).getDataType())) {
            return keyExprs.get(0);
        }
        // For each key, emit two hash arguments:
        //   1. ifnull(cast(key AS VARCHAR), '') — coalesces NULL to '' so hash never receives NULL
        //   2. cast(isnull(key) AS VARCHAR)     — encodes NULL position to distinguish
        //      e.g. (NULL, '') from ('', NULL)
        ImmutableList.Builder<Expression> hashArgs = ImmutableList.builderWithExpectedSize(keyExprs.size() * 2);
        for (Expression key : keyExprs) {
            Expression asVarchar = (key.getDataType() instanceof CharacterType)
                    ? key : new Cast(key, VarcharType.SYSTEM_DEFAULT);
            hashArgs.add(new Nvl(asVarchar, new VarcharLiteral("")));
            hashArgs.add(new Cast(new IsNull(key), VarcharType.SYSTEM_DEFAULT));
        }
        return new MurmurHash3128(hashArgs.build());
    }

    /**
     * Generates a hidden column name for an IVM aggregate state.
     * Format: __DORIS_IVM_AGG_{ordinal}_{stateType}_COL__
     * Example: __DORIS_IVM_AGG_2_SUM_COL__, __DORIS_IVM_AGG_2_COUNT_COL__
     *
     * @param ordinal   the 0-based ordinal of the aggregate target in the MV query
     * @param stateType the state type (SUM, COUNT, etc.)
     */
    public static String ivmAggHiddenColumnName(int ordinal, String stateType) {
        return Column.IVM_HIDDEN_COLUMN_PREFIX + "AGG_" + ordinal + "_" + stateType + "_COL__";
    }

    /**
     * Creates a hidden ColumnDefinition for the IVM row-id column. */
    public static ColumnDefinition newIvmRowIdColumnDefinition(DataType type, boolean isNullable) {
        ColumnDefinition columnDefinition = new ColumnDefinition(
                Column.IVM_ROW_ID_COL, type, false, null, isNullable, Optional.empty(),
                "ivm row id hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);
        return columnDefinition;
    }

    /**
     * Creates a hidden ColumnDefinition for an IVM aggregate state.
     *
     * @param name       the hidden column name (e.g. __DORIS_IVM_AGG_0_SUM_COL__)
     * @param type       the data type of this state column
     * @param isNullable whether this state column can be null
     */
    public static ColumnDefinition newIvmAggHiddenColumnDefinition(String name, DataType type, boolean isNullable) {
        ColumnDefinition columnDefinition = new ColumnDefinition(
                name, type, false, null, isNullable, Optional.empty(),
                "ivm aggregate hidden column", false);
        columnDefinition.setEnableAddHiddenColumn(true);
        return columnDefinition;
    }

    /**
     * Finds the IVM row_id slot in the given output list.
     * Throws IvmException if not found or if multiple row_id slots are present.
     *
     * @param output the plan's output slots
     * @param context description of where this lookup happens (e.g. "left child of join")
     */
    public static Slot findRowIdSlot(List<Slot> output, String context) {
        Slot found = findRowIdSlotOrNull(output);
        if (found == null) {
            throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                    "IVM: no row_id slot found in " + context);
        }
        return found;
    }

    /** IVM stream name prefix for auto-created streams. */
    public static final String IVM_STREAM_PREFIX = "__doris_ivm_stream_";
    private static final int IVM_STREAM_HASH_BYTES = 16;
    private static final int IVM_STREAM_HASH_BASE36_LENGTH = 25;

    /**
     * Computes the deterministic stream name for a base table backing an IVM-enabled MTMV.
     * Format: __doris_ivm_stream_{mvId}_{base36(sha256(catalog,db,table)[0..15])}
     */
    public static String streamName(long mvId, List<String> baseTableFullQualifiers) {
        Preconditions.checkArgument(mvId >= 0, "mvId must be non-negative");
        Preconditions.checkArgument(baseTableFullQualifiers != null && baseTableFullQualifiers.size() == 3,
                "base table full qualifiers must contain catalog, database, and table");
        MessageDigest digest = DigestUtils.getSha256Digest();
        for (String qualifier : baseTableFullQualifiers) {
            Preconditions.checkNotNull(qualifier, "base table qualifier must not be null");
            byte[] bytes = qualifier.getBytes(StandardCharsets.UTF_8);
            digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
            digest.update(bytes);
        }
        byte[] qualifiedTableDigest = Arrays.copyOf(digest.digest(), IVM_STREAM_HASH_BYTES);
        String base36Hash = new BigInteger(1, qualifiedTableDigest).toString(36);
        return IVM_STREAM_PREFIX + mvId + "_"
                + Strings.padStart(base36Hash, IVM_STREAM_HASH_BASE36_LENGTH, '0');
    }

    public static boolean isStreamOwnedBy(BaseTableStream stream, List<String> expectedBaseTableFullQualifiers) {
        return stream.getBaseTableFullQualifiers().equals(expectedBaseTableFullQualifiers);
    }

    public static boolean isIvmStreamUsable(BaseTableStream stream, OlapTable expectedBaseTable) {
        TableIf actualBaseTable = stream.getBaseTableNullable();
        return isStreamOwnedBy(stream, expectedBaseTable.getFullQualifiers())
                && !stream.isDisabled() && !stream.isStale()
                && actualBaseTable != null && actualBaseTable.getId() == expectedBaseTable.getId();
    }

    public static OlapTableStream getIvmStream(MTMV mtmv, OlapTable expectedBaseTable) {
        Database database = (Database) mtmv.getDatabase();
        String streamName = streamName(mtmv.getId(), expectedBaseTable.getFullQualifiers());
        TableIf table = database.getTableNullable(streamName);
        if (!(table instanceof OlapTableStream)) {
            throw new IvmException(IvmFailureReason.STREAM_UNSUPPORTED,
                    "IVM stream not found: " + streamName);
        }
        OlapTableStream stream = (OlapTableStream) table;
        if (!isIvmStreamUsable(stream, expectedBaseTable)) {
            throw new IvmException(IvmFailureReason.STREAM_UNSUPPORTED,
                    "IVM stream is unavailable or references a different base table: " + streamName);
        }
        return stream;
    }

    /**
     * Finds the IVM row_id slot in the given output list, or returns null if not found.
     * Throws IvmException if multiple row_id slots are present.
     */
    public static Slot findRowIdSlotOrNull(List<Slot> output) {
        Slot found = null;
        for (Slot slot : output) {
            if (Column.IVM_ROW_ID_COL.equals(slot.getName())) {
                if (found != null) {
                    throw new IvmException(IvmFailureReason.PLAN_REWRITE_FAILED,
                            "IVM: multiple row_id slots found in plan output");
                }
                found = slot;
            }
        }
        return found;
    }

    /** Whether the refresh reads this base table's changes while it is an excluded trigger table. */
    public static boolean isExcludedTriggerTable(Set<TableNameInfo> excludedTriggerTables, LogicalOlapScan scan) {
        if (excludedTriggerTables.isEmpty()) {
            return false;
        }
        OlapTable table = scan.getTable();
        TableNameInfo tableNameInfo = TableNameInfoUtils.fromTableOrNull(table);
        if (tableNameInfo == null) {
            List<String> qualifier = scan.getQualifier();
            String dbName = qualifier.isEmpty() ? table.getDBName() : qualifier.get(qualifier.size() - 1);
            if (dbName == null) {
                return false;
            }
            String ctlName = qualifier.size() >= 2 ? qualifier.get(qualifier.size() - 2)
                    : InternalCatalog.INTERNAL_CATALOG_NAME;
            tableNameInfo = new TableNameInfo(ctlName, dbName, table.getName());
        }
        return MTMVPartitionUtil.isTableExcluded(excludedTriggerTables, tableNameInfo);
    }

    /**
     * Checks that this base table can produce the changes a refresh reads: it must have row binlog
     * enabled, and it must not be a base table whose own refresh is a partition overwrite. An
     * excluded trigger table produces no delta at all, so it is exempt from the first.
     */
    public static void validateBinlogEnabled(Set<TableNameInfo> excludedTriggerTables, LogicalOlapScan scan) {
        if (isExcludedTriggerTable(excludedTriggerTables, scan)) {
            return;
        }
        OlapTable table = scan.getTable();
        if (!table.getBinlogConfig().isEnableForStreaming()) {
            throw new IvmException(IvmFailureReason.BINLOG_NOT_ENABLED,
                    "SQL can be incrementally refreshed, but row binlog is not enabled for table: "
                            + table.getName()
                            + ". Please set 'binlog.enable' = 'true' and 'binlog.format' = 'ROW'.");
        }
        if (table instanceof MTMV && !((MTMV) table).isIvm()) {
            throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED,
                    "INCREMENTAL materialized view cannot use non-IVM MTMV as a base table: "
                            + table.getName() + ". The base MTMV is refreshed by partition overwrite.");
        }
    }

    /**
     * Rejects an MV output column whose name a Doris hidden column already owns. Both table models lay
     * hidden columns on the MV they build -- the unique-key model its row-id, any model that enables row
     * binlog its own commit tso and row lsn -- so a user column taking one of the reserved prefixes would
     * be indistinguishable from an internal one: it is filtered out of the schema the user reads, leaving
     * an MV that holds fewer columns than its query produced.
     */
    public static void validateUserOutputColumnNames(List<NamedExpression> outputs) {
        for (NamedExpression output : outputs) {
            try {
                FeNameFormat.checkColumnName(output.getName());
            } catch (org.apache.doris.common.AnalysisException e) {
                throw new IvmException(IvmFailureReason.PLAN_PATTERN_UNSUPPORTED, e.getMessage(), e);
            }
        }
    }
}
