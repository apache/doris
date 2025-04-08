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

package org.apache.doris.nereids.hint;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.io.Writable;
import org.apache.doris.nereids.trees.plans.PlaceholderId;
import org.apache.doris.qe.SessionVariable;
import org.apache.doris.qe.VariableMgr;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Outline manager used to manage read write and cached of outline
 */
public class OutlineMgr implements Writable {
    public static final OutlineMgr INSTANCE = new OutlineMgr();
    private static final Logger LOG = LogManager.getLogger(OutlineMgr.class);
    private static final Map<String, OutlineInfo> outlineMap = new HashMap<>();
    private static final Map<String, OutlineInfo> visibleSignatureMap = new HashMap<>();
    private static final HashSet<String> nereidsVars = new HashSet<>(Arrays.asList(
            "DISABLE_NEREIDS_RULES", "ALLOW_MODIFY_MATERIALIZED_VIEW_DATA", "ALLOW_PARTITION_COLUMN_NULLABLE",
            "AUTO_BROADCAST_JOIN_THRESHOLD", "AUTO_INCREMENT_INCREMENT", "AUTO_PROFILE_THRESHOLD_MS",
            "BROADCAST_HASHTABLE_MEM_LIMIT_PERCENTAGE", "BROADCAST_RIGHT_TABLE_SCALE_FACTOR",
            "CBO_CPU_WEIGHT", "CBO_MEM_WEIGHT", "CBO_NET_WEIGHT", "CHECK_OVERFLOW_FOR_DECIMAL",
            "DECIMAL_OVERFLOW_SCALE", "DISABLE_COLOCATE_PLAN", "DISABLE_EMPTY_PARTITION_PRUNE",
            "DISABLE_JOIN_REORDER", "DUMP_NEREIDS_MEMO", "ENABLE_BUCKET_SHUFFLE_JOIN", "ENABLE_BUSHY_TREE",
            "ENABLE_CBO_STATISTICS", "ENABLE_COMMON_EXPR_PUSHDOWN", "ENABLE_COMMON_EXPR_PUSHDOWN_FOR_INVERTED_INDEX",
            "ENABLE_COOLDOWN_REPLICA_AFFINITY", "ENABLE_COST_BASED_JOIN_REORDER", "ENABLE_CTE_MATERIALIZE",
            "ENABLE_DECIMAL256", "ENABLE_DML_MATERIALIZED_VIEW_REWRITE", "BROADCAST_ROW_COUNT_LIMIT",
            "ENABLE_DML_MATERIALIZED_VIEW_REWRITE_WHEN_BASE_TABLE_UNAWARENESS",
            "ENABLE_DPHYP_OPTIMIZER", "ENABLE_DPHYP_TRACE", "ENABLE_ELIMINATE_SORT_NODE", "ENABLE_EXPR_TRACE",
            "ENABLE_EXT_FUNC_PRED_PUSHDOWN", "ENABLE_FAST_ANALYZE_INTO_VALUES", "ENABLE_FOLD_CONSTANT_BY_BE",
            "ENABLE_FOLD_NONDETERMINISTIC_FN", "ENABLE_FUNCTION_PUSHDOWN", "ENABLE_HASH_JOIN_EARLY_START_PROBE",
            "ENABLE_INFER_PREDICATE", "ENABLE_INSERT_STRICT", "ENABLE_INVERTED_INDEX_QUERY",
            "ENABLE_INVERTED_INDEX_QUERY_CACHE", "ENABLE_INVERTED_INDEX_SEARCHER_CACHE", "ENABLE_JOIN_SPILL",
            "ENABLE_LEFT_ZIG_ZAG", "ENABLE_LOCAL_MERGE_SORT", "ENABLE_MATERIALIZED_VIEW_NEST_REWRITE",
            "ENABLE_MATERIALIZED_VIEW_REWRITE", "ENABLE_NEREIDS_RULES", "ENABLE_NEREIDS_TIMEOUT",
            "ENABLE_NEREIDS_TRACE", "ENABLE_NEW_COST_MODEL", "ENABLE_ORDERED_SCAN_RANGE_LOCATIONS",
            "ENABLE_PARTITION_TOPN", "ENABLE_PROFILE", "ENABLE_PUSH_DOWN_NO_GROUP_AGG",
            "ENABLE_PUSHDOWN_MINMAX_ON_UNIQUE", "DEBUG_SKIP_FOLD_CONSTANT",
            "ENABLE_PUSHDOWN_STRING_MINMAX", "ENABLE_QUERY_CACHE", "ENABLE_REWRITE_ELEMENT_AT_TO_SLOT",
            "ENABLE_RUNTIME_FILTER_PARTITION_PRUNE", "ENABLE_RUNTIME_FILTER_PRUNE",
            "ENABLE_SHARED_EXCHANGE_SINK_BUFFER", "ENABLE_SQL_CACHE", "ENABLE_STATS",
            "ENABLE_SYNC_MV_COST_BASED_REWRITE", "ENABLE_VERBOSE_PROFILE", "EXPAND_RUNTIME_FILTER_BY_INNER_JOIN",
            "EXPERIMENTAL_ENABLE_AGG_STATE", "EXPERIMENTAL_ENABLE_COMPRESS_MATERIALIZE",
            "EXPERIMENTAL_ENABLE_LOCAL_SHUFFLE", "ENABLE_SHARE_HASH_TABLE_FOR_BROADCAST_JOIN",
            "EXPERIMENTAL_ENABLE_NEREIDS_DISTRIBUTE_PLANNER", "EXPERIMENTAL_ENABLE_PARALLEL_SCAN",
            "EXPERIMENTAL_ENABLE_SHARED_SCAN", "EXPERIMENTAL_FORCE_TO_LOCAL_SHUFFLE", "FILTER_COST_FACTOR",
            "FORBID_UNKNOWN_COL_STATS", "FORWARD_TO_MASTER", "GLOBAL_PARTITION_TOPN_THRESHOLD",
            "IGNORE_RUNTIME_FILTER_ERROR", "IGNORE_RUNTIME_FILTER_IDS", "INLINE_CTE_REFERENCED_THRESHOLD",
            "INSERT_MAX_FILTER_RATIO", "INSERT_TIMEOUT", "INSERT_VISIBLE_TIMEOUT_MS",
            "INTERACTIVE_TIMEOUT", "JOIN_ORDER_TIME_LIMIT", "LEFT_SEMI_OR_ANTI_PROBE_FACTOR",
            "MATERIALIZED_VIEW_RELATION_MAPPING_MAX_COUNT", "MATERIALIZED_VIEW_REWRITE_ENABLE_CONTAIN_EXTERNAL_TABLE",
            "MATERIALIZED_VIEW_REWRITE_SUCCESS_CANDIDATE_NUM", "MAX_EXECUTION_TIME",
            "MAX_JOIN_NUMBER_BUSHY_TREE", "MAX_JOIN_NUMBER_OF_REORDER", "MAX_PUSHDOWN_CONDITIONS_PER_COLUMN",
            "MAX_TABLE_COUNT_USE_CASCADES_JOIN_REORDER", "MEMO_MAX_GROUP_EXPRESSION_SIZE",
            "NEREIDS_CBO_PENALTY_FACTOR", "NEREIDS_STAR_SCHEMA_SUPPORT", "NEREIDS_TIMEOUT_SECOND",
            "NEREIDS_TRACE_EVENT_MODE", "NTH_OPTIMIZED_PLAN", "PARTITION_PRUNING_EXPAND_THRESHOLD",
            "PLAN_NEREIDS_DUMP", "PREFER_JOIN_METHOD", "PROFILE_LEVEL", "PROFILING", "PUSH_TOPN_TO_AGG",
            "QUERY_CACHE_ENTRY_MAX_BYTES", "QUERY_CACHE_ENTRY_MAX_ROWS", "QUERY_CACHE_FORCE_REFRESH",
            "QUERY_CACHE_TYPE", "QUERY_TIMEOUT", "REQUIRE_SEQUENCE_IN_INSERT", "REWRITE_COUNT_DISTINCT_TO_BITMAP_HLL",
            "REWRITE_OR_TO_IN_PREDICATE_THRESHOLD", "ROUND_PRECISE_DECIMALV2_VALUE", "RUNTIME_BLOOM_FILTER_MAX_SIZE",
            "RUNTIME_BLOOM_FILTER_MIN_SIZE", "RUNTIME_BLOOM_FILTER_SIZE", "RUNTIME_FILTER_JUMP_THRESHOLD",
            "RUNTIME_FILTER_MAX_IN_NUM", "RUNTIME_FILTER_MODE", "RUNTIME_FILTER_PRUNE_FOR_EXTERNAL",
            "RUNTIME_FILTER_TYPE", "RUNTIME_FILTER_WAIT_INFINITELY", "RUNTIME_FILTER_WAIT_TIME_MS",
            "RUNTIME_FILTERS_MAX_NUM", "SHOW_ALL_FE_CONNECTION", "SQL_DIALECT", "SQL_SELECT_LIMIT",
            "STORAGE_ENGINE", "TIME_ZONE", "TOPN_FILTER_RATIO", "TOPN_OPT_LIMIT_THRESHOLD",
            "TRACE_NEREIDS", "USE_MAX_LENGTH_OF_VARCHAR_IN_CTAS", "USE_RF_DEFAULT", "WAIT_TIMEOUT"));

    private static ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private static void writeLock() {
        lock.writeLock().lock();
    }

    private static void writeUnlock() {
        lock.writeLock().unlock();
    }

    public static Optional<OutlineInfo> getOutline(String outlineName) {
        if (outlineMap.containsKey(outlineName)) {
            return Optional.of(outlineMap.get(outlineName));
        }
        return Optional.empty();
    }

    public static Map<String, OutlineInfo> getOutlineMap() {
        return outlineMap;
    }

    public static Optional<OutlineInfo> getOutlineByVisibleSignature(String visibleSignature) {
        if (visibleSignatureMap.containsKey(visibleSignature)) {
            return Optional.of(visibleSignatureMap.get(visibleSignature));
        }
        return Optional.empty();
    }

    public static Map<String, OutlineInfo> getVisibleSignatureMap() {
        return visibleSignatureMap;
    }

    /**
     * replace constant by place holder
     * @param originalQuery original query input by create outline command
     * @param constantMap constant map collected by logicalPlanBuilder
     * @param startIndex a shift of create outline command
     * @return query replace constant by place holder
     */
    public static String replaceConstant(String originalQuery, Map<PlaceholderId,
            Pair<Integer, Integer>> constantMap, int startIndex) {
        List<Pair<Integer, Integer>> sortedKeys = new ArrayList<>(constantMap.values());

        // Sort by start index in descending order to avoid shifting problems
        sortedKeys.sort((a, b) -> b.first.compareTo(a.first));

        StringBuilder sb = new StringBuilder(originalQuery);

        for (Pair<Integer, Integer> range : sortedKeys) {
            if (range.first == 0 && range.second == 0) {
                continue;
            }
            int start = range.first - startIndex;
            int end = range.second - startIndex + 1;
            sb.replace(start, end, "?");
        }

        return sb.toString();
    }

    /**
     * create outline data which include some hints
     * @param sessionVariable sessionVariables used to generate corresponding plan
     * @return string include many hints
     */
    public static String createOutlineData(SessionVariable sessionVariable) {
        StringBuilder sb = new StringBuilder();
        sb.append("/*+ ");
        // add set_var hint
        List<List<String>> changedVars = VariableMgr.dumpNereidsVars(sessionVariable, nereidsVars);
        if (!changedVars.isEmpty()) {
            sb.append("set_var(");
            for (List<String> changedVar : changedVars) {
                sb.append(changedVar.get(0));
                sb.append("=");
                sb.append("\"");
                sb.append(changedVar.get(1));
                sb.append("\"");
                sb.append("\t");
            }
            sb.append(") ");
        }
        sb.append("*/ ");
        return sb.toString();
    }

    /**
     * createOutlineInternal
     * @param outlineInfo outline info used to create outline
     * @param ignoreIfExists if we add or replace to create outline statement, it would be true
     * @param isReplay when it is replay mode, editlog would not be written
     * @throws DdlException should throw exception when meeting problem
     */
    public static void createOutlineInternal(OutlineInfo outlineInfo, boolean ignoreIfExists, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            if (!ignoreIfExists && OutlineMgr.getOutline(outlineInfo.getOutlineName()).isPresent()) {
                LOG.info("outline already exists, ignored to create outline: {}, is replay: {}",
                        outlineInfo.getOutlineName(), isReplay);
                throw new DdlException(outlineInfo.getOutlineName() + " already exists");
            }

            if (OutlineMgr.getOutlineByVisibleSignature(outlineInfo.getVisibleSignature()).isPresent()) {
                if (!ignoreIfExists) {
                    LOG.info("outline already exists, ignored to create outline with signature: {}, is replay: {}",
                            outlineInfo.getVisibleSignature(), isReplay);
                    throw new DdlException(outlineInfo.getVisibleSignature() + " already exists");
                } else {
                    dropOutline(outlineInfo);
                }
            }

            createOutline(outlineInfo);
            if (!isReplay) {
                Env.getCurrentEnv().getEditLog().logCreateOutline(outlineInfo);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to create outline: {}, is replay: {}", outlineInfo.getOutlineName(), isReplay);
    }

    private static void createOutline(OutlineInfo outlineInfo) {
        outlineMap.put(outlineInfo.getOutlineName(), outlineInfo);
        visibleSignatureMap.put(outlineInfo.getVisibleSignature(), outlineInfo);
    }

    /**
     * createOutlineInternal
     * @param outlineName outline info used to create outline
     * @param ifExists if we add if exists to create outline statement, it would be true
     * @param isReplay when it is replay mode, editlog would not be written
     * @throws DdlException should throw exception when meeting problem
     */
    public static void dropOutlineInternal(String outlineName, boolean ifExists, boolean isReplay)
            throws DdlException {
        writeLock();
        try {
            boolean isPresent = outlineMap.containsKey(outlineName);
            if (!isPresent) {
                LOG.info("outline not exists, ignored to drop outline: {}, is replay: {}",
                        outlineName, isReplay);
                if (!ifExists) {
                    throw new DdlException(outlineName + " not exists");
                } else {
                    return;
                }
            }

            OutlineInfo outlineInfo = outlineMap.get(outlineName);
            dropOutline(outlineInfo);
            if (!isReplay && isPresent) {
                Env.getCurrentEnv().getEditLog().logDropOutline(outlineInfo);
            }
        } finally {
            writeUnlock();
        }
        LOG.info("finished to create outline: {}, is replay: {}", outlineName, isReplay);
    }

    private static void dropOutline(OutlineInfo outlineInfo) {
        outlineMap.remove(outlineInfo.getOutlineName());
        visibleSignatureMap.remove(outlineInfo.getVisibleSignature());
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(outlineMap.size());
        for (OutlineInfo outlineInfo : outlineMap.values()) {
            outlineInfo.write(out);
        }
    }

    /**
     * read fields from disk
     * @param in data source of disk
     * @throws IOException maybe throw ioexception
     */
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            OutlineInfo outlineInfo = OutlineInfo.read(in);
            outlineMap.put(outlineInfo.getOutlineName(), outlineInfo);
            visibleSignatureMap.put(outlineInfo.getVisibleSignature(), outlineInfo);
        }
    }
}
