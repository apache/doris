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

package org.apache.doris.qe;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.qe.VariableMgr.VarAttr;
import org.apache.doris.thrift.TQueryOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;

// System variable
public class SessionVariable implements Serializable, Writable {
    
    static final Logger LOG = LogManager.getLogger(StmtExecutor.class);
    public static final String EXEC_MEM_LIMIT = "exec_mem_limit";
    public static final String QUERY_TIMEOUT = "query_timeout";
    public static final String IS_REPORT_SUCCESS = "is_report_success";
    public static final String SQL_MODE = "sql_mode";
    public static final String RESOURCE_VARIABLE = "resource_group";
    public static final String AUTO_COMMIT = "autocommit";
    public static final String TX_ISOLATION = "tx_isolation";
    public static final String CHARACTER_SET_CLIENT = "character_set_client";
    public static final String CHARACTER_SET_CONNNECTION = "character_set_connection";
    public static final String CHARACTER_SET_RESULTS = "character_set_results";
    public static final String CHARACTER_SET_SERVER = "character_set_server";
    public static final String COLLATION_CONNECTION = "collation_connection";
    public static final String COLLATION_DATABASE = "collation_database";
    public static final String COLLATION_SERVER = "collation_server";
    public static final String SQL_AUTO_IS_NULL = "SQL_AUTO_IS_NULL";
    public static final String SQL_SELECT_LIMIT = "sql_select_limit";
    public static final String MAX_ALLOWED_PACKET = "max_allowed_packet";
    public static final String AUTO_INCREMENT_INCREMENT = "auto_increment_increment";
    public static final String QUERY_CACHE_TYPE = "query_cache_type";
    public static final String INTERACTIVE_TIMTOUT = "interactive_timeout";
    public static final String WAIT_TIMEOUT = "wait_timeout";
    public static final String NET_WRITE_TIMEOUT = "net_write_timeout";
    public static final String NET_READ_TIMEOUT = "net_read_timeout";
    public static final String TIME_ZONE = "time_zone";
    public static final String SQL_SAFE_UPDATES = "sql_safe_updates";
    public static final String NET_BUFFER_LENGTH = "net_buffer_length";
    public static final String CODEGEN_LEVEL = "codegen_level";
    // mem limit can't smaller than bufferpool's default page size
    public static final int MIN_EXEC_MEM_LIMIT = 2097152;   
    public static final String BATCH_SIZE = "batch_size";
    public static final String DISABLE_STREAMING_PREAGGREGATIONS = "disable_streaming_preaggregations";
    public static final String DISABLE_COLOCATE_JOIN = "disable_colocate_join";
    public static final String PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM = "parallel_fragment_exec_instance_num";
    public static final String ENABLE_INSERT_STRICT = "enable_insert_strict";
    public static final String ENABLE_SPILLING = "enable_spilling";
    public static final int MIN_EXEC_INSTANCE_NUM = 1;
    public static final int MAX_EXEC_INSTANCE_NUM = 32;
    // if set to true, some of stmt will be forwarded to master FE to get result
    public static final String FORWARD_TO_MASTER = "forward_to_master";
    // user can set instance num after exchange, no need to be equal to nums of before exchange
    public static final String PARALLEL_EXCHANGE_INSTANCE_NUM = "parallel_exchange_instance_num";
    /*
     * configure the mem limit of load process on BE. 
     * Previously users used exec_mem_limit to set memory limits.
     * To maintain compatibility, the default value of load_mem_limit is 0,
     * which means that the load memory limit is still using exec_mem_limit.
     * Users can set a value greater than zero to explicitly specify the load memory limit.
     * This variable is mainly for INSERT operation, because INSERT operation has both query and load part.
     * Using only the exec_mem_limit variable does not make a good distinction of memory limit between the two parts.
     */
    public static final String LOAD_MEM_LIMIT = "load_mem_limit";
    public static final String USE_V2_ROLLUP = "use_v2_rollup";
    public static final String TEST_MATERIALIZED_VIEW = "test_materialized_view";
    public static final String REWRITE_COUNT_DISTINCT_TO_BITMAP_HLL = "rewrite_count_distinct_to_bitmap_hll";
    public static final String EVENT_SCHEDULER = "event_scheduler";
    public static final String STORAGE_ENGINE = "storage_engine";
    public static final String DIV_PRECISION_INCREMENT = "div_precision_increment";

    // see comment of `doris_max_scan_key_num` and `max_pushdown_conditions_per_column` in BE config
    public static final String MAX_SCAN_KEY_NUM = "max_scan_key_num";
    public static final String MAX_PUSHDOWN_CONDITIONS_PER_COLUMN = "max_pushdown_conditions_per_column";

    // max memory used on every backend.
    @VariableMgr.VarAttr(name = EXEC_MEM_LIMIT)
    public long maxExecMemByte = 2147483648L;

    @VariableMgr.VarAttr(name = ENABLE_SPILLING)
    public boolean enableSpilling = false;

    // query timeout in second.
    @VariableMgr.VarAttr(name = QUERY_TIMEOUT)
    private int queryTimeoutS = 300;

    // if true, need report to coordinator when plan fragment execute successfully.
    @VariableMgr.VarAttr(name = IS_REPORT_SUCCESS)
    private boolean isReportSucc = false;

    // Set sqlMode to empty string
    @VariableMgr.VarAttr(name = SQL_MODE)
    private long sqlMode = 0L;

    @VariableMgr.VarAttr(name = RESOURCE_VARIABLE)
    private String resourceGroup = "normal";

    // this is used to make mysql client happy
    @VariableMgr.VarAttr(name = AUTO_COMMIT)
    private boolean autoCommit = true;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = TX_ISOLATION)
    private String txIsolation = "REPEATABLE-READ";

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = CHARACTER_SET_CLIENT)
    private String charsetClient = "utf8";
    @VariableMgr.VarAttr(name = CHARACTER_SET_CONNNECTION)
    private String charsetConnection = "utf8";
    @VariableMgr.VarAttr(name = CHARACTER_SET_RESULTS)
    private String charsetResults = "utf8";
    @VariableMgr.VarAttr(name = CHARACTER_SET_SERVER)
    private String charsetServer = "utf8";
    @VariableMgr.VarAttr(name = COLLATION_CONNECTION)
    private String collationConnection = "utf8_general_ci";
    @VariableMgr.VarAttr(name = COLLATION_DATABASE)
    private String collationDatabase = "utf8_general_ci";

    @VariableMgr.VarAttr(name = COLLATION_SERVER)
    private String collationServer = "utf8_general_ci";

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = SQL_AUTO_IS_NULL)
    private boolean sqlAutoIsNull = false;

    @VariableMgr.VarAttr(name = SQL_SELECT_LIMIT)
    private long sqlSelectLimit = 9223372036854775807L;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = MAX_ALLOWED_PACKET)
    private int maxAllowedPacket = 1048576;

    @VariableMgr.VarAttr(name = AUTO_INCREMENT_INCREMENT)
    private int autoIncrementIncrement = 1;

    // this is used to make c3p0 library happy
    @VariableMgr.VarAttr(name = QUERY_CACHE_TYPE)
    private int queryCacheType = 0;

    // The number of seconds the server waits for activity on an interactive connection before closing it
    @VariableMgr.VarAttr(name = INTERACTIVE_TIMTOUT)
    private int interactiveTimeout = 3600;

    // The number of seconds the server waits for activity on a noninteractive connection before closing it.
    @VariableMgr.VarAttr(name = WAIT_TIMEOUT)
    private int waitTimeout = 28800;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    @VariableMgr.VarAttr(name = NET_WRITE_TIMEOUT)
    private int netWriteTimeout = 60;

    // The number of seconds to wait for a block to be written to a connection before aborting the write
    @VariableMgr.VarAttr(name = NET_READ_TIMEOUT)
    private int netReadTimeout = 60;

    // The current time zone
    @VariableMgr.VarAttr(name = TIME_ZONE)
    private String timeZone = TimeUtils.getSystemTimeZone().getID();

    @VariableMgr.VarAttr(name = PARALLEL_EXCHANGE_INSTANCE_NUM)
    private int exchangeInstanceParallel = -1;

    @VariableMgr.VarAttr(name = SQL_SAFE_UPDATES)
    private int sqlSafeUpdates = 0;

    // only
    @VariableMgr.VarAttr(name = NET_BUFFER_LENGTH, flag = VariableMgr.READ_ONLY)
    private int netBufferLength = 16384;

    // if true, need report to coordinator when plan fragment execute successfully.
    @VariableMgr.VarAttr(name = CODEGEN_LEVEL)
    private int codegenLevel = 0;

    @VariableMgr.VarAttr(name = BATCH_SIZE)
    private int batchSize = 1024;

    @VariableMgr.VarAttr(name = DISABLE_STREAMING_PREAGGREGATIONS)
    private boolean disableStreamPreaggregations = false;

    @VariableMgr.VarAttr(name = DISABLE_COLOCATE_JOIN)
    private boolean disableColocateJoin = false;

    /*
     * the parallel exec instance num for one Fragment in one BE
     * 1 means disable this feature
     */
    @VariableMgr.VarAttr(name = PARALLEL_FRAGMENT_EXEC_INSTANCE_NUM)
    private int parallelExecInstanceNum = 1;

    @VariableMgr.VarAttr(name = ENABLE_INSERT_STRICT)
    private boolean enableInsertStrict = false;

    @VariableMgr.VarAttr(name = FORWARD_TO_MASTER)
    private boolean forwardToMaster = false;

    @VariableMgr.VarAttr(name = LOAD_MEM_LIMIT)
    private long loadMemLimit = 0L;

    @VariableMgr.VarAttr(name = USE_V2_ROLLUP)
    private boolean useV2Rollup = false;

    // TODO(ml): remove it after test
    @VariableMgr.VarAttr(name = TEST_MATERIALIZED_VIEW)
    private boolean testMaterializedView = false;

    @VariableMgr.VarAttr(name = REWRITE_COUNT_DISTINCT_TO_BITMAP_HLL)
    private boolean rewriteCountDistinct = true;

    // compatible with some mysql client connect, say DataGrip of JetBrains
    @VariableMgr.VarAttr(name = EVENT_SCHEDULER)
    private String eventScheduler = "OFF";
    @VariableMgr.VarAttr(name = STORAGE_ENGINE)
    private String storageEngine = "olap";
    @VariableMgr.VarAttr(name = DIV_PRECISION_INCREMENT)
    private int divPrecisionIncrement = 4;

    // -1 means unset, BE will use its config value
    @VariableMgr.VarAttr(name = MAX_SCAN_KEY_NUM)
    private int maxScanKeyNum = -1;
    @VariableMgr.VarAttr(name = MAX_PUSHDOWN_CONDITIONS_PER_COLUMN)
    private int maxPushdownConditionsPerColumn = -1;

    public long getMaxExecMemByte() {
        return maxExecMemByte;
    }

    public long getLoadMemLimit() {
        return loadMemLimit;
    }

    public int getQueryTimeoutS() {
        return queryTimeoutS;
    }

    public boolean isReportSucc() {
        return isReportSucc;
    }

    public int getWaitTimeoutS() {
        return waitTimeout;
    }

    public long getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(long sqlMode) {
        this.sqlMode = sqlMode;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public String getTxIsolation() {
        return txIsolation;
    }

    public String getCharsetClient() {
        return charsetClient;
    }

    public String getCharsetConnection() {
        return charsetConnection;
    }

    public String getCharsetResults() {
        return charsetResults;
    }

    public String getCharsetServer() {
        return charsetServer;
    }

    public String getCollationConnection() {
        return collationConnection;
    }

    public String getCollationDatabase() {
        return collationDatabase;
    }

    public String getCollationServer() {
        return collationServer;
    }

    public boolean isSqlAutoIsNull() {
        return sqlAutoIsNull;
    }

    public long getSqlSelectLimit() {
        return sqlSelectLimit;
    }

    public int getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    public int getAutoIncrementIncrement() {
        return autoIncrementIncrement;
    }

    public int getQueryCacheType() {
        return queryCacheType;
    }

    public int getInteractiveTimeout() {
        return interactiveTimeout;
    }

    public int getWaitTimeout() {
        return waitTimeout;
    }

    public int getNetWriteTimeout() {
        return netWriteTimeout;
    }

    public int getNetReadTimeout() {
        return netReadTimeout;
    }

    public String getTimeZone() {
        return timeZone;
    }

    public void setTimeZone(String timeZone) {
        this.timeZone = timeZone;
    }

    public int getSqlSafeUpdates() {
        return sqlSafeUpdates;
    }

    public int getNetBufferLength() {
        return netBufferLength;
    }

    public int getCodegenLevel() {
        return codegenLevel;
    }

    public void setMaxExecMemByte(long maxExecMemByte) {
        if (maxExecMemByte < MIN_EXEC_MEM_LIMIT) {
            this.maxExecMemByte = MIN_EXEC_MEM_LIMIT;
        } else {
            this.maxExecMemByte = maxExecMemByte;
        }
    }

    public void setLoadMemLimit(long loadMemLimit) {
        this.loadMemLimit = loadMemLimit;
    }

    public void setQueryTimeoutS(int queryTimeoutS) {
        this.queryTimeoutS = queryTimeoutS;
    }

    public String getResourceGroup() {
        return resourceGroup;
    }

    public void setResourceGroup(String resourceGroup) {
        this.resourceGroup = resourceGroup;
    }

    public boolean isDisableColocateJoin() {
        return disableColocateJoin;
    }

    public int getParallelExecInstanceNum() {
        return parallelExecInstanceNum;
    }

    public int getExchangeInstanceParallel() {
        return exchangeInstanceParallel;
    }

    public boolean getEnableInsertStrict() { return enableInsertStrict; }

    public void setEnableInsertStrict(boolean enableInsertStrict) {
        this.enableInsertStrict = enableInsertStrict;
    }

    public boolean getForwardToMaster() {
        return forwardToMaster;
    }

    public boolean isUseV2Rollup() { return useV2Rollup; }

    // for unit test
    public void setUseV2Rollup(boolean useV2Rollup) {
        this.useV2Rollup = useV2Rollup;
    }

    public boolean getTestMaterializedView() {
        return this.testMaterializedView;
    }

    public void setTestMaterializedView(boolean testMaterializedView) {
        this.testMaterializedView = testMaterializedView;
    }

    public boolean isRewriteCountDistinct() {
        return rewriteCountDistinct;
    }

    public void setRewriteCountDistinct(boolean rewriteCountDistinct) {
        this.rewriteCountDistinct = rewriteCountDistinct;
    }

    public String getEventScheduler() {
        return eventScheduler;
    }

    public void setEventScheduler(String eventScheduler) {
        this.eventScheduler = eventScheduler;
    }

    public String getStorageEngine() {
        return storageEngine;
    }

    public void setStorageEngine(String storageEngine) {
        this.storageEngine = storageEngine;
    }

    public int getDivPrecisionIncrement() {
        return divPrecisionIncrement;
    }

    public int getMaxScanKeyNum() {
        return maxScanKeyNum;
    }

    public void setMaxScanKeyNum(int maxScanKeyNum) {
        this.maxScanKeyNum = maxScanKeyNum;
    }

    public int getMaxPushdownConditionsPerColumn() {
        return maxPushdownConditionsPerColumn;
    }

    public void setMaxPushdownConditionsPerColumn(int maxPushdownConditionsPerColumn) {
        this.maxPushdownConditionsPerColumn = maxPushdownConditionsPerColumn;
    }

    // Serialize to thrift object
    // used for rest api
    public TQueryOptions toThrift() {
        TQueryOptions tResult = new TQueryOptions();
        tResult.setMem_limit(maxExecMemByte);

        // TODO chenhao, reservation will be calculated by cost
        tResult.setMin_reservation(0);
        tResult.setMax_reservation(maxExecMemByte);
        tResult.setInitial_reservation_total_claims(maxExecMemByte);
        tResult.setBuffer_pool_limit(maxExecMemByte);

        tResult.setQuery_timeout(queryTimeoutS);
        tResult.setIs_report_success(isReportSucc);
        tResult.setCodegen_level(codegenLevel);

        tResult.setBatch_size(batchSize);
        tResult.setDisable_stream_preaggregations(disableStreamPreaggregations);
        tResult.setLoad_mem_limit(loadMemLimit);

        if (maxScanKeyNum > -1) {
            tResult.setMax_scan_key_num(maxScanKeyNum);
        }
        if (maxPushdownConditionsPerColumn > -1) {
            tResult.setMax_pushdown_conditions_per_column(maxPushdownConditionsPerColumn);
        }
        tResult.setEnable_spilling(enableSpilling);
        return tResult;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        JSONObject root = new JSONObject();
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }
                switch (field.getType().getSimpleName()) {
                    case "boolean":
                        root.put(attr.name(), (Boolean) field.get(this));
                        break;
                    case "int":
                        root.put(attr.name(), (Integer) field.get(this));
                        break;
                    case "long":
                        root.put(attr.name(), (Long) field.get(this));
                        break;
                    case "float":
                        root.put(attr.name(), (Float) field.get(this));
                        break;
                    case "double":
                        root.put(attr.name(), (Double) field.get(this));
                        break;
                    case "String":
                        root.put(attr.name(), (String) field.get(this));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + field.getType().getSimpleName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to write session variable: " + e.getMessage());
        }
        Text.writeString(out, root.toString());
    }

    public void readFields(DataInput in) throws IOException {
        if (Catalog.getCurrentCatalogJournalVersion() < FeMetaVersion.VERSION_67) {
            codegenLevel = in.readInt();
            netBufferLength = in.readInt();
            sqlSafeUpdates = in.readInt();
            timeZone = Text.readString(in);
            netReadTimeout = in.readInt();
            netWriteTimeout = in.readInt();
            waitTimeout = in.readInt();
            interactiveTimeout = in.readInt();
            queryCacheType = in.readInt();
            autoIncrementIncrement = in.readInt();
            maxAllowedPacket = in.readInt();
            sqlSelectLimit = in.readLong();
            sqlAutoIsNull = in.readBoolean();
            collationDatabase = Text.readString(in);
            collationConnection = Text.readString(in);
            charsetServer = Text.readString(in);
            charsetResults = Text.readString(in);
            charsetConnection = Text.readString(in);
            charsetClient = Text.readString(in);
            txIsolation = Text.readString(in);
            autoCommit = in.readBoolean();
            resourceGroup = Text.readString(in);
            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_65) {
                sqlMode = in.readLong();
            } else {
                // read old version SQL mode
                Text.readString(in);
                sqlMode = 0L;
            }
            isReportSucc = in.readBoolean();
            queryTimeoutS = in.readInt();
            maxExecMemByte = in.readLong();
            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_37) {
                collationServer = Text.readString(in);
            }
            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_38) {
                batchSize = in.readInt();
                disableStreamPreaggregations = in.readBoolean();
                parallelExecInstanceNum = in.readInt();
            }
            if (Catalog.getCurrentCatalogJournalVersion() >= FeMetaVersion.VERSION_62) {
                exchangeInstanceParallel = in.readInt();
            }
        } else {
            readFromJson(in);
        }
    }

    private void readFromJson(DataInput in) throws IOException {
        String json = Text.readString(in);
        JSONObject root = new JSONObject(json);
        try {
            for (Field field : SessionVariable.class.getDeclaredFields()) {
                VarAttr attr = field.getAnnotation(VarAttr.class);
                if (attr == null) {
                    continue;
                }

                if (!root.has(attr.name())) {
                    continue;
                }

                switch (field.getType().getSimpleName()) {
                    case "boolean":
                        field.set(this, root.getBoolean(attr.name()));
                        break;
                    case "int":
                        field.set(this, root.getInt(attr.name()));
                        break;
                    case "long":
                        field.set(this, root.getLong(attr.name()));
                        break;
                    case "float":
                        field.set(this, root.getFloat(attr.name()));
                        break;
                    case "double":
                        field.set(this, root.getDouble(attr.name()));
                        break;
                    case "String":
                        field.set(this, root.getString(attr.name()));
                        break;
                    default:
                        // Unsupported type variable.
                        throw new IOException("invalid type: " + field.getType().getSimpleName());
                }
            }
        } catch (Exception e) {
            throw new IOException("failed to read session variable: " + e.getMessage());
        }
    }
}
