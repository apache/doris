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
import org.apache.doris.thrift.TQueryOptions;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.time.ZoneId;

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
    public static final int MIN_EXEC_INSTANCE_NUM = 1;
    public static final int MAX_EXEC_INSTANCE_NUM = 32;
    // if set to true, some of stmt will be forwarded to master FE to get result
    public static final String FORWARD_TO_MASTER = "forward_to_master";

    // max memory used on every backend.
    @VariableMgr.VarAttr(name = EXEC_MEM_LIMIT)
    public long maxExecMemByte = 2147483648L;

    @VariableMgr.VarAttr(name = "enable_spilling")
    public boolean enableSpilling = false;

    // query timeout in second.
    @VariableMgr.VarAttr(name = QUERY_TIMEOUT)
    private int queryTimeoutS = 300;

    // if true, need report to coordinator when plan fragment execute successfully.
    @VariableMgr.VarAttr(name = IS_REPORT_SUCCESS)
    private boolean isReportSucc = false;

    @VariableMgr.VarAttr(name = SQL_MODE)
    private String sqlMode = "";

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
    private String timeZone = ZoneId.systemDefault().normalized().toString();

    // The current time zone
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

    public long getMaxExecMemByte() {
        return maxExecMemByte;
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

    public String getSqlMode() {
        return sqlMode;
    }

    public void setSqlMode(String sqlMode) {
        this.sqlMode = sqlMode;
    }

    public boolean isAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public String getTxIsolation() {
        return txIsolation;
    }

    public void setTxIsolation(String txIsolation) {
        this.txIsolation = txIsolation;
    }

    public String getCharsetClient() {
        return charsetClient;
    }

    public void setCharsetClient(String charsetClient) {
        this.charsetClient = charsetClient;
    }

    public String getCharsetConnection() {
        return charsetConnection;
    }

    public void setCharsetConnection(String charsetConnection) {
        this.charsetConnection = charsetConnection;
    }

    public String getCharsetResults() {
        return charsetResults;
    }

    public void setCharsetResults(String charsetResults) {
        this.charsetResults = charsetResults;
    }

    public String getCharsetServer() {
        return charsetServer;
    }

    public void setCharsetServer(String charsetServer) {
        this.charsetServer = charsetServer;
    }

    public String getCollationConnection() {
        return collationConnection;
    }

    public void setCollationConnection(String collationConnection) {
        this.collationConnection = collationConnection;
    }

    public String getCollationDatabase() {
        return collationDatabase;
    }

    public void setCollationDatabase(String collationDatabase) {
        this.collationDatabase = collationDatabase;
    }

    public String getCollationServer() {
        return collationServer;
    }

    public void setCollationServer(String collationServer) {
        this.collationServer = collationServer;
    }

    public boolean isSqlAutoIsNull() {
        return sqlAutoIsNull;
    }

    public void setSqlAutoIsNull(boolean sqlAutoIsNull) {
        this.sqlAutoIsNull = sqlAutoIsNull;
    }

    public long getSqlSelectLimit() {
        return sqlSelectLimit;
    }

    public void setSqlSelectLimit(long sqlSelectLimit) {
        this.sqlSelectLimit = sqlSelectLimit;
    }

    public int getMaxAllowedPacket() {
        return maxAllowedPacket;
    }

    public void setMaxAllowedPacket(int maxAllowedPacket) {
        this.maxAllowedPacket = maxAllowedPacket;
    }

    public int getAutoIncrementIncrement() {
        return autoIncrementIncrement;
    }

    public void setAutoIncrementIncrement(int autoIncrementIncrement) {
        this.autoIncrementIncrement = autoIncrementIncrement;
    }

    public int getQueryCacheType() {
        return queryCacheType;
    }

    public void setQueryCacheType(int queryCacheType) {
        this.queryCacheType = queryCacheType;
    }

    public int getInteractiveTimeout() {
        return interactiveTimeout;
    }

    public void setInteractiveTimeout(int interactiveTimeout) {
        this.interactiveTimeout = interactiveTimeout;
    }

    public int getWaitTimeout() {
        return waitTimeout;
    }

    public void setWaitTimeout(int waitTimeout) {
        this.waitTimeout = waitTimeout;
    }

    public int getNetWriteTimeout() {
        return netWriteTimeout;
    }

    public void setNetWriteTimeout(int netWriteTimeout) {
        this.netWriteTimeout = netWriteTimeout;
    }

    public int getNetReadTimeout() {
        return netReadTimeout;
    }

    public void setNetReadTimeout(int netReadTimeout) {
        this.netReadTimeout = netReadTimeout;
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

    public void setSqlSafeUpdates(int sqlSafeUpdates) {
        this.sqlSafeUpdates = sqlSafeUpdates;
    }

    public int getNetBufferLength() {
        return netBufferLength;
    }

    public void setNetBufferLength(int netBufferLength) {
        this.netBufferLength = netBufferLength;
    }

    public int getCodegenLevel() {
        return codegenLevel;
    }

    public void setCodegenLevel(int codegenLevel) {
        this.codegenLevel = codegenLevel;
    }

    public void setMaxExecMemByte(long maxExecMemByte) {
        if (maxExecMemByte < MIN_EXEC_MEM_LIMIT) {
            this.maxExecMemByte = MIN_EXEC_MEM_LIMIT;
        } else {
            this.maxExecMemByte = maxExecMemByte;
        }
    }

    public void setQueryTimeoutS(int queryTimeoutS) {
        this.queryTimeoutS = queryTimeoutS;
    }

    public void setReportSucc(boolean isReportSucc) {
        this.isReportSucc = isReportSucc;
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

    public void setDisableColocateJoin(boolean disableColocateJoin) {
        this.disableColocateJoin = disableColocateJoin;
    }

    public int getParallelExecInstanceNum() {
        return parallelExecInstanceNum;
    }

    public void setParallelExecInstanceNum(int parallelExecInstanceNum) {
        if (parallelExecInstanceNum < MIN_EXEC_INSTANCE_NUM) {
            this.parallelExecInstanceNum = MIN_EXEC_INSTANCE_NUM;
        } else if (parallelExecInstanceNum > MAX_EXEC_INSTANCE_NUM) {
            this.parallelExecInstanceNum = MAX_EXEC_INSTANCE_NUM;
        } else {
            this.parallelExecInstanceNum = parallelExecInstanceNum;
        }
    }

    public boolean getEnableInsertStrict() { return enableInsertStrict; }
    public void setEnableInsertStrict(boolean enableInsertStrict) { this.enableInsertStrict = enableInsertStrict; }

    
   // Serialize to thrift object
    public boolean getForwardToMaster() {
        return forwardToMaster;
    }

    public void setForwardToMaster(boolean forwardToMaster) {
        this.forwardToMaster = forwardToMaster;
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
        return tResult;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(codegenLevel);
        out.writeInt(netBufferLength);
        out.writeInt(sqlSafeUpdates);
        Text.writeString(out, timeZone);
        out.writeInt(netReadTimeout);
        out.writeInt(netWriteTimeout);
        out.writeInt(waitTimeout);
        out.writeInt(interactiveTimeout);
        out.writeInt(queryCacheType);
        out.writeInt(autoIncrementIncrement);
        out.writeInt(maxAllowedPacket);
        out.writeLong(sqlSelectLimit);
        out.writeBoolean(sqlAutoIsNull);
        Text.writeString(out, collationDatabase);
        Text.writeString(out, collationConnection);
        Text.writeString(out, charsetServer);
        Text.writeString(out, charsetResults);
        Text.writeString(out, charsetConnection);
        Text.writeString(out, charsetClient);
        Text.writeString(out, txIsolation);
        out.writeBoolean(autoCommit);
        Text.writeString(out, resourceGroup);
        Text.writeString(out, sqlMode);
        out.writeBoolean(isReportSucc);
        out.writeInt(queryTimeoutS);
        out.writeLong(maxExecMemByte);
        Text.writeString(out, collationServer);
        out.writeInt(batchSize);
        out.writeBoolean(disableStreamPreaggregations); 
        out.writeInt(parallelExecInstanceNum);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        codegenLevel =  in.readInt();
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
        sqlMode = Text.readString(in);
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
    }
}
