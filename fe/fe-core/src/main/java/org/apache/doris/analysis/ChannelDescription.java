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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.datasource.InternalCatalog;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

// used to describe channel info in data sync job
//      channel_desc:
//          FROM mysql_db.src_tbl INTO doris_db.des_tbl
//          [PARTITION (p1, p2)]
//          [(col1, ...)]
//          [KEEP ORDER]
public class ChannelDescription implements Writable {
    private static final Logger LOG = LogManager.getLogger(ChannelDescription.class);

    @SerializedName(value = "srcDatabase")
    private final String srcDatabase;
    @SerializedName(value = "srcTableName")
    private final String srcTableName;
    @SerializedName(value = "targetTable")
    private final String targetTable;
    @SerializedName(value = "partitionNames")
    private final PartitionNames partitionNames;
    // column names of source table
    @SerializedName(value = "colNames")
    private final List<String> colNames;
    @SerializedName(value = "channelId")
    private long channelId;

    public ChannelDescription(String srcDatabase, String srcTableName, String targetTable,
            PartitionNames partitionNames, List<String> colNames) {
        this.srcDatabase = srcDatabase;
        this.srcTableName = srcTableName;
        this.targetTable = targetTable;
        this.partitionNames = partitionNames;
        this.colNames = colNames;
    }

    public List<String> getColNames() {
        if (colNames == null || colNames.isEmpty()) {
            return null;
        }
        return colNames;
    }

    public void analyze(String fullDbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(srcDatabase)) {
            throw new AnalysisException("No source database in channel description.");
        }

        if (Strings.isNullOrEmpty(srcTableName)) {
            throw new AnalysisException("No source table in channel description.");
        }

        checkAuth(fullDbName);

        if (partitionNames != null) {
            partitionNames.analyze(null);
        }

        analyzeColumns();
    }

    private void checkAuth(String fullDbName) throws AnalysisException {
        if (Strings.isNullOrEmpty(targetTable)) {
            throw new AnalysisException("No target table is assigned in channel description.");
        }

        // check target table auth
        if (!Env.getCurrentEnv().getAccessManager()
                .checkTblPriv(ConnectContext.get(), InternalCatalog.INTERNAL_CATALOG_NAME, fullDbName, targetTable,
                        PrivPredicate.LOAD)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_TABLEACCESS_DENIED_ERROR, "LOAD",
                    ConnectContext.get().getQualifiedUser(),
                    ConnectContext.get().getRemoteIP(), fullDbName + ": " + targetTable);
        }
    }

    private void analyzeColumns() throws AnalysisException {
        Set<String> columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        if ((colNames != null && !colNames.isEmpty())) {
            for (String columnName : colNames) {
                if (!columnNames.add(columnName)) {
                    throw new AnalysisException("Duplicate column: " + columnName);
                }
            }
        }
    }

    public void setChannelId(long channelId) {
        this.channelId = channelId;
    }

    public long getChannelId() {
        return this.channelId;
    }

    public String getTargetTable() {
        return targetTable;
    }

    public String getSrcDatabase() {
        return srcDatabase;
    }

    public String getSrcTableName() {
        return srcTableName;
    }

    public PartitionNames getPartitionNames() {
        return partitionNames;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ChannelDescription read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ChannelDescription.class);
    }
}
