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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;

import com.google.common.base.Strings;

import java.util.Map;

// clause which is used to drop a partition from specified index
public class DropPartitionFromIndexClause extends AlterTableClause {
    private boolean ifExists;
    private String partitionName;
    // true if this is to drop a temp partition
    private boolean isTempPartition;
    private boolean forceDrop;
    private String indexName;

    public DropPartitionFromIndexClause(boolean ifExists, String partitionName, boolean isTempPartition,
            boolean forceDrop, String indexName) {
        super(AlterOpType.DROP_PARTITION);
        this.ifExists = ifExists;
        this.partitionName = partitionName;
        this.isTempPartition = isTempPartition;
        this.needTableStable = false;
        this.forceDrop = forceDrop;
        this.indexName = indexName;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {
        if (Strings.isNullOrEmpty(partitionName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_PARTITION_NAME, partitionName);
        }
        if (Strings.isNullOrEmpty(indexName)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_WRONG_NAME_FOR_INDEX, indexName);
        }
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("DROP PARTITION " + partitionName);
        sb.append(" FROM INDEX " + indexName);
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }
}
