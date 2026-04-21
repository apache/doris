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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.catalog.info.PartitionNamesInfo;
import org.apache.doris.catalog.info.TableNameInfo;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import org.apache.commons.lang3.StringUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * DropIndexOp
 */
public class DropIndexOp extends AlterTableOp {
    private final String indexName;
    private boolean ifExists;

    private boolean alter;

    private final PartitionNamesInfo partitionNamesInfo;

    /**
     * DropIndexOp without partition spec.
     */
    public DropIndexOp(String indexName, boolean ifExists, TableNameInfo tableName, boolean alter) {
        this(indexName, ifExists, tableName, alter, null);
    }

    /**
     * DropIndexOp with optional partition spec.
     */
    public DropIndexOp(String indexName, boolean ifExists, TableNameInfo tableName, boolean alter,
            PartitionNamesInfo partitionNamesInfo) {
        super(AlterOpType.SCHEMA_CHANGE);
        this.indexName = indexName;
        this.ifExists = ifExists;
        this.tableName = tableName;
        this.alter = alter;
        this.partitionNamesInfo = partitionNamesInfo;
    }

    public String getIndexName() {
        return indexName;
    }

    public TableNameInfo getTableName() {
        return tableName;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public boolean isAlter() {
        return alter;
    }

    public boolean hasPartitionSpec() {
        return partitionNamesInfo != null;
    }

    public List<String> getPartitionNames() {
        return partitionNamesInfo == null
                ? Collections.emptyList()
                : partitionNamesInfo.getPartitionNames();
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (StringUtils.isEmpty(indexName)) {
            throw new AnalysisException("index name is excepted");
        }
        if (tableName != null) {
            tableName.analyze(ctx.getNameSpaceContext());
        }
        if (partitionNamesInfo != null) {
            if (partitionNamesInfo.isTemp()) {
                throw new AnalysisException(
                        "DROP INDEX ON PARTITION does not support temporary partitions");
            }
            if (partitionNamesInfo.getPartitionNames() == null
                    || partitionNamesInfo.getPartitionNames().isEmpty()) {
                throw new AnalysisException(
                        "DROP INDEX ON PARTITION requires explicit partition names, "
                                + "PARTITIONS (*) is not supported");
            }
        }
    }

    @Override
    public boolean allowOpMTMV() {
        return true;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    @Override
    public String toSql() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("DROP INDEX ").append(indexName);
        if (!alter) {
            stringBuilder.append(" ON ").append(tableName != null ? tableName.toSql() : null);
        }
        if (partitionNamesInfo != null) {
            stringBuilder.append(" ").append(partitionNamesInfo.toSql());
        }
        return stringBuilder.toString();
    }
}
