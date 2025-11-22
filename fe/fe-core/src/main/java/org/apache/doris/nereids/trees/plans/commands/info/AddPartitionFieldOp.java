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
import org.apache.doris.analysis.AddPartitionFieldClause;
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import java.util.Collections;
import java.util.Map;

/**
 * AddPartitionFieldOp for Iceberg partition evolution
 */
public class AddPartitionFieldOp extends AlterTableOp {
    private final String transformName;
    private final Integer transformArg;
    private final String columnName;
    private final String partitionFieldName;

    public AddPartitionFieldOp(String transformName, Integer transformArg, String columnName,
            String partitionFieldName) {
        super(AlterOpType.ADD_PARTITION_FIELD);
        this.transformName = transformName;
        this.transformArg = transformArg;
        this.columnName = columnName;
        this.partitionFieldName = partitionFieldName;
    }

    public String getTransformName() {
        return transformName;
    }

    public Integer getTransformArg() {
        return transformArg;
    }

    public String getColumnName() {
        return columnName;
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (columnName == null) {
            throw new UserException("Column name must be specified");
        }
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new AddPartitionFieldClause(transformName, transformArg, columnName, partitionFieldName);
    }

    @Override
    public Map<String, String> getProperties() {
        return Collections.emptyMap();
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
        sb.append("ADD PARTITION KEY ");
        if (transformName != null) {
            sb.append(transformName);
            if (transformArg != null) {
                sb.append("(").append(transformArg);
                if (columnName != null) {
                    sb.append(", ").append(columnName);
                }
                sb.append(")");
            } else if (columnName != null) {
                sb.append("(").append(columnName).append(")");
            }
        } else if (columnName != null) {
            sb.append(columnName);
        }
        if (partitionFieldName != null) {
            sb.append(" AS ").append(partitionFieldName);
        }
        return sb.toString();
    }
}
