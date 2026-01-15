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
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import java.util.Collections;
import java.util.Map;

/**
 * DropPartitionFieldOp for Iceberg partition evolution
 */
public class DropPartitionFieldOp extends AlterTableOp {
    private final String partitionFieldName;
    private final String transformName;
    private final Integer transformArg;
    private final String columnName;

    public DropPartitionFieldOp(String partitionFieldName) {
        this(partitionFieldName, null, null, null);
    }

    public DropPartitionFieldOp(String transformName, Integer transformArg, String columnName) {
        this(null, transformName, transformArg, columnName);
    }

    public DropPartitionFieldOp(String partitionFieldName, String transformName,
            Integer transformArg, String columnName) {
        super(AlterOpType.DROP_PARTITION_FIELD);
        this.partitionFieldName = partitionFieldName;
        this.transformName = transformName;
        this.transformArg = transformArg;
        this.columnName = columnName;
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
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

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (partitionFieldName == null && columnName == null) {
            throw new UserException("Partition field name or column name must be specified");
        }
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
        sb.append("DROP PARTITION KEY ");
        if (partitionFieldName != null) {
            sb.append(partitionFieldName);
        } else {
            appendPartitionTransform(sb);
        }
        return sb.toString();
    }

    private void appendPartitionTransform(StringBuilder sb) {
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
    }
}
