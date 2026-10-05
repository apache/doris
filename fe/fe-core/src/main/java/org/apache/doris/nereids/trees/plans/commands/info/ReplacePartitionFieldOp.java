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
 * ReplacePartitionFieldOp for Iceberg partition evolution
 */
public class ReplacePartitionFieldOp extends AlterTableOp {
    private final String oldPartitionFieldName;
    private final String oldTransformName;
    private final Integer oldTransformArg;
    private final String oldColumnName;
    private final String newTransformName;
    private final Integer newTransformArg;
    private final String newColumnName;
    private final String newPartitionFieldName;

    /**
     * Constructor for ReplacePartitionFieldOp
     */
    public ReplacePartitionFieldOp(String oldPartitionFieldName, String oldTransformName,
            Integer oldTransformArg, String oldColumnName,
            String newTransformName, Integer newTransformArg, String newColumnName,
            String newPartitionFieldName) {
        super(AlterOpType.REPLACE_PARTITION_FIELD);
        this.oldPartitionFieldName = oldPartitionFieldName;
        this.oldTransformName = oldTransformName;
        this.oldTransformArg = oldTransformArg;
        this.oldColumnName = oldColumnName;
        this.newTransformName = newTransformName;
        this.newTransformArg = newTransformArg;
        this.newColumnName = newColumnName;
        this.newPartitionFieldName = newPartitionFieldName;
    }

    public String getOldPartitionFieldName() {
        return oldPartitionFieldName;
    }

    public String getOldTransformName() {
        return oldTransformName;
    }

    public Integer getOldTransformArg() {
        return oldTransformArg;
    }

    public String getOldColumnName() {
        return oldColumnName;
    }

    public String getNewTransformName() {
        return newTransformName;
    }

    public Integer getNewTransformArg() {
        return newTransformArg;
    }

    public String getNewColumnName() {
        return newColumnName;
    }

    public String getNewPartitionFieldName() {
        return newPartitionFieldName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        if (oldPartitionFieldName == null && oldColumnName == null) {
            throw new UserException("Old partition field name or old column name must be specified");
        }
        if (newColumnName == null) {
            throw new UserException("New column name must be specified");
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
        sb.append("REPLACE PARTITION KEY ");
        if (oldPartitionFieldName != null) {
            sb.append(oldPartitionFieldName);
        } else {
            appendPartitionTransform(sb, oldTransformName, oldTransformArg, oldColumnName);
        }
        sb.append(" WITH ");
        appendPartitionTransform(sb, newTransformName, newTransformArg, newColumnName);
        if (newPartitionFieldName != null) {
            sb.append(" AS ").append(newPartitionFieldName);
        }
        return sb.toString();
    }

    private void appendPartitionTransform(StringBuilder sb, String transformName, Integer transformArg,
            String columnName) {
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
