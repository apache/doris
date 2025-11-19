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
import org.apache.doris.analysis.AlterTableClause;
import org.apache.doris.analysis.DropPartitionFieldClause;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ConnectContext;

import java.util.Collections;
import java.util.Map;

/**
 * DropPartitionFieldOp for Iceberg partition evolution
 */
public class DropPartitionFieldOp extends AlterTableOp {
    private final String partitionFieldName;

    public DropPartitionFieldOp(String partitionFieldName) {
        super(AlterOpType.DROP_PARTITION_FIELD);
        this.partitionFieldName = partitionFieldName;
    }

    public String getPartitionFieldName() {
        return partitionFieldName;
    }

    @Override
    public void validate(ConnectContext ctx) throws UserException {
        // Validation will be done in IcebergMetadataOps
    }

    @Override
    public AlterTableClause translateToLegacyAlterClause() {
        return new DropPartitionFieldClause(partitionFieldName);
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
        return "DROP PARTITION KEY " + partitionFieldName;
    }
}
