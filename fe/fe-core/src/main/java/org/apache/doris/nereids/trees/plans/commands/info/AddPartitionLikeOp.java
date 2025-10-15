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

import java.util.Map;

/**
 * AddPartitionLikeOp
 */
public class AddPartitionLikeOp extends AlterTableOp {
    private final String partitionName;
    private final String existedPartitionName;
    private final Boolean isTempPartition;

    public AddPartitionLikeOp(String partitionName,
                                  String existedPartitionName,
                                  boolean isTempPartition) {
        super(AlterOpType.ADD_PARTITION);
        this.partitionName = partitionName;
        this.existedPartitionName = existedPartitionName;
        this.isTempPartition = isTempPartition;
        this.needTableStable = false;
    }

    @Override
    public boolean allowOpMTMV() {
        return false;
    }

    @Override
    public boolean needChangeMTMVState() {
        return false;
    }

    public String getPartitionName() {
        return partitionName;
    }

    public String getExistedPartitionName() {
        return existedPartitionName;
    }

    public Boolean getTempPartition() {
        return isTempPartition;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD PARTITION ").append(partitionName).append(" LIKE ");
        sb.append(existedPartitionName);
        return sb.toString();
    }

    @Override
    public Map<String, String> getProperties() {
        return null;
    }

    @Override
    public String toString() {
        return toSql();
    }
}
