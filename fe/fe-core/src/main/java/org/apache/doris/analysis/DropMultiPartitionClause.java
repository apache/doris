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

import java.util.Map;


// clause which is used to drop a partition
public class DropMultiPartitionClause extends AlterTableClause {
    private boolean ifExists;
    private PartitionKeyDesc partitionKeyDesc;
    // true if this is to drop a temp partition
    private boolean isTempPartition;
    private boolean forceDrop;

    public DropMultiPartitionClause(boolean ifExists, boolean forceDrop, PartitionKeyDesc partitionKeyDesc,
                                    boolean isTempPartition) {
        super(AlterOpType.DROP_PARTITION);
        this.ifExists = ifExists;
        this.forceDrop = forceDrop;
        this.partitionKeyDesc = partitionKeyDesc;
        this.isTempPartition = isTempPartition;
        this.needTableStable = false;
    }

    public boolean isSetIfExists() {
        return ifExists;
    }

    public PartitionKeyDesc getPartitionKeyDesc() {
        return partitionKeyDesc;
    }

    public boolean isTempPartition() {
        return isTempPartition;
    }

    public boolean isForceDrop() {
        return forceDrop;
    }


    @Override
    public void analyze() throws AnalysisException {
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
        return String.format("DROP PARTITION %s", partitionKeyDesc.toSql());
    }

    @Override
    public String toString() {
        return toSql();
    }
}
