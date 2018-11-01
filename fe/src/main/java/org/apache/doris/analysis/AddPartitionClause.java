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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.io.Text;

import com.google.common.collect.Maps;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

// clause which is used to add partition
public class AddPartitionClause extends AlterClause {

    private SingleRangePartitionDesc partitionDesc;
    private DistributionDesc distributionDesc;
    private Map<String, String> properties;

    public AddPartitionClause() {
        // for persist
        partitionDesc = new SingleRangePartitionDesc();
        properties = Maps.newHashMap();
    }

    public SingleRangePartitionDesc getSingeRangePartitionDesc() {
        return partitionDesc;
    }

    public DistributionDesc getDistributionDesc() {
        return distributionDesc;
    }

    public AddPartitionClause(SingleRangePartitionDesc partitionDesc,
                              DistributionDesc distributionDesc,
                              Map<String, String> properties) {
        this.partitionDesc = partitionDesc;
        this.distributionDesc = distributionDesc;
        this.properties = properties;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException {

    }

    @Override
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ADD ");
        sb.append(partitionDesc.toSql() + "\n");
        if (distributionDesc != null) {
            sb.append(distributionDesc.toSql());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String className = AddPartitionClause.class.getCanonicalName();
        Text.writeString(out, className);

        partitionDesc.write(out);
        if (distributionDesc == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            distributionDesc.write(out);
        }

        if (properties == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            int count = properties.size();
            out.writeInt(count);
            for (Map.Entry<String, String> prop : properties.entrySet()) {
                Text.writeString(out, prop.getKey());
                Text.writeString(out, prop.getValue());
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        partitionDesc.readFields(in);
        boolean has = in.readBoolean();
        if (has) {
            distributionDesc = DistributionDesc.read(in);
        }
        
        if (in.readBoolean()) {
            int count = in.readInt();
            for (int i = 0; i < count; i++) {
                String key = Text.readString(in);
                String value = Text.readString(in);
                properties.put(key, value);
            }
        }
    }
}
