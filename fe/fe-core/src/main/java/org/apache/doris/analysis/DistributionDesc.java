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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.DistributionInfo;
import org.apache.doris.catalog.DistributionInfo.DistributionInfoType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;

import org.apache.commons.lang.NotImplementedException;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Set;

public class DistributionDesc implements Writable {
    protected DistributionInfoType type;

    public DistributionDesc() {

    }

    public void analyze(Set<String> colSet, List<ColumnDef> columnDefs) throws AnalysisException {
        throw new NotImplementedException();
    }

    public String toSql() {
        throw new NotImplementedException();
    }

    public DistributionInfo toDistributionInfo(List<Column> columns) throws DdlException {
        throw new NotImplementedException();
    }

    public static DistributionDesc read(DataInput in) throws IOException {
        DistributionInfoType type = DistributionInfoType.valueOf(Text.readString(in));
        if (type == DistributionInfoType.HASH) {
            DistributionDesc desc = new HashDistributionDesc();
            desc.readFields(in);
            return desc;
        } else if (type == DistributionInfoType.RANDOM) {
            DistributionDesc desc = new RandomDistributionDesc();
            desc.readFields(in);
            return desc;
        } else {
            throw new IOException("Unknown distribution type: " + type);
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, type.name());
    }

    public void readFields(DataInput in) throws IOException {
        throw new NotImplementedException();
    }
}
