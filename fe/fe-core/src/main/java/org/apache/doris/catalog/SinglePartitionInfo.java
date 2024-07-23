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

package org.apache.doris.catalog;

import org.apache.doris.common.FeMetaVersion;
import org.apache.doris.common.io.Text;
import org.apache.doris.persist.gson.GsonUtils;

import java.io.DataInput;
import java.io.IOException;

public class SinglePartitionInfo extends PartitionInfo {
    public SinglePartitionInfo() {
        super(PartitionType.UNPARTITIONED);
    }

    @Deprecated
    public static PartitionInfo read(DataInput in) throws IOException {
        if (Env.getCurrentEnvJournalVersion() >= FeMetaVersion.VERSION_136) {
            return GsonUtils.GSON.fromJson(Text.readString(in), SinglePartitionInfo.class);
        }

        PartitionInfo partitionInfo = new SinglePartitionInfo();
        partitionInfo.readFields(in);
        return partitionInfo;
    }
}
