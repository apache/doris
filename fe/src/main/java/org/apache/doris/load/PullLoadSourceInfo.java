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

package org.apache.doris.load;

import org.apache.doris.common.io.Writable;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * PullLoadSourceInfo
 */
public class PullLoadSourceInfo implements Writable {
    private static final Logger LOG = LogManager.getLogger(PullLoadSourceInfo.class);

    // Table id to file groups
    private Map<Long, List<BrokerFileGroup>> idToFileGroups;

    public PullLoadSourceInfo() {
        idToFileGroups = Maps.newHashMap();
    }

    public void addFileGroup(BrokerFileGroup fileGroup) {
        List<BrokerFileGroup> fileGroupList = idToFileGroups.get(fileGroup.getTableId());
        if (fileGroupList == null) {
            fileGroupList = Lists.newArrayList(fileGroup);
            idToFileGroups.put(fileGroup.getTableId(), fileGroupList);
        } else {
            fileGroupList.add(fileGroup);
        }
    }

    public Map<Long, List<BrokerFileGroup>> getIdToFileGroups() {
        return idToFileGroups;
    }

    public List<BrokerFileGroup> getBrokerFileGroups(long id) {
        return idToFileGroups.get(id);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PullLoadSourceInfo{");
        int idx = 0;
        for (Map.Entry<Long, List<BrokerFileGroup>> entry : idToFileGroups.entrySet()) {
            if (idx++ > 0) {
                sb.append(",");
            }
            sb.append(entry.getKey()).append(":[");
            int groupIdx = 0;
            for (BrokerFileGroup fileGroup : entry.getValue()) {
                if (groupIdx++ > 0) {
                    sb.append(",");
                }
                sb.append(fileGroup);
            }
            sb.append("]");
        }
        sb.append("}");

        return sb.toString();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(idToFileGroups.size());
        for (Map.Entry<Long, List<BrokerFileGroup>> entry : idToFileGroups.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeInt(entry.getValue().size());
            for (BrokerFileGroup fileGroup : entry.getValue()) {
                fileGroup.write(out);
            }
        }
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        int mapSize = in.readInt();
        for (int i = 0; i < mapSize; ++i) {
            long id = in.readLong();

            List<BrokerFileGroup> fileGroupList = Lists.newArrayList();
            int listSize = in.readInt();
            for (int j = 0; j < listSize; ++j) {
                BrokerFileGroup fileGroup = BrokerFileGroup.read(in);
                fileGroupList.add(fileGroup);
            }

            idToFileGroups.put(id, fileGroupList);
        }
    }

    public static PullLoadSourceInfo read(DataInput in) throws IOException {
        PullLoadSourceInfo sourceInfo = new PullLoadSourceInfo();
        sourceInfo.readFields(in);
        return sourceInfo;
    }
}
