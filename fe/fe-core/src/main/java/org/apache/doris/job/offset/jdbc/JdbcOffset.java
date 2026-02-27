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

package org.apache.doris.job.offset.jdbc;

import org.apache.doris.job.cdc.split.AbstractSourceSplit;
import org.apache.doris.job.cdc.split.BinlogSplit;
import org.apache.doris.job.offset.Offset;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.MapUtils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class JdbcOffset implements Offset {

    // There may be multiple snapshot splits,
    // but only one binlog split, with the ID fixed as "binlog-split".
    private List<? extends AbstractSourceSplit> splits;

    @Override
    public String toSerializedJson() {
        return null;
    }

    @Override
    public boolean isEmpty() {
        return false;
    }

    @Override
    public boolean isValidOffset() {
        return false;
    }

    @Override
    public String showRange() {
        if (!splits.isEmpty()) {
            AbstractSourceSplit split = splits.get(0);
            if (split.snapshotSplit()) {
                // need to show hw
                return new Gson().toJson(splits);
            } else {
                BinlogSplit binlogSplit = (BinlogSplit) split;
                HashMap<String, Object> showMap = new HashMap<>();
                showMap.put(JdbcSourceOffsetProvider.SPLIT_ID, BinlogSplit.BINLOG_SPLIT_ID);
                if (binlogSplit.getStartingOffset() != null) {
                    showMap.put("startOffset", binlogSplit.getStartingOffset());
                } else if (CollectionUtils.isNotEmpty(binlogSplit.getFinishedSplits())) {
                    showMap.put("finishedSplitSize", binlogSplit.getFinishedSplits().size());
                }
                if (MapUtils.isNotEmpty(binlogSplit.getEndingOffset())) {
                    showMap.put("endOffset", binlogSplit.getEndingOffset());
                }
                return new Gson().toJson(showMap);
            }
        }
        return "split is null";
    }

    @Override
    public String toString() {
        return "JdbcOffset{"
                + "splits="
                + splits
                + '}';
    }

    public boolean snapshotSplit() {
        Preconditions.checkState(splits != null && !splits.isEmpty(), "splits is null or empty");
        AbstractSourceSplit split = splits.get(0);
        return split.snapshotSplit();
    }

    /**
     * Generate meta info for offset
     * Snapshot: {splits:[{splitId:"tb:0"},...]}
     * Binlog: {"splitId:":"binlog-split",...}
     * @return
     */
    public Map<String, Object> generateMeta() {
        if (!splits.isEmpty()) {
            AbstractSourceSplit split = splits.get(0);
            if (split.snapshotSplit()) {
                // need to show hw
                return new ObjectMapper().convertValue(this,
                        new TypeReference<Map<String, Object>>() {
                        });
            } else {
                return new ObjectMapper().convertValue(split,
                        new TypeReference<Map<String, Object>>() {
                        });
            }
        }
        return new HashMap<>();
    }
}
