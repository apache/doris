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

package org.apache.doris.cdcclient.source.split;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.flink.cdc.common.utils.Preconditions;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
@NoArgsConstructor
public class SnapshotSplit extends AbstractSourceSplit {
    private static final long serialVersionUID = 1L;
    private static ObjectMapper objectMapper = new ObjectMapper();
    private String tableId;
    private List<String> splitKey;
    private Object[] splitStart;
    private Object[] splitEnd;
    private Map<String, String> highWatermark;

    public SnapshotSplit(
            String splitId,
            String tableId,
            List<String> splitKey,
            Object[] splitStart,
            Object[] splitEnd,
            Map<String, String> highWatermark) {
        super(splitId);
        this.tableId = tableId;
        this.splitKey = splitKey;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.highWatermark = highWatermark;
    }

    public static SnapshotSplit fromMap(Map<String, String> map) throws JsonProcessingException {
        if (map == null || map.isEmpty()) {
            return null;
        }

        SnapshotSplit split = new SnapshotSplit();
        String splitId = map.get("splitId");
        String tableId = map.get("tableId");
        String splitKeyStr = map.get("splitKey");
        Preconditions.checkNotNull(splitKeyStr, "splitKey must not be null");
        List<String> splitKey =
                objectMapper.readValue(splitKeyStr, new TypeReference<List<String>>() {});

        split.setSplitId(splitId);
        split.setTableId(tableId);
        split.setSplitKey(splitKey);

        String splitStartStr = map.get("splitStart");
        if (splitStartStr != null) {
            Object[] splitStart = objectMapper.readValue(splitStartStr, Object[].class);
            split.setSplitStart(splitStart);
        }

        String splitEndStr = map.get("splitEnd");
        if (splitEndStr != null) {
            Object[] splitEnd = objectMapper.readValue(splitEndStr, Object[].class);
            split.setSplitEnd(splitEnd);
        }

        String highWatermarkStr = map.get("highWatermark");
        if (highWatermarkStr != null) {
            Map<String, String> highWatermark =
                    objectMapper.readValue(
                            highWatermarkStr, new TypeReference<Map<String, String>>() {});
            split.setHighWatermark(highWatermark);
        }

        return split;
    }

    public static String getOrEmptyArray(Map<String, String> map, String key) {
        return Optional.ofNullable(map.get(key)).orElse("[]");
    }
}
