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

package org.apache.doris.job.cdc.split;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.List;
import java.util.Map;

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

    @Override
    public String toString() {
        return new Gson().toJson(this);
    }
}
