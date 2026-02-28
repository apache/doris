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

package org.apache.doris.cdcclient.source.reader;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.List;
import java.util.Map;

import lombok.Data;

/**
 * Container for source splits and their associated states. Supports both single split (binlog) and
 * multiple splits (snapshot). Iteration over records for these splits is handled separately (for
 * example via pollRecords).
 */
@Data
public class SplitReadResult {
    // List of splits (size=1 for binlog, size>=1 for snapshot)
    private List<SourceSplit> splits;

    // Map of split states (key: splitId, value: state)
    private Map<String, Object> splitStates;

    /** Get the first split ( The types in `splits` are the same.) */
    public SourceSplit getSplit() {
        return splits != null && !splits.isEmpty() ? splits.get(0) : null;
    }

    /** Get the state of the first split */
    public Object getSplitState() {
        if (splits == null || splits.isEmpty() || splitStates == null) {
            return null;
        }
        String firstSplitId = splits.get(0).splitId();
        return splitStates.get(firstSplitId);
    }
}
