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

package org.apache.doris.regression.util

class RowsetInfoUtils {
    private static final int LEGACY_FIELD_COUNT = 8
    private static final int SEGMENT_LIST_FIELD_COUNT = 9
    private static final int DATA_SIZE_FIELD_INDEX = 5
    private static final int DATA_SIZE_UNIT_FIELD_INDEX = 6
    private static final int COMPACTION_LEVEL_FIELD_INDEX = 7
    private static final String COMPACTION_LEVEL_PATTERN = /level=[0-9]+/
    private static final String SEGMENT_LIST_PATTERN = /\[[0-9]+(?:,[0-9]+)*\]/

    static List<String> dataSizeFields(String rowsetInfo) {
        String[] fields = rowsetInfo.split(" ")
        if (fields.length != LEGACY_FIELD_COUNT && fields.length != SEGMENT_LIST_FIELD_COUNT) {
            throw new IllegalArgumentException("Unexpected rowset info: ${rowsetInfo}")
        }
        if (!(fields[COMPACTION_LEVEL_FIELD_INDEX] ==~ COMPACTION_LEVEL_PATTERN)) {
            throw new IllegalArgumentException("Unexpected compaction level in rowset info: ${rowsetInfo}")
        }
        if (fields.length == SEGMENT_LIST_FIELD_COUNT &&
                !(fields[-1] ==~ SEGMENT_LIST_PATTERN)) {
            throw new IllegalArgumentException("Unexpected segment list in rowset info: ${rowsetInfo}")
        }
        return [fields[DATA_SIZE_FIELD_INDEX], fields[DATA_SIZE_UNIT_FIELD_INDEX]]
    }
}
