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

package org.apache.doris.cdcclient.source.reader.mysql;

import java.util.List;
import java.util.Map;

final class MySqlBinlogLagCalculator {
    private static final String FILE_KEY = "file";
    private static final String POSITION_KEY = "pos";

    private MySqlBinlogLagCalculator() {}

    static long calculate(
            Map<String, String> referenceOffset,
            Map<String, String> endOffset,
            List<BinlogFile> binlogFiles) {
        if (referenceOffset == null || endOffset == null || binlogFiles == null) {
            return -1;
        }
        String referenceFile = referenceOffset.get(FILE_KEY);
        String endFile = endOffset.get(FILE_KEY);
        String referencePositionValue = referenceOffset.get(POSITION_KEY);
        if (referenceFile == null || referencePositionValue == null) {
            // GTID-only startup offsets have no byte position until the reader advances.
            return -1;
        }
        long referencePosition = Long.parseLong(referencePositionValue);
        long endPosition = Long.parseLong(endOffset.get(POSITION_KEY));
        int referenceIndex = indexOf(binlogFiles, referenceFile);
        int endIndex = indexOf(binlogFiles, endFile);
        if (referenceIndex < 0
                || endIndex < referenceIndex
                || referencePosition < 0
                || endPosition < 0) {
            return -1;
        }
        if (referenceIndex == endIndex) {
            return endPosition >= referencePosition ? endPosition - referencePosition : -1;
        }

        long lag = binlogFiles.get(referenceIndex).size() - referencePosition;
        if (lag < 0) {
            return -1;
        }
        for (int i = referenceIndex + 1; i < endIndex; i++) {
            lag = Math.addExact(lag, binlogFiles.get(i).size());
        }
        return Math.addExact(lag, endPosition);
    }

    private static int indexOf(List<BinlogFile> binlogFiles, String name) {
        for (int i = 0; i < binlogFiles.size(); i++) {
            if (binlogFiles.get(i).name().equals(name)) {
                return i;
            }
        }
        return -1;
    }

    record BinlogFile(String name, long size) {}
}
