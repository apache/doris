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

package org.apache.doris.cdcloader.mysql.loader;

import org.apache.flink.cdc.connectors.mysql.source.split.SourceRecords;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Iterator;

public class SplitRecords {
    private final String splitId;;
    private final SourceRecords records;
    private Iterator<SourceRecord> iterator;

    public SplitRecords(String splitId, SourceRecords records) {
        this.splitId = splitId;
        this.records = records;
        this.iterator = records.iterator();
    }

    public String getSplitId() {
        return splitId;
    }

    public SourceRecords getRecords() {
        return records;
    }

    public Iterator<SourceRecord> getIterator() {
        return iterator;
    }

    public void setIterator(Iterator<SourceRecord> iterator) {
        this.iterator = iterator;
    }

    public boolean isEmpty() {
        return splitId == null || records == null || !records.iterator().hasNext();
    }

    @Override
    public String toString() {
        return "SplitRecords{" +
            "split=" + splitId +
            ", records=" + records +
            '}';
    }
}
