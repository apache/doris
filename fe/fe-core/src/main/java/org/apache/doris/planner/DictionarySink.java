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

package org.apache.doris.planner;

import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.nereids.trees.plans.commands.info.DictionaryColumnDefinition;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDataSink;
import org.apache.doris.thrift.TDataSinkType;
import org.apache.doris.thrift.TDictLayoutType;
import org.apache.doris.thrift.TDictionarySink;
import org.apache.doris.thrift.TExplainLevel;

import java.util.ArrayList;
import java.util.List;

/**
 * DictionarySink is used to write data into a dictionary table.
 */
public class DictionarySink extends DataSink {
    private final Dictionary dictionary;
    private final List<String> columnNames;
    // not send to BE. use for UnassignedAllBEJob to adjust number of BEs to load.
    private final boolean allowAdaptiveLoad;
    // if full load, keep it null.
    private List<Backend> partialLoadBes = null;

    public DictionarySink(Dictionary dictionary, boolean allowAdaptiveLoad, List<String> columnNames) {
        this.dictionary = dictionary;
        this.allowAdaptiveLoad = allowAdaptiveLoad;
        this.columnNames = columnNames;
    }

    public Dictionary getDictionary() {
        return dictionary;
    }

    public boolean allowAdaptiveLoad() {
        return allowAdaptiveLoad;
    }

    public void setPartialLoadBEs(List<Backend> partialLoadBes) {
        this.partialLoadBes = partialLoadBes;
    }

    public List<Backend> getPartialLoadBEs() {
        return partialLoadBes;
    }

    @Override
    public String getExplainString(String prefix, TExplainLevel explainLevel) {
        StringBuilder strBuilder = new StringBuilder();
        strBuilder.append(prefix).append("DICTIONARY SINK\n");
        strBuilder.append(prefix).append("  TABLE: ").append(dictionary.getName()).append("\n");
        strBuilder.append(prefix).append("  COLUMNS: ").append(String.join(", ", columnNames)).append("\n");
        return strBuilder.toString();
    }

    @Override
    protected TDataSink toThrift() {
        TDataSink tDataSink = new TDataSink(TDataSinkType.DICTIONARY_SINK);

        TDictionarySink tDictionarySink = new TDictionarySink();
        tDictionarySink.setDictionaryId(dictionary.getId());
        if (this.partialLoadBes != null) { // for partial load
            tDictionarySink.setVersionId(dictionary.getVersion()); // complete existing version
        } else {
            tDictionarySink.setVersionId(dictionary.getVersion() + 1); // refresh make a new version
        }
        tDictionarySink.setDictionaryName(dictionary.getName());
        tDictionarySink.setLayoutType(TDictLayoutType.valueOf(dictionary.getLayout().name()));

        // Get columns from dictionary
        List<DictionaryColumnDefinition> columns = dictionary.getDicColumns();

        List<Long> keySlotIds = new ArrayList<>();
        List<Long> valueSlotIds = new ArrayList<>();
        List<String> valueNames = new ArrayList<>();

        // Iterate through columns and separate keys and values
        for (int i = 0; i < columns.size(); i++) {
            DictionaryColumnDefinition column = columns.get(i);
            if (column.isKey()) {
                keySlotIds.add((long) i);
            } else {
                valueSlotIds.add((long) i);
                valueNames.add(column.getName());
            }
        }

        tDictionarySink.setKeyOutputExprSlots(keySlotIds);
        tDictionarySink.setValueOutputExprSlots(valueSlotIds);
        tDictionarySink.setValueNames(valueNames);
        tDictionarySink.setSkipNullKey(dictionary.skipNullKey());
        tDictionarySink.setMemoryLimit(dictionary.getMemoryLimit());

        tDataSink.setDictionarySink(tDictionarySink);
        return tDataSink;
    }

    @Override
    public PlanNodeId getExchNodeId() {
        return null;
    }

    @Override
    public DataPartition getOutputPartition() {
        return DataPartition.UNPARTITIONED;
    }
}
