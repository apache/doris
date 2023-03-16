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

package org.apache.doris.load.sync.canal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.common.collect.Lists;

import java.io.Serializable;
import java.sql.Types;
import java.util.List;

public class CanalTestUtil {

    public static CanalEntry.Column buildColumn(String colName, int colValue) {
        CanalEntry.Column.Builder columnBuilder = CanalEntry.Column.newBuilder();
        Serializable value = Integer.valueOf(colValue);
        columnBuilder.setName(colName);
        columnBuilder.setIsKey(true);
        columnBuilder.setMysqlType("bigint");
        columnBuilder.setIndex(0);
        columnBuilder.setIsNull(false);
        columnBuilder.setValue(value.toString());
        columnBuilder.setSqlType(Types.BIGINT);
        columnBuilder.setUpdated(false);
        return columnBuilder.build();
    }

    public static CanalEntry.RowChange buildRowChange() {
        CanalEntry.RowData.Builder rowDataBuilder = CanalEntry.RowData.newBuilder();
        CanalEntry.RowChange.Builder rowChangeBuider = CanalEntry.RowChange.newBuilder();
        rowChangeBuider.setIsDdl(false);
        rowChangeBuider.setEventType(CanalEntry.EventType.INSERT);
        rowDataBuilder.addAfterColumns(buildColumn("a", 1));
        rowDataBuilder.addAfterColumns(buildColumn("b", 2));
        rowChangeBuider.addRowDatas(rowDataBuilder.build());
        return rowChangeBuider.build();
    }

    public static CanalEntry.Entry buildEntry(String binlogFile, long offset, long timestamp) {
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(offset);
        headerBuilder.setExecuteTime(timestamp);
        CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
        entryBuilder.setHeader(headerBuilder.build());
        entryBuilder.setEntryType(CanalEntry.EntryType.ROWDATA);
        entryBuilder.setStoreValue(buildRowChange().toByteString());
        return entryBuilder.build();
    }

    public static CanalEntry.Entry buildEntry(String binlogFile, long offset, long timestamp, String schemaName, String tableName) {
        CanalEntry.Header.Builder headerBuilder = CanalEntry.Header.newBuilder();
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(offset);
        headerBuilder.setExecuteTime(timestamp);
        headerBuilder.setSchemaName(schemaName);
        headerBuilder.setTableName(tableName);
        CanalEntry.Entry.Builder entryBuilder = CanalEntry.Entry.newBuilder();
        entryBuilder.setHeader(headerBuilder.build());
        entryBuilder.setEntryType(CanalEntry.EntryType.ROWDATA);
        entryBuilder.setStoreValue(buildRowChange().toByteString());
        return entryBuilder.build();
    }

    public static Message fetchEOFMessage() {
        return new Message(-1L, Lists.newArrayList());
    }

    public static Message fetchMessage(long id, boolean isRaw, int batchSize, String binlogFile, long offset, String schemaName, String tableName) {
        List<CanalEntry.Entry> entries = Lists.newArrayList();
        for (int i = 0; i < batchSize; i++) {
            entries.add(buildEntry(binlogFile, offset++, 1024, schemaName, tableName));
        }
        return new Message(id, isRaw, entries);
    }


}
