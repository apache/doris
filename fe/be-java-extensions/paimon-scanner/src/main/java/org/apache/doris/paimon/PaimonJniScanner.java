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

package org.apache.doris.paimon;

import org.apache.doris.common.jni.JniScanner;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ScanPredicate;
import org.apache.doris.common.jni.vec.TableSchema;
import org.apache.doris.paimon.PaimonTableCache.PaimonTableCacheKey;
import org.apache.doris.paimon.PaimonTableCache.TableExt;

import org.apache.paimon.data.InternalRow;
import org.apache.paimon.predicate.Predicate;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.Split;
import org.apache.paimon.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


public class PaimonJniScanner extends JniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(PaimonJniScanner.class);
    private static final String PAIMON_OPTION_PREFIX = "paimon_option_prefix.";
    private final Map<String, String> paimonOptionParams;
    private final String dbName;
    private final String tblName;
    private final String paimonSplit;
    private final String paimonPredicate;
    private Table table;
    private RecordReader<InternalRow> reader;
    private final PaimonColumnValue columnValue = new PaimonColumnValue();
    private List<String> paimonAllFieldNames;

    private long ctlId;
    private long dbId;
    private long tblId;
    private long lastUpdateTime;

    public PaimonJniScanner(int batchSize, Map<String, String> params) {
        LOG.debug("params:{}", params);
        paimonSplit = params.get("paimon_split");
        paimonPredicate = params.get("paimon_predicate");
        dbName = params.get("db_name");
        tblName = params.get("table_name");
        ctlId = Long.parseLong(params.get("ctl_id"));
        dbId = Long.parseLong(params.get("db_id"));
        tblId = Long.parseLong(params.get("tbl_id"));
        lastUpdateTime = Long.parseLong(params.get("last_update_time"));
        super.batchSize = batchSize;
        super.fields = params.get("paimon_column_names").split(",");
        super.predicates = new ScanPredicate[0];
        paimonOptionParams = params.entrySet().stream()
                .filter(kv -> kv.getKey().startsWith(PAIMON_OPTION_PREFIX))
                .collect(Collectors
                        .toMap(kv1 -> kv1.getKey().substring(PAIMON_OPTION_PREFIX.length()), kv1 -> kv1.getValue()));

    }

    @Override
    public void open() throws IOException {
        initTable();
        initReader();
        parseRequiredTypes();
    }

    private void initReader() throws IOException {
        ReadBuilder readBuilder = table.newReadBuilder();
        readBuilder.withProjection(getProjected());
        readBuilder.withFilter(getPredicates());
        reader = readBuilder.newRead().createReader(getSplit());
    }

    private int[] getProjected() {
        return Arrays.stream(fields).mapToInt(paimonAllFieldNames::indexOf).toArray();
    }

    private List<Predicate> getPredicates() {
        List<Predicate> predicates = PaimonScannerUtils.decodeStringToObject(paimonPredicate);
        LOG.info("predicates:{}", predicates);
        return predicates;
    }

    private Split getSplit() {
        Split split = PaimonScannerUtils.decodeStringToObject(paimonSplit);
        LOG.info("split:{}", split);
        return split;
    }

    private void parseRequiredTypes() {
        ColumnType[] columnTypes = new ColumnType[fields.length];
        for (int i = 0; i < fields.length; i++) {
            int index = paimonAllFieldNames.indexOf(fields[i]);
            if (index == -1) {
                throw new RuntimeException(String.format("Cannot find field %s in schema %s",
                        fields[i], paimonAllFieldNames));
            }
            DataType dataType = table.rowType().getTypeAt(index);
            columnTypes[i] = ColumnType.parseType(fields[i], dataType.toString());
        }
        super.types = columnTypes;
    }

    @Override
    public void close() throws IOException {
        reader.close();
    }

    @Override
    protected int getNext() throws IOException {
        int rows = 0;
        try {
            RecordReader.RecordIterator<InternalRow> batch;
            while ((batch = reader.readBatch()) != null) {
                InternalRow record;
                while ((record = batch.next()) != null) {
                    columnValue.setOffsetRow(record);
                    for (int i = 0; i < fields.length; i++) {
                        columnValue.setIdx(i, types[i]);
                        appendData(i, columnValue);
                    }
                    rows++;
                }
                batch.releaseBatch();
            }
        } catch (IOException e) {
            LOG.warn("failed to getNext columnValue ", e);
            throw new RuntimeException(e);
        }
        return rows;
    }

    @Override
    protected TableSchema parseTableSchema() throws UnsupportedOperationException {
        // do nothing
        return null;
    }

    private void initTable() {
        PaimonTableCacheKey key = new PaimonTableCacheKey(ctlId, dbId, tblId, paimonOptionParams, dbName, tblName);
        TableExt tableExt = PaimonTableCache.getTable(key);
        if (tableExt.getCreateTime() < lastUpdateTime) {
            LOG.warn("invalidate cacha table:{}, localTime:{}, remoteTime:{}", key, tableExt.getCreateTime(),
                    lastUpdateTime);
            PaimonTableCache.invalidateTableCache(key);
            tableExt = PaimonTableCache.getTable(key);
        }
        this.table = tableExt.getTable();
        paimonAllFieldNames = PaimonScannerUtils.fieldNames(this.table.rowType());
        LOG.info("paimonAllFieldNames:{}", paimonAllFieldNames);
    }

}
