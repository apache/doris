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
import org.apache.doris.common.jni.utils.OffHeap;
import org.apache.doris.common.jni.vec.ColumnType;
import org.apache.doris.common.jni.vec.ScanPredicate;
import org.apache.doris.common.jni.vec.TableSchema;

import org.apache.log4j.Logger;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.InternalRow;
import org.apache.paimon.data.columnar.ColumnarRow;
import org.apache.paimon.hive.mapred.PaimonInputSplit;
import org.apache.paimon.options.Options;
import org.apache.paimon.reader.RecordReader;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.TableRead;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;


public class PaimonJniScanner extends JniScanner {
    private static final Logger LOG = Logger.getLogger(PaimonJniScanner.class);

    private static final String PAIMON_OPTION_PREFIX = "paimon_option_prefix.";
    private final Map<String, String> params;
    private final String dbName;
    private final String tblName;
    private final String[] ids;
    private final long splitAddress;
    private final int lengthByte;
    private PaimonInputSplit paimonInputSplit;
    private Table table;
    private RecordReader<InternalRow> reader;
    private final PaimonColumnValue columnValue = new PaimonColumnValue();

    public PaimonJniScanner(int batchSize, Map<String, String> params) {
        this.params = params;
        splitAddress = Long.parseLong(params.get("split_byte"));
        lengthByte = Integer.parseInt(params.get("length_byte"));
        dbName = params.get("db_name");
        tblName = params.get("table_name");
        String[] requiredFields = params.get("required_fields").split(",");
        String[] types = Arrays.stream(params.get("columns_types").split("#"))
            .map(s -> s.replaceAll("\\s+", ""))
            .toArray(String[]::new);
        ids = params.get("columns_id").split(",");
        ColumnType[] columnTypes = new ColumnType[types.length];
        for (int i = 0; i < types.length; i++) {
            columnTypes[i] = ColumnType.parseType(requiredFields[i], types[i]);
        }
        ScanPredicate[] predicates = new ScanPredicate[0];
        if (params.containsKey("push_down_predicates")) {
            long predicatesAddress = Long.parseLong(params.get("push_down_predicates"));
            if (predicatesAddress != 0) {
                predicates = ScanPredicate.parseScanPredicates(predicatesAddress, columnTypes);
                LOG.info("MockJniScanner gets pushed-down predicates:  " + ScanPredicate.dump(predicates));
            }
        }
        initTableInfo(columnTypes, requiredFields, predicates, batchSize);
    }

    @Override
    public void open() throws IOException {
        getCatalog();
        // deserialize it into split
        byte[] splitByte = new byte[lengthByte];
        OffHeap.copyMemory(null, splitAddress, splitByte, OffHeap.BYTE_ARRAY_OFFSET, lengthByte);
        ByteArrayInputStream bais = new ByteArrayInputStream(splitByte);
        DataInputStream input = new DataInputStream(bais);
        try {
            paimonInputSplit.readFields(input);
        } catch (IOException e) {
            e.printStackTrace();
        }
        ReadBuilder readBuilder = table.newReadBuilder()
                                    .withProjection(Arrays.stream(ids).mapToInt(Integer::parseInt).toArray());
        TableRead read = readBuilder.newRead();
        reader = read.createReader(paimonInputSplit.split());
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
                Object record;
                while ((record = batch.next()) != null) {
                    columnValue.setOffsetRow((ColumnarRow) record);
                    for (int i = 0; i < ids.length; i++) {
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

    private void getCatalog() {
        paimonInputSplit = new PaimonInputSplit();
        try {
            Catalog catalog = createCatalog();
            table = catalog.getTable(Identifier.create(dbName, tblName));
        } catch (Catalog.TableNotExistException e) {
            LOG.warn("failed to create paimon external catalog ", e);
            throw new RuntimeException(e);
        }
    }

    private Catalog createCatalog() {
        Options options = new Options();
        for (Map.Entry<String, String> kv : params.entrySet()) {
            if (kv.getKey().startsWith(PAIMON_OPTION_PREFIX)) {
                options.set(kv.getKey().substring(PAIMON_OPTION_PREFIX.length()), kv.getValue());
            }
        }
        CatalogContext context = CatalogContext.create(options);
        return CatalogFactory.createCatalog(context);
    }
}
