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

package org.apache.doris.lakesoul;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.google.common.base.Preconditions;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.doris.common.jni.vec.ScanPredicate;
import org.apache.doris.lakesoul.arrow.LakeSoulArrowJniScanner;
import org.apache.doris.lakesoul.parquet.ParquetFilter;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.doris.lakesoul.LakeSoulUtils.*;

public class LakeSoulJniScanner extends LakeSoulArrowJniScanner {

    private final Map<String, String> params;

    private transient LakeSoulArrowReader lakesoulArrowReader;

    private VectorSchemaRoot currentBatch = null;

    private final int awaitTimeout;

    private final int batchSize;

    public LakeSoulJniScanner(int batchSize, Map<String, String> params) {
        super();
        this.params = params;
        awaitTimeout = 10000;
        this.batchSize = batchSize;
    }

    @Override
    public void open() throws IOException {
        NativeIOReader nativeIOReader = new NativeIOReader();
        withAllocator(nativeIOReader.getAllocator());
        nativeIOReader.setBatchSize(batchSize);

        // add files
        for (String file : params.get(FILE_NAMES).split(LIST_DELIM)) {
            nativeIOReader.addFile(file);
        }

        // set primary keys
        String primaryKeys = params.getOrDefault(PRIMARY_KEYS, "");
        if (!primaryKeys.isEmpty()) {
            nativeIOReader.setPrimaryKeys(
                Arrays.stream(primaryKeys.split(LIST_DELIM)).collect(Collectors.toList()));
        }

        Schema schema = Schema.fromJSON(params.get(SCHEMA_JSON));
        String[] requiredFieldNames = params.get(REQUIRED_FIELDS).split(LIST_DELIM);

        List<Field> requiredFields = new ArrayList<>();
        for (String fieldName : requiredFieldNames) {
            requiredFields.add(schema.findField(fieldName));
        }

        requiredSchema = new Schema(requiredFields);

        nativeIOReader.setSchema(requiredSchema);

        HashSet<String> partitionColumn = new HashSet<>();
        for (String partitionKV:params.getOrDefault(PARTITION_DESC, "").split(LIST_DELIM)) {
            if (partitionKV.isEmpty()) break;
            String[] kv = partitionKV.split(PARTITIONS_KV_DELIM);
            Preconditions.checkArgument(kv.length == 2, "Invalid partition column = " + partitionKV);
            partitionColumn.add(kv[0]);
        }

        initTableInfo(params);

        for (ScanPredicate predicate : predicates) {
            if (!partitionColumn.contains(predicate.columName)) {
                nativeIOReader.addFilter(ParquetFilter.toParquetFilter(predicate).toString());
            }
        }

        nativeIOReader.initializeReader();
        lakesoulArrowReader = new LakeSoulArrowReader(nativeIOReader, awaitTimeout);
    }

    @Override
    public void close() {
        super.close();
        if (currentBatch != null) {
            currentBatch.close();
        }
        if (lakesoulArrowReader != null) {
            lakesoulArrowReader.close();
        }
    }

    @Override
    public int getNext() throws IOException {
        if (lakesoulArrowReader.hasNext()) {
            currentBatch = lakesoulArrowReader.nextResultVectorSchemaRoot();
            int rows = currentBatch.getRowCount();
            vectorTable = loadVectorSchemaRoot(currentBatch);
            return rows;
        } else {
            return 0;
        }
    }

    @Override
    public long getNextBatchMeta() throws IOException {
        int numRows;
        try {
            numRows = getNext();
        } catch (IOException e) {
            releaseTable();
            throw e;
        }
        if (numRows == 0) {
            releaseTable();
            return 0;
        }
        assert (numRows == vectorTable.getNumRows());
        return vectorTable.getMetaAddress();
    }

    @Override
    public void releaseTable() {
        super.releaseTable();
        if (currentBatch != null) {
            currentBatch.close();
        }
    }
}
