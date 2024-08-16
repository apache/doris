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

import org.apache.doris.lakesoul.arrow.LakeSoulArrowJniScanner;

import com.dmetasoul.lakesoul.LakeSoulArrowReader;
import com.dmetasoul.lakesoul.lakesoul.io.NativeIOReader;
import com.dmetasoul.lakesoul.lakesoul.io.substrait.SubstraitUtil;
import com.lakesoul.shaded.com.fasterxml.jackson.core.type.TypeReference;
import com.lakesoul.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import com.lakesoul.shaded.io.substrait.proto.Plan;
import com.lakesoul.shaded.org.apache.arrow.vector.VectorSchemaRoot;
import com.lakesoul.shaded.org.apache.arrow.vector.types.pojo.Field;
import com.lakesoul.shaded.org.apache.arrow.vector.types.pojo.Schema;
import com.lakesoul.shaded.org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class LakeSoulJniScanner extends LakeSoulArrowJniScanner {
    private static final Logger LOG = LoggerFactory.getLogger(LakeSoulJniScanner.class);

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

        LOG.info("opening LakeSoulJniScanner with params={}", params);

        // add files
        for (String file : params.get(LakeSoulUtils.FILE_NAMES).split(LakeSoulUtils.LIST_DELIM)) {
            nativeIOReader.addFile(file);
        }

        // set primary keys
        String primaryKeys = params.getOrDefault(LakeSoulUtils.PRIMARY_KEYS, "");
        if (!primaryKeys.isEmpty()) {
            nativeIOReader.setPrimaryKeys(
                    Arrays.stream(primaryKeys.split(LakeSoulUtils.LIST_DELIM)).collect(Collectors.toList()));
        }

        String options = params.getOrDefault(LakeSoulUtils.OPTIONS, "{}");
        Map<String, String> optionsMap = new ObjectMapper().readValue(
                options, new TypeReference<Map<String, String>>() {}
        );
        String base64Predicate = optionsMap.get(LakeSoulUtils.SUBSTRAIT_PREDICATE);
        if (base64Predicate != null) {
            Plan predicate = SubstraitUtil.decodeBase64String(base64Predicate);
            if (LOG.isDebugEnabled()) {
                LOG.debug("push predicate={}", predicate);
            }
            nativeIOReader.addFilterProto(predicate);
        }

        for (String key : LakeSoulUtils.OBJECT_STORE_OPTIONS) {
            String value = optionsMap.get(key);
            if (key != null) {
                nativeIOReader.setObjectStoreOption(key, value);
            }
        }

        Schema tableSchema = Schema.fromJSON(params.get(LakeSoulUtils.SCHEMA_JSON));
        String[] requiredFieldNames = params.get(LakeSoulUtils.REQUIRED_FIELDS).split(LakeSoulUtils.LIST_DELIM);

        List<Field> requiredFields = new ArrayList<>();
        for (String fieldName : requiredFieldNames) {
            String name = fieldName.strip();
            if (StringUtils.isEmpty(name)) {
                continue;
            }
            requiredFields.add(tableSchema.findField(fieldName));
        }

        requiredSchema = new Schema(requiredFields);

        nativeIOReader.setSchema(requiredSchema);

        List<Field> partitionFields = new ArrayList<>();
        for (String partitionKV : params.getOrDefault(LakeSoulUtils.PARTITION_DESC, "")
                .split(LakeSoulUtils.LIST_DELIM)) {
            if (partitionKV.isEmpty()) {
                break;
            }
            String[] kv = partitionKV.split(LakeSoulUtils.PARTITIONS_KV_DELIM);
            if (kv.length != 2) {
                throw new IllegalArgumentException("Invalid partition column = " + partitionKV);
            }
            nativeIOReader.setDefaultColumnValue(kv[0], kv[1]);
            partitionFields.add(tableSchema.findField(kv[0]));
        }
        if (!partitionFields.isEmpty()) {
            nativeIOReader.setPartitionSchema(new Schema(partitionFields));
        }

        initTableInfo(params);

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
