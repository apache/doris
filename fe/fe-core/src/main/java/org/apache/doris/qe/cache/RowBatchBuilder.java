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

package org.apache.doris.qe.cache;

import com.google.common.collect.Lists;
import org.apache.doris.analysis.Expr;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Type;
import org.apache.doris.qe.RowBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

/**
 *  According to the query partition range and cache hit, the rowbatch to update the cache is constructed
 */
public class RowBatchBuilder {
    private static final Logger LOG = LogManager.getLogger(RowBatchBuilder.class);

    private CacheBeProxy.UpdateCacheRequest updateRequest;
    private CacheAnalyzer.CacheMode cacheMode;
    private int keyIndex;
    private Type keyType;
    private HashMap<Long, PartitionRange.PartitionSingle> cachePartMap;
    private List<byte[]> rowList;
    private int batchSize;
    private int rowSize;
    private int dataSize;

    public int getRowSize() {
        return rowSize;
    }

    public RowBatchBuilder(CacheAnalyzer.CacheMode model) {
        cacheMode = model;
        keyIndex = 0;
        keyType = Type.INVALID;
        rowList = Lists.newArrayList();
        cachePartMap = new HashMap<>();
        batchSize = 0;
        rowSize = 0;
        dataSize = 0;
    }

    public void buildPartitionIndex(ArrayList<Expr> resultExpr,
                               List<String> columnLabel, Column partColumn,
                               List<PartitionRange.PartitionSingle> newSingleList) {
        if (cacheMode != CacheAnalyzer.CacheMode.Partition) {
            return;
        }

        for (int i = 0; i < columnLabel.size(); i++) {
            if (columnLabel.get(i).equalsIgnoreCase(partColumn.getName())) {
                keyType = resultExpr.get(i).getType();
                keyIndex = i;
                break;
            }
        }
        if (newSingleList != null) {
            for (PartitionRange.PartitionSingle single : newSingleList) {
                cachePartMap.put(single.getCacheKey().realValue(), single);
            }
        } else {
            LOG.info("no new partition single list ");
        }
    }

    public void copyRowData(RowBatch rowBatch) {
        batchSize++;
        rowSize += rowBatch.getBatch().getRowsSize();
        for (ByteBuffer buf : rowBatch.getBatch().getRows()) {
            byte[] bytes = Arrays.copyOfRange(buf.array(), buf.position(), buf.limit());
            dataSize += bytes.length;
            rowList.add(bytes);
        }
    }

    public CacheBeProxy.UpdateCacheRequest buildSqlUpdateRequest(String sql, long partitionKey, long lastVersion, long lastestTime) {
        if (updateRequest == null) {
            updateRequest = new CacheBeProxy.UpdateCacheRequest(sql);
        }
        updateRequest.addValue(partitionKey, lastVersion, lastestTime, rowList);
        return updateRequest;
    }

    public PartitionRange.PartitionKeyType getKeyFromRow(byte[] row, int index, Type type) {
        PartitionRange.PartitionKeyType key = new PartitionRange.PartitionKeyType();
        ByteBuffer buf = ByteBuffer.wrap(row);
        int len;
        for (int i = 0; i <= index; i++) {
            len = buf.get();
            if (i < index) {
                buf.position(buf.position() + len);
            }
            if (i == index) {
                byte[] content = Arrays.copyOfRange(buf.array(), buf.position(), buf.position() + len);
                String str = new String(content);
                key.init(type, str.toString());
            }
        }
        return key;
    }

    /**
     * Rowbatch split to Row
     */
    public CacheBeProxy.UpdateCacheRequest buildPartitionUpdateRequest(String sql) {
        if (updateRequest == null) {
            updateRequest = new CacheBeProxy.UpdateCacheRequest(sql);
        }
        HashMap<Long, List<byte[]>> partRowMap = new HashMap<>();
        List<byte[]> partitionRowList;
        PartitionRange.PartitionKeyType cacheKey;
        for (byte[] row : rowList) {
            cacheKey = getKeyFromRow(row, keyIndex, keyType);
            if (!cachePartMap.containsKey(cacheKey.realValue())) {
                LOG.info("cant find partition key {}", cacheKey.realValue());
                continue;
            }
            if (!partRowMap.containsKey(cacheKey.realValue())) {
                partitionRowList = Lists.newArrayList();
                partitionRowList.add(row);
                partRowMap.put(cacheKey.realValue(), partitionRowList);
            } else {
                partRowMap.get(cacheKey).add(row);
            }
        }

        for (HashMap.Entry<Long, List<byte[]>> entry : partRowMap.entrySet()) {
            Long key = entry.getKey();
            PartitionRange.PartitionSingle partition = cachePartMap.get(key);
            partitionRowList = entry.getValue();
            updateRequest.addValue(key, partition.getPartition().getVisibleVersion(),
                    partition.getPartition().getVisibleVersionTime(), partitionRowList);
        }
        return updateRequest;
    }
}
