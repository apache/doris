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

import org.apache.doris.common.Status;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.proto.PCacheParam;
import org.apache.doris.proto.PCacheValue;
import org.apache.doris.proto.PClearCacheRequest;
import org.apache.doris.proto.PFetchCacheRequest;
import org.apache.doris.proto.PFetchCacheResult;
import org.apache.doris.proto.PUniqueId;
import org.apache.doris.proto.PUpdateCacheRequest;
import org.apache.doris.qe.RowBatch;
import org.apache.doris.thrift.TResultBatch;

import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.security.MessageDigest;
import java.util.List;

/**
 * It encapsulates the request and response parameters and methods,
 * Based on this abstract class, the cache can be placed in FE/BE  and other places such as redis
 */
public abstract class CacheProxy {
    private static final Logger LOG = LogManager.getLogger(CacheBeProxy.class);
    public static int FETCH_TIMEOUT = 10000;
    public static int UPDATE_TIMEOUT = 10000;
    public static int CLEAR_TIMEOUT = 30000;

    public static class CacheParam extends PCacheParam {
        public CacheParam(PCacheParam param) {
            partition_key = param.partition_key;
            last_version = param.last_version;
            last_version_time = param.last_version_time;
        }

        public CacheParam(long partitionKey, long lastVersion, long lastVersionTime) {
            partition_key = partitionKey;
            last_version = lastVersion;
            last_version_time = lastVersionTime;
        }

        public PCacheParam getParam() {
            PCacheParam param = new PCacheParam();
            param.partition_key = partition_key;
            param.last_version = last_version;
            param.last_version_time = last_version_time;
            return param;
        }

        public void debug() {
            LOG.info("cache param, part key {}, version {}, time {}",
                    partition_key, last_version, last_version_time);
        }
    }

    public static class CacheValue extends PCacheValue {
        public CacheParam param;
        public TResultBatch resultBatch;

        public CacheValue() {
            param = null;
            rows = Lists.newArrayList();
            data_size = 0;
            resultBatch = new TResultBatch();
        }

        public void addRpcResult(PCacheValue value) {
            param = new CacheParam(value.param);
            data_size += value.data_size;
            rows.addAll(value.rows);
        }

        public RowBatch getRowBatch() {
            for (byte[] one : rows) {
                resultBatch.addToRows(ByteBuffer.wrap(one));
            }
            RowBatch batch = new RowBatch();
            resultBatch.setPacketSeq(1);
            resultBatch.setIsCompressed(false);
            batch.setBatch(resultBatch);
            batch.setEos(true);
            return batch;
        }

        public void addUpdateResult(long partitionKey, long lastVersion, long lastVersionTime, List<byte[]> rowList) {
            param = new CacheParam(partitionKey, lastVersion, lastVersionTime);
            for (byte[] buf : rowList) {
                data_size += buf.length;
                rows.add(buf);
            }
        }

        public PCacheValue getRpcValue() {
            PCacheValue value = new PCacheValue();
            value.param = param.getParam();
            value.data_size = data_size;
            value.rows = rows;
            return value;
        }

        public void debug() {
            LOG.info("cache value, partkey {}, ver:{}, time {}, row_num {}, data_size {}",
                    param.partition_key, param.last_version, param.last_version_time,
                    rows.size(),
                    data_size);
            for (int i = 0; i < rows.size(); i++) {
                LOG.info("{}:{}", i, rows.get(i));
            }
        }
    }

    public static class UpdateCacheRequest extends PUpdateCacheRequest {
        public int value_count;
        public int row_count;
        public int data_size;
        private List<CacheValue> valueList;

        public UpdateCacheRequest(String sqlStr) {
            this.sql_key = getMd5(sqlStr);
            this.valueList = Lists.newArrayList();
            value_count = 0;
            row_count = 0;
            data_size = 0;
        }

        public void addValue(long partitionKey, long lastVersion, long lastVersionTime, List<byte[]> rowList) {
            CacheValue value = new CacheValue();
            value.addUpdateResult(partitionKey, lastVersion, lastVersionTime, rowList);
            valueList.add(value);
            value_count++;
        }

        public PUpdateCacheRequest getRpcRequest() {
            value_count = valueList.size();
            PUpdateCacheRequest request = new PUpdateCacheRequest();
            request.values = Lists.newArrayList();
            request.sql_key = sql_key;
            for (CacheValue value : valueList) {
                request.values.add(value.getRpcValue());
                row_count += value.rows.size();
                data_size = value.data_size;
            }
            return request;
        }

        public void debug() {
            LOG.info("update cache request, sql_key {}, value_size {}", DebugUtil.printId(sql_key),
                    valueList.size());
            for (CacheValue value : valueList) {
                value.debug();
            }
        }
    }


    public static class FetchCacheRequest extends PFetchCacheRequest {
        private List<CacheParam> paramList;

        public FetchCacheRequest(String sqlStr) {
            this.sql_key = getMd5(sqlStr);
            this.paramList = Lists.newArrayList();
        }

        public void addParam(long partitionKey, long lastVersion, long lastVersionTime) {
            CacheParam param = new CacheParam(partitionKey, lastVersion, lastVersionTime);
            paramList.add(param);
        }

        public PFetchCacheRequest getRpcRequest() {
            PFetchCacheRequest request = new PFetchCacheRequest();
            request.params = Lists.newArrayList();
            request.sql_key = sql_key;
            for (CacheParam param : paramList) {
                request.params.add(param.getParam());
            }
            return request;
        }

        public void debug() {
            LOG.info("fetch cache request, sql_key {}, param count {}", DebugUtil.printId(sql_key), paramList.size());
            for (CacheParam param : paramList) {
                param.debug();
            }
        }
    }

    public static class FetchCacheResult extends PFetchCacheResult {
        public int all_count;
        public int value_count;
        public int row_count;
        public int data_size;
        private List<CacheValue> valueList;

        public FetchCacheResult() {
            valueList = Lists.newArrayList();
            all_count = 0;
            value_count = 0;
            row_count = 0;
            data_size = 0;
        }

        public List<CacheValue> getValueList() {
            return valueList;
        }

        public void setResult(PFetchCacheResult rpcResult) {
            value_count = rpcResult.values.size();
            for (int i = 0; i < rpcResult.values.size(); i++) {
                PCacheValue rpcValue = rpcResult.values.get(i);
                CacheValue value = new CacheValue();
                value.addRpcResult(rpcValue);
                valueList.add(value);
                row_count += value.rows.size();
                data_size += value.data_size;
            }
        }

        public void debug() {
            LOG.info("fetch cache result, value size {}", valueList.size());
            for (CacheValue value : valueList) {
                value.debug();
            }
        }
    }

    public enum CacheProxyType {
        FE,
        BE,
        OUTER
    }

    protected CacheProxy() {
    }

    public static CacheProxy getCacheProxy(CacheProxyType type) {
        switch (type) {
            case BE:
                return new CacheBeProxy();
            case FE:
            case OUTER:
                return null;
        }
        return null;
    }

    public abstract void updateCache(UpdateCacheRequest request, int timeoutMs, Status status);

    public abstract FetchCacheResult fetchCache(FetchCacheRequest request, int timeoutMs, Status status);

    public abstract void clearCache(PClearCacheRequest clearRequest);


    public static PUniqueId getMd5(String str) {
        MessageDigest msgDigest;
        try {
            //128 bit
            msgDigest = MessageDigest.getInstance("MD5");
        } catch (Exception e) {
            return null;
        }
        final byte[] digest = msgDigest.digest(str.getBytes());
        PUniqueId key = new PUniqueId();
        key.lo = getLongFromByte(digest, 0);//64 bit
        key.hi = getLongFromByte(digest, 8);//64 bit
        return key;
    }

    public static final long getLongFromByte(final byte[] array, final int offset) {
        long value = 0;
        for (int i = 0; i < 8; i++) {
            value = ((value << 8) | (array[offset + i] & 0xFF));
        }
        return value;
    }
}
