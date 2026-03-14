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

package org.apache.doris.catalog.stream;

import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.lock.MonitoredReentrantReadWriteLock;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.thrift.TCell;
import org.apache.doris.thrift.TRow;

import com.google.common.base.Preconditions;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class StreamManager implements Writable {
    private static final Logger LOG = LogManager.getLogger(StreamManager.class);
    @SerializedName(value = "dbStreamMap")
    private Map<Long, Set<Long>> dbStreamMap;
    protected MonitoredReentrantReadWriteLock rwLock;

    public StreamManager() {
        this.rwLock = new MonitoredReentrantReadWriteLock(true);
        this.dbStreamMap = new HashMap<>();
    }

    public void addStream(BaseStream stream) {
        rwLock.writeLock().lock();
        try {
            dbStreamMap.computeIfAbsent(stream.getDatabase().getId(), k -> new HashSet<>()).add(stream.getId());
        } finally {
            rwLock.writeLock().unlock();
        }

    }

    public void removeStream(BaseStream stream) {
        rwLock.writeLock().lock();
        try {
            Optional.ofNullable(dbStreamMap.get(stream.getDatabase().getId()))
                    .ifPresent(set -> set.remove(stream.getId()));
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static StreamManager read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, StreamManager.class);
    }

    public Set<Long> getStreamIds(DatabaseIf db) {
        Set<Long> result = new HashSet<>();
        rwLock.readLock().lock();
        try {
            result.addAll(dbStreamMap.getOrDefault(db.getId(), new HashSet<>()));
        } finally {
            rwLock.readLock().unlock();
        }
        return result;
    }

    public void fillStreamValuesMetadataResult(List<TRow> dataBatch) {
        Map<Long, Set<Long>> coipedMap = new HashMap<>();
        rwLock.readLock().lock();
        try {
            for (Map.Entry<Long, Set<Long>> e : dbStreamMap.entrySet()) {
                coipedMap.put(e.getKey(), new HashSet<>(e.getValue()));
            }
        } finally {
            rwLock.readLock().unlock();
        }
        for (Map.Entry<Long, Set<Long>> entry : coipedMap.entrySet()) {
            Optional<Database> db = Env.getCurrentInternalCatalog().getDb(entry.getKey());
            if (db.isPresent()) {
                for (Long tableId : entry.getValue()) {
                    Optional<Table> table = db.get().getTable(tableId);
                    if (!table.isPresent()) {
                        if (LOG.isDebugEnabled()) {
                            LOG.warn("invalid stream id: {}, db: {}", tableId, db.get().getFullName());
                        }
                        continue;
                    }
                    Preconditions.checkArgument(table.get() instanceof BaseStream);
                    BaseStream stream = (BaseStream) table.get();
                    stream.readLock();
                    try {
                        TRow trow = new TRow();
                        trow.addToColumnValue(new TCell().setStringVal(stream.getName()));
                        trow.addToColumnValue(new TCell().setStringVal(stream.getStreamType()));
                        trow.addToColumnValue(new TCell().setStringVal(stream.getConsumeType()));
                        trow.addToColumnValue(new TCell().setStringVal(stream.getComment()));
                        trow.addToColumnValue(new TCell().setStringVal(stream.getDatabase().getFullName()));
                        TableIf baseTable = stream.getBaseTableNullable();
                        if (baseTable == null) {
                            trow.addToColumnValue(new TCell().setStringVal("N/A"));
                            trow.addToColumnValue(new TCell().setStringVal("N/A"));
                            trow.addToColumnValue(new TCell().setStringVal("N/A"));
                            trow.addToColumnValue(new TCell().setStringVal("N/A"));
                        } else {
                            List<String> baseTableQualifiers = baseTable.getFullQualifiers();
                            Preconditions.checkArgument(baseTableQualifiers.size() == 3);
                            trow.addToColumnValue(new TCell().setStringVal(baseTableQualifiers.get(2)));
                            trow.addToColumnValue(new TCell().setStringVal(baseTableQualifiers.get(1)));
                            trow.addToColumnValue(new TCell().setStringVal(baseTableQualifiers.get(0)));
                            trow.addToColumnValue(new TCell().setStringVal(baseTable.getType().name()));
                        }
                        trow.addToColumnValue(new TCell().setBoolVal(!stream.isDisabled()));
                        trow.addToColumnValue(new TCell().setBoolVal(stream.isStale()));
                        trow.addToColumnValue(new TCell().setStringVal(stream.getStaleReason()));
                        dataBatch.add(trow);
                    } finally {
                        stream.readUnlock();
                    }
                }
            }
        }
    }
}
