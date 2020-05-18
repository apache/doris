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

package org.apache.doris.cluster;

import org.apache.doris.catalog.InfoSchemaDb;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.LinkDbInfo;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * cluster only save db and user's id and name
 * 
 */
public class Cluster implements Writable {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);

    private Long id;
    private String name;
    // backend which cluster own
    private Set<Long> backendIdSet = ConcurrentHashMap.newKeySet();

    private ConcurrentHashMap<String, LinkDbInfo> linkDbNames = new ConcurrentHashMap<>();
    private ConcurrentHashMap<Long, LinkDbInfo> linkDbIds = new ConcurrentHashMap<>();

    private Set<Long> dbIds = ConcurrentHashMap.newKeySet();
    private Set<String> dbNames = ConcurrentHashMap.newKeySet();
    private ConcurrentHashMap<String, Long> dbNameToIDs = new ConcurrentHashMap<>();

    // lock to perform atomic operations
    private ReentrantLock lock = new ReentrantLock(true);

    private Cluster() {
        // for persist
    }

    public Cluster(String name, long id) {
        this.name = name;
        this.id = id;
    }

    private void lock() {
        this.lock.lock();
    }

    private void unlock() {
        this.lock.unlock();
    }

    public Long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public void addLinkDb(BaseParam param) {
        lock();
        try {
            if (Strings.isNullOrEmpty(param.getStringParam(1)) || param.getLongParam(1) <= 0) {
                return;
            }
            final LinkDbInfo info = new LinkDbInfo(param.getStringParam(1), param.getLongParam(1));
            linkDbNames.put(param.getStringParam(), info);
            linkDbIds.put(param.getLongParam(), info);
        } finally {
            unlock();
        }
    }

    public void removeLinkDb(BaseParam param) {
        lock();
        try {
            linkDbNames.remove(param.getStringParam());
            linkDbIds.remove(param.getLongParam());
        } finally {
            unlock();
        }

    }

    public boolean containLink(String dest, String src) {
        final LinkDbInfo info = linkDbNames.get(dest);
        if (info != null && info.getName().equals(src)) {
            return true;
        }
        return false;
    }

    public void addDb(String name, long id) {
        if (Strings.isNullOrEmpty(name)) {
            return;
        }
        lock();
        try {
            dbNames.add(name);
            dbIds.add(id);
            dbNameToIDs.put(name, id);
        } finally {
            unlock();
        }
    }

    public List<String> getDbNames() {
        final ArrayList<String> ret = new ArrayList<String>();
        lock();
        try {
            ret.addAll(dbNames);
            ret.addAll(linkDbNames.keySet());
        } finally {
            unlock();
        }
        return ret;
    }

    public void removeDb(String name, long id) {
        lock();
        try {
            dbNames.remove(name);
            dbIds.remove(id);
        } finally {
            unlock();
        }
    }

    public boolean containDb(String name) {
        return dbNames.contains(name);
    }

    public List<Long> getBackendIdList() {
        return Lists.newArrayList(backendIdSet);
    }

    public void setBackendIdList(List<Long> backendIdList) {
        if (backendIdList == null) {
            return;
        }
        backendIdSet = ConcurrentHashMap.newKeySet();
        backendIdSet.addAll(backendIdList);
    }

    public void addBackend(long backendId) {
        backendIdSet.add(backendId);
    }

    public void addBackends(List<Long> backendIds) {
        backendIdSet.addAll(backendIds);
    }

    public void removeBackend(long removedBackendId) {
        backendIdSet.remove((Long) removedBackendId);
    }

    public static Cluster read(DataInput in) throws IOException {
        Cluster cluster = new Cluster();
        cluster.readFields(in);
        return cluster;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(id);
        Text.writeString(out, name);

        out.writeLong(backendIdSet.size());
        for (Long id : backendIdSet) {
            out.writeLong(id);
        }

        int dbCount = dbIds.size();
        if (dbNames.contains(ClusterNamespace.getFullName(this.name, InfoSchemaDb.DATABASE_NAME))) {
            dbCount--;
        }

        out.writeInt(dbCount);
        // don't persist InfoSchemaDb meta
        for (String name : dbNames) {
            if (!name.equals(ClusterNamespace.getFullName(this.name, InfoSchemaDb.DATABASE_NAME))) {
                Text.writeString(out, name);
            } else {
                dbIds.remove(dbNameToIDs.get(name));
            }
        }

        String errMsg = String.format("%d vs %d, fatal error, Write cluster meta failed!",
                dbNames.size(), dbIds.size() + 1);
        // ensure we have removed InfoSchemaDb id
        Preconditions.checkState(dbNames.size() == dbIds.size() + 1, errMsg);

        out.writeInt(dbCount);
        for (long id : dbIds) {
            out.writeLong(id);
        }

        out.writeInt(linkDbNames.size());
        for (Map.Entry<String, LinkDbInfo> infoMap : linkDbNames.entrySet()) {
            Text.writeString(out, infoMap.getKey());
            infoMap.getValue().write(out);
        }

        out.writeInt(linkDbIds.size());
        for (Map.Entry<Long, LinkDbInfo> infoMap : linkDbIds.entrySet()) {
            out.writeLong(infoMap.getKey());
            infoMap.getValue().write(out);
        }
    }

    public void readFields(DataInput in) throws IOException {
        id = in.readLong();
        name = Text.readString(in);
        Long len = in.readLong();
        while (len-- > 0) {
            Long id = in.readLong();
            backendIdSet.add(id);
        }
        int count = in.readInt();
        while (count-- > 0) {
            dbNames.add(Text.readString(in));
        }

        count = in.readInt();
        while (count-- > 0) {
            dbIds.add(in.readLong());
        }

        count = in.readInt();
        while (count-- > 0) {
            final String key = Text.readString(in);
            final LinkDbInfo value = new LinkDbInfo();
            value.readFields(in);
            linkDbNames.put(key, value);
        }

        count = in.readInt();
        while (count-- > 0) {
            final long key = in.readLong();
            final LinkDbInfo value = new LinkDbInfo();
            value.readFields(in);
            linkDbIds.put(key, value);
        }
    }
}
