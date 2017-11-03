// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.cluster;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.catalog.InfoSchemaDb;
import com.baidu.palo.common.io.Text;
import com.baidu.palo.common.io.Writable;
import com.baidu.palo.persist.LinkDbInfo;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * cluster only save db and user's id and name
 * 
 * @author chenhao
 *
 */
public class Cluster implements Writable {
    private static final Logger LOG = LogManager.getLogger(Cluster.class);

    private Long id;
    private String name;
    // backend which cluster own
    private Set<Long> backendIdSet;

    private Set<Long> userIdSet;
    private Set<String> userNameSet;

    private Map<String, LinkDbInfo> linkDbNames;
    private Map<Long, LinkDbInfo> linkDbIds;

    private Set<Long> dbIds;
    private Set<String> dbNames;

    private ReentrantReadWriteLock rwLock;

    public Cluster() {
        this.rwLock = new ReentrantReadWriteLock(true);
        this.backendIdSet = Sets.newHashSet();
        this.userIdSet = Sets.newHashSet();
        this.userNameSet = Sets.newHashSet();
        this.linkDbNames = Maps.newHashMap();
        this.linkDbIds = Maps.newHashMap();
        this.dbIds = Sets.newHashSet();
        this.dbNames = Sets.newHashSet();
    }

    public Cluster(String name, long id) {
        this.name = name;
        this.id = id;
        this.rwLock = new ReentrantReadWriteLock(true);
        this.backendIdSet = Sets.newHashSet();
        this.userIdSet = Sets.newHashSet();
        this.userNameSet = Sets.newHashSet();
        this.linkDbNames = Maps.newHashMap();
        this.linkDbIds = Maps.newHashMap();
        this.dbIds = Sets.newHashSet();
        this.dbNames = Sets.newHashSet();
    }

    public void addLinkDb(BaseParam param) {
        writeLock();
        try {
            if (Strings.isNullOrEmpty(param.getStringParam(1)) || param.getLongParam(1) <= 0) {
                return;
            }
            final LinkDbInfo info = new LinkDbInfo(param.getStringParam(1), param.getLongParam(1));
            linkDbNames.put(param.getStringParam(), info);
            linkDbIds.put(param.getLongParam(), info);
        } finally {
            writeUnlock();
        }

    }

    public void removeLinkDb(BaseParam param) {
        writeLock();
        try {
            linkDbNames.remove(param.getStringParam());
            linkDbIds.remove(param.getLongParam());
        } finally {
            writeUnlock();
        }

    }

    public boolean containLink(String des, String src) {
        readLock();
        try {
            final LinkDbInfo info = linkDbNames.get(des);
            if (info != null && info.getName().equals(src)) {
                return true;
            }
        } finally {
            readUnlock();
        }
        return false;
    }

    public void addUser(String name, long id) {
        if (Strings.isNullOrEmpty(name)) {
            return;
        }
        writeLock();
        try {
            userNameSet.add(name);
            userIdSet.add(id);
        } finally {
            writeUnlock();
        }
    }

    public void addDb(String name, long id) {
        if (Strings.isNullOrEmpty(name)) {
            return;
        }
        writeLock();
        try {
            dbNames.add(name);
            dbIds.add(id);
        } finally {
            writeUnlock();
        }
    }

    public List<String> getDbNames() {
        final ArrayList<String> ret = new ArrayList<String>();
        readLock();
        try {
            ret.addAll(dbNames);
            ret.addAll(linkDbNames.keySet());
        } finally {
            readUnlock();
        }
        return ret;
    }

    public void removeDb(String name, long id) {
        writeLock();
        try {
            dbNames.remove(name);
            dbIds.remove(id);
        } finally {
            writeUnlock();
        }
    }

    public boolean containUser(String name) {
        return userNameSet.contains(name);
    }

    public boolean containUser(long id) {
        return userIdSet.contains(id);
    }

    public boolean containDb(String name) {
        return dbNames.contains(name);
    }

    public boolean containDb(long id) {
        return dbIds.contains(id);
    }

    public void readLock() {
        this.rwLock.readLock().lock();
    }

    public boolean tryReadLock(long timeout, TimeUnit unit) {
        try {
            return this.rwLock.readLock().tryLock(timeout, unit);
        } catch (InterruptedException e) {
            LOG.warn("failed to try read lock at cluster[" + id + "]", e);
            return false;
        }
    }

    public void readUnlock() {
        this.rwLock.readLock().unlock();
    }

    public void writeLock() {
        this.rwLock.writeLock().lock();
    }

    public boolean tryWriteLock(long timeout, TimeUnit unit) {
        try {
            return this.rwLock.writeLock().tryLock(timeout, unit);
        } catch (InterruptedException e) {
            LOG.warn("failed to try write lock at cluster[" + id + "]", e);
            return false;
        }
    }

    public void writeUnlock() {
        this.rwLock.writeLock().unlock();
    }

    public boolean isWriteLockHeldByCurrentThread() {
        return this.rwLock.writeLock().isHeldByCurrentThread();
    }

    public int getClusterCapacity() {
        return backendIdSet.size();
    }

    public List<Long> getBackendIdList() {
        return Lists.newArrayList(backendIdSet);
    }

    public void setBackendIdList(List<Long> backendIdList) {
        if (backendIdList == null) {
            return;
        }
        writeLock();
        try {
            this.backendIdSet = Sets.newHashSet(backendIdList);
        } finally {
            writeUnlock();
        }
    }

    public void addBackend(long backendId) {
        writeLock();
        try {
            this.backendIdSet.add(backendId);
        } finally {
            writeUnlock();
        }
    }

    public void addBackends(List<Long> backendIds) {
        writeLock();
        try {
            this.backendIdSet.addAll(backendIds);
       } finally {
            writeUnlock();
        }
    }

    public Long getId() {
        return id;
    }

    public void setId(Long clusterId) {
        this.id = clusterId;
    }

    public String getName() {
        return name;
    }

    public void setName(String clusterName) {
        this.name = clusterName;
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
        for (String name : dbNames) {
            if (!name.equals(ClusterNamespace.getFullName(this.name, InfoSchemaDb.DATABASE_NAME))) {
                Text.writeString(out, name);
            }
        }

        out.writeInt(dbCount);
        for (long id : dbIds) {
            if (id >= Catalog.NEXT_ID_INIT_VALUE) {
                out.writeLong(id);
            }
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

    @Override
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
    
    public void removeBackend(long removedBackendId) {
        writeLock();
        try {
            backendIdSet.remove((Long)removedBackendId);
        } finally {
            writeUnlock();
        }
    }
    
    public void removeBackends(List<Long> removedBackendIds) {
        writeLock();
        try {
            backendIdSet.remove(removedBackendIds);
        } finally {
            writeUnlock();
        }
    }

}
