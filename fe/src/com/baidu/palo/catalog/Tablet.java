// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

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

package com.baidu.palo.catalog;

import com.baidu.palo.catalog.Replica.ReplicaState;
import com.baidu.palo.common.io.Writable;

import com.google.common.collect.Sets;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * This class represents the olap tablet related metadata.
 */
public class Tablet extends MetaObject implements Writable {
    private static final Logger LOG = LogManager.getLogger(Tablet.class);

    private long id;
    private List<Replica> replicas;
    
    private long checkedVersion;
    private long checkedVersionHash;

    private boolean isConsistent;

    public Tablet() {
        this(0L, new ArrayList<Replica>());
    }
    
    public Tablet(long tabletId) {
        this(tabletId, new ArrayList<Replica>());
    }
    
    public Tablet(long tabletId, List<Replica> replicas) {
        this.id = tabletId;
        this.replicas = replicas;
        if (this.replicas == null) {
            this.replicas = new ArrayList<Replica>();
        }
        
        checkedVersion = -1L;
        checkedVersionHash = -1L;

        isConsistent = true;
    }
    
    public long getId() {
        return this.id;
    }
    
    public long getCheckedVersion() {
        return this.checkedVersion;
    }

    public long getCheckedVersionHash() {
        return this.checkedVersionHash;
    }

    public void setCheckedVersion(long checkedVersion, long checkedVersionHash) {
        this.checkedVersion = checkedVersion;
        this.checkedVersionHash = checkedVersionHash;
    }

    public void setIsConsistent(boolean good) {
        this.isConsistent = good;
    }

    public boolean isConsistent() {
        return isConsistent;
    }

    private boolean deleteRedundantReplica(long backendId, long version) {
        boolean delete = false;
        boolean hasBackend = false;
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getBackendId() == backendId) {
                hasBackend = true;
                if (replica.getVersion() <= version) {
                    iterator.remove();
                    delete = true;
                }
            }
        }

        return delete || !hasBackend;
    }

    public void addReplica(Replica replica) {
        if (deleteRedundantReplica(replica.getBackendId(), replica.getVersion())) {
            replicas.add(replica);
            Catalog.getCurrentInvertedIndex().addReplica(id, replica);
        }
    }
    
    public List<Replica> getReplicas() {
        return this.replicas;
    }
    
    public Set<Long> getBackendIds() {
        Set<Long> beIds = Sets.newHashSet();
        for (Replica replica : replicas) {
            beIds.add(replica.getBackendId());
        }
        return beIds;
    }

    // for query
    public List<Replica> getQueryableReplicas(long committedVersion, long committedVersionHash) {
        List<Replica> queryableReplicas = new LinkedList<Replica>();
        for (Replica replica : replicas) {
            ReplicaState state = replica.getState();
            if (state == ReplicaState.NORMAL || state == ReplicaState.SCHEMA_CHANGE) {
                if (replica.getVersion() > committedVersion 
                        || (replica.getVersion() == committedVersion
                        && replica.getVersionHash() == committedVersionHash)) {
                    queryableReplicas.add(replica);
                }
            }
        }
        return queryableReplicas;
    }

    public Replica getReplicaById(long replicaId) {
        for (Replica replica : replicas) {
            if (replica.getId() == replicaId) {
                return replica;
            }
        }
        return null;
    }
    
    public Replica getReplicaByBackendId(long backendId) {
        for (Replica replica : replicas) {
            if (replica.getBackendId() == backendId) {
                return replica;
            }
        }
        return null;
    }
    
    public boolean deleteReplica(Replica replica) {
        if (replicas.contains(replica)) {
            replicas.remove(replica);
            Catalog.getCurrentInvertedIndex().deleteReplica(id, replica.getBackendId());
            return true;
        }
        return false;
    }
    
    public boolean deleteReplicaByBackendId(long backendId) {
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getBackendId() == backendId) {
                iterator.remove();
                Catalog.getCurrentInvertedIndex().deleteReplica(id, backendId);
                return true;
            }
        }
        return false;
    }
    
    @Deprecated
    public Replica deleteReplicaById(long replicaId) {
        Iterator<Replica> iterator = replicas.iterator();
        while (iterator.hasNext()) {
            Replica replica = iterator.next();
            if (replica.getId() == replicaId) {
                LOG.info("delete replica[" + replica.getId() + "]");
                iterator.remove();
                return replica;
            }
        }
        return null;
    }

    // for test only
    public void clearReplica() {
        this.replicas.clear();
    }
   
    public void setTabletId(long tabletId) {
        this.id = tabletId;
    }

    public static void sortReplicaByVersionDesc(List<Replica> replicas) {
        // sort replicas by version. higher version in the tops
        Collections.sort(replicas, Replica.VERSION_DESC_COMPARATOR);
    }
 
    public String toString() {
        return "tabletId=" + this.id;
    }

    public void write(DataOutput out) throws IOException {
        super.write(out);

        out.writeLong(id);
        int replicaCount = replicas.size();
        out.writeInt(replicaCount);
        for (int i = 0; i < replicaCount; ++i) {
            replicas.get(i).write(out);
        }

        out.writeLong(checkedVersion);
        out.writeLong(checkedVersionHash);
        out.writeBoolean(isConsistent);
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);

        id = in.readLong();
        int replicaCount = in.readInt();
        for (int i = 0; i < replicaCount; ++i) {
            Replica replica = Replica.read(in);
            if (deleteRedundantReplica(replica.getBackendId(), replica.getVersion())) {
                replicas.add(replica);
            }
        }

        if (Catalog.getCurrentCatalogJournalVersion() >= 6) {
            checkedVersion = in.readLong();
            checkedVersionHash = in.readLong();
            isConsistent = in.readBoolean();
        }
    }
    
    public static Tablet read(DataInput in) throws IOException {
        Tablet tablet = new Tablet();
        tablet.readFields(in);
        return tablet;
    }
    
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Tablet)) {
            return false;
        }
        
        Tablet tablet = (Tablet) obj;
        
        if (replicas != tablet.replicas) {
            if (replicas.size() != tablet.replicas.size()) {
                return false;
            }
            int size = replicas.size();
            for (int i = 0; i < size; i++) {
                if (!tablet.replicas.contains(replicas.get(i))) {
                    return false;
                }
            }
        }
        return id == tablet.id;
    }
}
