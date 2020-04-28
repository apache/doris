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

package org.apache.doris.catalog;

import org.apache.doris.analysis.ModifyEtlClusterClause;
import org.apache.doris.catalog.EtlCluster.EtlClusterType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Etl cluster manager
 */
public class EtlClusterMgr {
    private static final Logger LOG = LogManager.getLogger(EtlClusterMgr.class);

    public static final ImmutableList<String> ETL_CLUSTER_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("ClusterType").add("Key").add("Value")
            .build();

    // { clusterName -> EtlCluster }
    private final Map<String, EtlCluster> nameToEtlCluster = Maps.newHashMap();
    private final ReentrantLock lock = new ReentrantLock();
    private EtlClusterProcNode procNode = null;

    public EtlClusterMgr() {
    }

    public void execute(ModifyEtlClusterClause clause) throws DdlException {
        switch (clause.getOp()) {
            case OP_ADD:
                addEtlCluster(clause);
                break;
            case OP_DROP:
                dropEtlCluster(clause.getClusterName());
                break;
            default:
                break;
        }
    }

    private void addEtlCluster(ModifyEtlClusterClause clause) throws DdlException {
        lock.lock();
        try {
            if (clause.getClusterType() != EtlClusterType.SPARK) {
                throw new DdlException("Only support Spark cluster.");
            }

            String clusterName = clause.getClusterName();
            if (nameToEtlCluster.containsKey(clusterName)) {
                throw new DdlException("Etl cluster(" + clusterName + ") already exist");
            }

            EtlCluster etlCluster = EtlCluster.fromClause(clause);
            nameToEtlCluster.put(clusterName, etlCluster);
            // log add
            Catalog.getInstance().getEditLog().logAddEtlCluster(etlCluster);
            LOG.info("add etl cluster success. cluster: {}", etlCluster);
        } finally {
            lock.unlock();
        }
    }

    public void replayAddEtlCluster(EtlCluster etlCluster) {
        lock.lock();
        try {
            nameToEtlCluster.put(etlCluster.getName(), etlCluster);
        } finally {
            lock.unlock();
        }
    }

    private void dropEtlCluster(String name) throws DdlException {
        lock.lock();
        try {
            if (!nameToEtlCluster.containsKey(name)) {
                throw new DdlException("Etl cluster name(" + name + ") does not exist");
            }

            nameToEtlCluster.remove(name);
            // log drop
            Catalog.getInstance().getEditLog().logDropEtlCluster(name);
            LOG.info("drop etl cluster success. cluster name: {}", name);
        } finally {
            lock.unlock();
        }
    }

    public void replayDropEtlCluster(String name) {
        lock.lock();
        try {
            nameToEtlCluster.remove(name);
        } finally {
            lock.unlock();
        }
    }

    public boolean containsEtlCluster(String name) {
        lock.lock();
        try {
            return nameToEtlCluster.containsKey(name);
        } finally {
            lock.unlock();
        }
    }

    public EtlCluster getEtlCluster(String name) {
        lock.lock();
        try {
            return nameToEtlCluster.get(name);
        } finally {
            lock.unlock();
        }
    }

    // for catalog save image
    public Collection<EtlCluster> getEtlClusters() {
        return nameToEtlCluster.values();
    }

    public List<List<String>> getEtlClustersInfo() {
        lock.lock();
        try {
            if (procNode == null) {
                procNode = new EtlClusterProcNode();
            }
            return procNode.fetchResult().getRows();
        } finally {
            lock.unlock();
        }
    }

    public EtlClusterProcNode getProcNode() {
        lock.lock();
        try {
            if (procNode == null) {
                procNode = new EtlClusterProcNode();
            }
            return procNode;
        } finally {
            lock.unlock();
        }
    }

    public class EtlClusterProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(ETL_CLUSTER_PROC_NODE_TITLE_NAMES);

            lock.lock();
            try {
                for (Map.Entry<String, EtlCluster> entry : nameToEtlCluster.entrySet()) {
                    EtlCluster etlCluster = entry.getValue();
                    etlCluster.getProcNodeData(result);
                }
            } finally {
                lock.unlock();
            }
            return result;
        }
    }
}

