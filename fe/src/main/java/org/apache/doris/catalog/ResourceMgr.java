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

import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.analysis.DropResourceStmt;
import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Resource manager is responsible for managing external resources used by Doris.
 * For example, Spark/MapReduce used for ETL, Spark/GPU used for queries, HDFS/S3 used for external storage.
 * Now only support Spark.
 */
public class ResourceMgr {
    private static final Logger LOG = LogManager.getLogger(ResourceMgr.class);

    public static final ImmutableList<String> RESOURCE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("ResourceType").add("Key").add("Value")
            .build();

    // { resourceName -> Resource}
    private final Map<String, Resource> nameToResource = Maps.newHashMap();
    private final ReentrantLock lock = new ReentrantLock();
    private ResourceProcNode procNode = null;

    public ResourceMgr() {
    }

    public void createResource(CreateResourceStmt stmt) throws DdlException {
        lock.lock();
        try {
            if (stmt.getResourceType() != ResourceType.SPARK) {
                throw new DdlException("Only support Spark resource.");
            }

            String resourceName = stmt.getResourceName();
            if (nameToResource.containsKey(resourceName)) {
                throw new DdlException("Resource(" + resourceName + ") already exist");
            }

            Resource resource = Resource.fromStmt(stmt);
            nameToResource.put(resourceName, resource);
            // log add
            Catalog.getInstance().getEditLog().logCreateResource(resource);
            LOG.info("create resource success. resource: {}", resource);
        } finally {
            lock.unlock();
        }
    }

    public void replayCreateResource(Resource resource) {
        lock.lock();
        try {
            nameToResource.put(resource.getName(), resource);
        } finally {
            lock.unlock();
        }
    }

    public void dropResource(DropResourceStmt stmt) throws DdlException {
        lock.lock();
        try {
            String name = stmt.getResourceName();
            if (!nameToResource.containsKey(name)) {
                throw new DdlException("Resource(" + name + ") does not exist");
            }

            nameToResource.remove(name);
            // log drop
            Catalog.getInstance().getEditLog().logDropResource(name);
            LOG.info("drop resource success. resource name: {}", name);
        } finally {
            lock.unlock();
        }
    }

    public void replayDropResource(String name) {
        lock.lock();
        try {
            nameToResource.remove(name);
        } finally {
            lock.unlock();
        }
    }

    public boolean containsResource(String name) {
        lock.lock();
        try {
            return nameToResource.containsKey(name);
        } finally {
            lock.unlock();
        }
    }

    public Resource getResource(String name) {
        lock.lock();
        try {
            return nameToResource.get(name);
        } finally {
            lock.unlock();
        }
    }

    // for catalog save image
    public Collection<Resource> getResources() {
        return nameToResource.values();
    }

    public List<List<String>> getResourcesInfo() {
        lock.lock();
        try {
            if (procNode == null) {
                procNode = new ResourceProcNode();
            }
            return procNode.fetchResult().getRows();
        } finally {
            lock.unlock();
        }
    }

    public ResourceProcNode getProcNode() {
        lock.lock();
        try {
            if (procNode == null) {
                procNode = new ResourceProcNode();
            }
            return procNode;
        } finally {
            lock.unlock();
        }
    }

    public class ResourceProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(RESOURCE_PROC_NODE_TITLE_NAMES);

            lock.lock();
            try {
                for (Map.Entry<String, Resource> entry : nameToResource.entrySet()) {
                    Resource resource = entry.getValue();
                    // check resource privs
                    if (!Catalog.getCurrentCatalog().getAuth().checkResourcePriv(ConnectContext.get(), resource.getName(),
                                                                                 PrivPredicate.SHOW)) {
                        continue;
                    }
                    resource.getProcNodeData(result);
                }
            } finally {
                lock.unlock();
            }
            return result;
        }
    }
}

