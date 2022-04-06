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

import org.apache.doris.analysis.AlterResourceStmt;
import org.apache.doris.analysis.CreateResourceStmt;
import org.apache.doris.analysis.DropResourceStmt;
import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.persist.DropResourceOperationLog;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.google.gson.annotations.SerializedName;

/**
 * Resource manager is responsible for managing external resources used by Doris.
 * For example, Spark/MapReduce used for ETL, Spark/GPU used for queries, HDFS/S3 used for external storage.
 * Now only support Spark.
 */
public class ResourceMgr implements Writable {
    private static final Logger LOG = LogManager.getLogger(ResourceMgr.class);

    public static final ImmutableList<String> RESOURCE_PROC_NODE_TITLE_NAMES = new ImmutableList.Builder<String>()
            .add("Name").add("ResourceType").add("Item").add("Value")
            .build();

    // { resourceName -> Resource}
    @SerializedName(value = "nameToResource")
    private final Map<String, Resource> nameToResource = Maps.newConcurrentMap();
    private final ResourceProcNode procNode = new ResourceProcNode();

    public ResourceMgr() {
    }

    public void createResource(CreateResourceStmt stmt) throws DdlException {
        if (stmt.getResourceType() != ResourceType.SPARK
                && stmt.getResourceType() != ResourceType.ODBC_CATALOG
                && stmt.getResourceType() != ResourceType.S3) {
            throw new DdlException("Only support SPARK, ODBC_CATALOG and REMOTE_STORAGE resource.");
        }
        Resource resource = Resource.fromStmt(stmt);
        createResource(resource);
        // log add
        Catalog.getCurrentCatalog().getEditLog().logCreateResource(resource);
        LOG.info("Create resource success. Resource: {}", resource);
    }

    public void createResource(Resource resource) throws DdlException {
        String resourceName = resource.getName();
        if (nameToResource.putIfAbsent(resourceName, resource) != null) {
            throw new DdlException("Resource(" + resourceName + ") already exist");
        }
    }

    public void replayCreateResource(Resource resource) {
        nameToResource.put(resource.getName(), resource);
    }

    public void dropResource(DropResourceStmt stmt) throws DdlException {
        String resourceName = stmt.getResourceName();
        if (!nameToResource.containsKey(resourceName)) {
            throw new DdlException("Resource(" + resourceName + ") does not exist");
        }
        // Check whether the resource is in use before deleting it, except spark resource
        List<String> usedTables = new ArrayList<>();
        List<Long> dbIds = Catalog.getCurrentCatalog().getDbIds();
        for (Long dbId : dbIds) {
            Optional<Database> database = Catalog.getCurrentCatalog().getDb(dbId);
            database.ifPresent(db -> {
                List<Table> tables = db.getTablesOnIdOrder();
                for (Table table : tables) {
                    if (table instanceof OdbcTable) {
                        // odbc resource
                        if (resourceName.equals(((OdbcTable) table).getOdbcCatalogResourceName())) {
                            usedTables.add(db.getFullName() + "." + table.getName());
                        }
                    } else if (table instanceof OlapTable) {
                        // remote resource, such as s3 resource
                        PartitionInfo partitionInfo = ((OlapTable) table).getPartitionInfo();
                        List<Long> partitionIds = ((OlapTable) table).getPartitionIds();
                        for (Long partitionId : partitionIds) {
                            DataProperty dataProperty = partitionInfo.getDataProperty(partitionId);
                            if (resourceName.equals(dataProperty.getRemoteStorageResourceName())) {
                                usedTables.add(db.getFullName() + "." + table.getName());
                                break;
                            }
                        }
                    }
                }
            });
        }
        if (usedTables.size() > 0) {
            LOG.warn("Can not drop resource, since it's used in tables {}", usedTables);
            throw new DdlException("Can not drop resource, since it's used in tables " + usedTables);
        }
        nameToResource.remove(resourceName);
        // log drop
        Catalog.getCurrentCatalog().getEditLog().logDropResource(new DropResourceOperationLog(resourceName));
        LOG.info("Drop resource success. Resource resourceName: {}", resourceName);
    }

    // Drop resource whether successful or not
    public void dropResource(Resource resource) {
        String name = resource.getName();
        if (nameToResource.remove(name) == null) {
            LOG.info("resource " + name + " does not exists.");
            return;
        }
    }

    public void replayDropResource(DropResourceOperationLog operationLog) {
        nameToResource.remove(operationLog.getName());
    }

    public void alterResource(AlterResourceStmt stmt) throws DdlException {
        String resourceName = stmt.getResourceName();
        Map<String, String> properties = stmt.getProperties();

        if (!nameToResource.containsKey(resourceName)) {
            throw new DdlException("Resource(" + resourceName + ") dose not exist.");
        }

        Resource resource = nameToResource.get(resourceName);
        resource.modifyProperties(properties);

        // log alter
        Catalog.getCurrentCatalog().getEditLog().logAlterResource(resource);
        LOG.info("Alter resource success. Resource: {}", resource);
    }

    public void replayAlterResource(Resource resource) {
        nameToResource.put(resource.getName(), resource);
    }

    public boolean containsResource(String name) {
        return nameToResource.containsKey(name);
    }

    public Resource getResource(String name) {
        return nameToResource.get(name);
    }

    public int getResourceNum() {
        return nameToResource.size();
    }

    public List<List<Comparable>> getResourcesInfo(String name, boolean accurateMatch, Set<String> typeSets) {
        List<List<String>> targetRows = procNode.fetchResult().getRows();
        List<List<Comparable>> returnRows = Lists.newArrayList();

        for (List<String> row : targetRows) {
            if (row == null || row.size() < 2) {
                continue;
            }

            String resourceName = row.get(0);
            String resourceType = row.get(1);

            if (name != null) {
                if (accurateMatch && !resourceName.equals(name)) {
                    continue;
                }
                if (!accurateMatch && !resourceName.contains(name)) {
                    continue;
                }
            }

            if (typeSets != null) {
                if (!typeSets.contains(resourceType.toUpperCase())) {
                    continue;
                }
            }

            List<Comparable> comparableRow = Lists.newArrayList();
            for (Comparable slot : row) {
                comparableRow.add(slot);
            }
            returnRows.add(comparableRow);
        }

        return returnRows;
    }

    public ResourceProcNode getProcNode() {
        return procNode;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static ResourceMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, ResourceMgr.class);
    }

    public class ResourceProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(RESOURCE_PROC_NODE_TITLE_NAMES);

            for (Map.Entry<String, Resource> entry : nameToResource.entrySet()) {
                Resource resource = entry.getValue();
                // check resource privs
                if (!Catalog.getCurrentCatalog().getAuth().checkResourcePriv(ConnectContext.get(), resource.getName(),
                                                                             PrivPredicate.SHOW)) {
                    continue;
                }
                resource.getProcNodeData(result);
            }
            return result;
        }
    }

    public static int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : RESOURCE_PROC_NODE_TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return RESOURCE_PROC_NODE_TITLE_NAMES.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }
}
