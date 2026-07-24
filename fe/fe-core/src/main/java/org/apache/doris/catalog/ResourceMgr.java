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

import org.apache.doris.catalog.Resource.ResourceType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.PatternMatcher;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.common.proc.ProcNodeInterface;
import org.apache.doris.common.proc.ProcResult;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.DropResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.persist.DropResourceOperationLog;
import org.apache.doris.persist.EditLog;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

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
    private final ConcurrentMap<String, Resource> nameToResource = Maps.newConcurrentMap();
    private final ResourceProcNode procNode = new ResourceProcNode();

    public ResourceMgr() {
    }

    public void createResource(CreateResourceCommand command) throws DdlException {
        CreateResourceInfo info = command.getInfo();
        if (info.getResourceType() == ResourceType.UNKNOWN) {
            throw new DdlException(
                    "Only support SPARK, ODBC_CATALOG, JDBC, S3_COOLDOWN, S3, HDFS(JFS/JUICEFS), and HMS resource.");
        }
        Resource resource = Resource.fromCommand(command);
        Resource logResource;
        EditLog.EditLogItem logItem;
        synchronized (nameToResource) {
            if (nameToResource.containsKey(resource.getName())) {
                if (info.isIfNotExists()) {
                    return;
                }
                throw new DdlException("Resource(" + resource.getName() + ") already exist");
            }
            logResource = getCreateLogResource(resource);
            // Queue CREATE before publishing the live object so dependent operations cannot journal first.
            logItem = Env.getCurrentEnv().getEditLog()
                    .submitEdit(OperationType.OP_CREATE_RESOURCE, logResource);
            nameToResource.put(resource.getName(), resource);
        }
        logItem.await();
        LOG.info("Create resource success. Resource: {}", logResource.getName());
    }

    // Return true if the resource is truly added,
    // otherwise, return false or throw exception.
    public boolean createResource(Resource resource, boolean ifNotExists) throws DdlException {
        String resourceName = resource.getName();
        synchronized (nameToResource) {
            if (nameToResource.putIfAbsent(resourceName, resource) != null) {
                if (ifNotExists) {
                    return false;
                }
                throw new DdlException("Resource(" + resourceName + ") already exist");
            }
            return true;
        }
    }

    public void replayCreateResource(Resource resource) {
        resource.applyDefaultProperties();
        nameToResource.put(resource.getName(), resource);
    }

    public void dropResource(DropResourceCommand dropResourceCommand) throws DdlException {
        String resourceName = dropResourceCommand.getResourceName();
        Resource resource = nameToResource.get(resourceName);
        if (resource == null) {
            if (dropResourceCommand.isIfExists()) {
                return;
            }
            throw new DdlException("Resource(" + resourceName + ") does not exist");
        }

        EditLog.EditLogItem logItem;
        synchronized (resource.getAlterLock()) {
            if (nameToResource.get(resourceName) != resource) {
                if (dropResourceCommand.isIfExists() && !nameToResource.containsKey(resourceName)) {
                    return;
                }
                throw new DdlException("Resource(" + resourceName + ") changed concurrently");
            }

            resource.dropResource();

            // Check whether the resource is in use before deleting it, except spark resource
            StoragePolicy checkedStoragePolicy = StoragePolicy.ofCheck(null);
            checkedStoragePolicy.setStorageResource(resourceName);
            if (Env.getCurrentEnv().getPolicyMgr().existPolicy(checkedStoragePolicy)) {
                Policy policy = Env.getCurrentEnv().getPolicyMgr().getPolicy(checkedStoragePolicy);
                LOG.warn("Can not drop resource, since it's used in policy {}", policy.getPolicyName());
                throw new DdlException("Can not drop resource, since it's used in policy " + policy.getPolicyName());
            }

            logItem = Env.getCurrentEnv().getEditLog().submitEdit(
                    OperationType.OP_DROP_RESOURCE, new DropResourceOperationLog(resourceName));
            if (!nameToResource.remove(resourceName, resource)) {
                LOG.error("Fatal Error: failed to remove resource {} after submitting its drop log", resourceName);
                System.exit(-1);
                throw new IllegalStateException("failed to remove resource " + resourceName);
            }
        }

        logItem.await();
        LOG.info("Drop resource success. Resource resourceName: {}", resourceName);
    }

    // Drop resource whether successful or not
    public void dropResource(Resource resource) {
        String name = resource.getName();
        synchronized (resource.getAlterLock()) {
            if (!nameToResource.remove(name, resource)) {
                LOG.info("resource " + name + " does not exists.");
            }
        }
    }

    public void replayDropResource(DropResourceOperationLog operationLog) {
        nameToResource.remove(operationLog.getName());
    }

    public void alterResource(String resourceName, Map<String, String> properties) throws DdlException {
        Resource resource = nameToResource.get(resourceName);
        if (resource == null) {
            throw new DdlException("Resource(" + resourceName + ") dose not exist.");
        }

        Resource logResource;
        EditLog.EditLogItem logItem;
        synchronized (resource.getAlterLock()) {
            if (nameToResource.get(resourceName) != resource) {
                throw new DdlException("Resource(" + resourceName + ") changed concurrently");
            }
            resource.modifyProperties(properties);
            // Keep mutation, snapshot, and submission ordered without holding the resource state lock.
            logResource = getAlterLogResource(resource);
            logItem = Env.getCurrentEnv().getEditLog().submitEdit(OperationType.OP_ALTER_RESOURCE, logResource);
        }

        logItem.await();
        LOG.info("Alter resource success. Resource: {}", logResource);
    }

    public void replayAlterResource(Resource resource) {
        Resource replayed = nameToResource.computeIfPresent(resource.getName(), (name, currentResource) -> {
            if (resource.getId() != currentResource.getId()) {
                LOG.warn("Ignore alter log for replaced resource {} with id {}, current id is {}",
                        name, resource.getId(), currentResource.getId());
                return currentResource;
            }
            resource.copyReferencesFrom(currentResource);
            return resource;
        });
        if (replayed == null) {
            LOG.warn("Ignore alter log for non-existent resource {}", resource.getName());
        }
    }

    public boolean containsResource(String name) {
        return nameToResource.containsKey(name);
    }

    public Resource getResource(String name) {
        // nameToResource == null iff this is in replay thread
        // just return null to ignore this.
        if (nameToResource == null) {
            return null;
        }
        return nameToResource.get(name);
    }

    public int getResourceNum() {
        return nameToResource.size();
    }

    public List<Resource> getResource(ResourceType type) {
        return nameToResource.values().stream().filter(resource -> resource.getType() == type)
                .collect(Collectors.toList());
    }

    public List<List<Comparable>> getResourcesInfo(PatternMatcher matcher,
            String name, boolean accurateMatch, Set<String> typeSets) {
        List<List<String>> targetRows = procNode.fetchResult().getRows();
        List<List<Comparable>> returnRows = Lists.newArrayList();

        for (List<String> row : targetRows) {
            if (row == null || row.size() < 2) {
                continue;
            }

            String resourceName = row.get(0);
            String resourceType = row.get(1);

            if (matcher != null && !matcher.match(resourceName)) {
                continue;
            }

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
        String json = GsonUtils.GSON.toJson(getCopiedResourceMgr());
        Text.writeString(out, json);
    }

    private ResourceMgr getCopiedResourceMgr() throws IOException {
        ResourceMgr copied = new ResourceMgr();
        try {
            for (Map.Entry<String, Resource> entry : nameToResource.entrySet()) {
                copied.nameToResource.put(entry.getKey(), entry.getValue().getCopiedResourceSnapshot());
            }
        } catch (RuntimeException e) {
            throw new IOException("failed to snapshot resources for image", e);
        }
        return copied;
    }

    private Resource getCreateLogResource(Resource resource) throws DdlException {
        try {
            return resource.getCopiedResourceSnapshot();
        } catch (RuntimeException e) {
            throw new DdlException("failed to copy created resource " + resource.getName(), e);
        }
    }

    private Resource getAlterLogResource(Resource resource) {
        try {
            return resource.getAlterLogResourceSnapshot();
        } catch (Throwable t) {
            LOG.error("Fatal Error: failed to copy altered resource {}", resource.getName(), t);
            System.exit(-1);
            throw new IllegalStateException(t);
        }
    }

    public static ResourceMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        json = addLegacyClazzForResourcesIfMissing(json);
        return GsonUtils.GSON.fromJson(json, ResourceMgr.class);
    }

    // ResourceMgr image keeps Resource objects nested in nameToResource, so Resource.read() is not used.
    private static String addLegacyClazzForResourcesIfMissing(String json) {
        JsonElement jsonElement = JsonParser.parseString(json);
        if (!jsonElement.isJsonObject()) {
            return json;
        }

        JsonObject jsonObject = jsonElement.getAsJsonObject();
        JsonElement resourcesElement = jsonObject.get("nameToResource");
        if (resourcesElement == null || !resourcesElement.isJsonObject()) {
            return json;
        }

        boolean changed = false;
        for (Map.Entry<String, JsonElement> entry : resourcesElement.getAsJsonObject().entrySet()) {
            JsonElement resourceElement = entry.getValue();
            if (resourceElement != null && resourceElement.isJsonObject()) {
                changed |= Resource.addLegacyClazzIfMissing(resourceElement.getAsJsonObject());
            }
        }
        return changed ? jsonObject.toString() : json;
    }

    public class ResourceProcNode implements ProcNodeInterface {
        @Override
        public ProcResult fetchResult() {
            BaseProcResult result = new BaseProcResult();
            result.setNames(RESOURCE_PROC_NODE_TITLE_NAMES);

            for (Map.Entry<String, Resource> entry : nameToResource.entrySet()) {
                Resource resource = entry.getValue();
                // check resource privs
                if (!Env.getCurrentEnv().getAccessManager().checkResourcePriv(ConnectContext.get(), resource.getName(),
                                                                             PrivPredicate.SHOW_RESOURCES)) {
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
