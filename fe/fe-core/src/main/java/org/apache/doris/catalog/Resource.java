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
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.io.DeepCopy;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.policy.Policy;
import org.apache.doris.policy.PolicyTypeEnum;
import org.apache.doris.policy.StoragePolicy;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.NotifyUpdateStoragePolicyTask;

import com.google.common.base.Strings;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.stream.Collectors;

public abstract class Resource implements Writable, GsonPostProcessable {
    private static final Logger LOG = LogManager.getLogger(OdbcCatalogResource.class);
    public static final String REFERENCE_SPLIT = "@";

    public enum ResourceType {
        UNKNOWN,
        SPARK,
        ODBC_CATALOG,
        S3,
        JDBC,
        HDFS,
        HMS,
        ES;

        public static ResourceType fromString(String resourceType) {
            for (ResourceType type : ResourceType.values()) {
                if (type.name().equalsIgnoreCase(resourceType)) {
                    return type;
                }
            }
            return UNKNOWN;
        }
    }

    public enum ReferenceType {
        TVF, // table valued function
        LOAD,
        EXPORT,
        REPOSITORY,
        OUTFILE,
        TABLE,
        POLICY,
        CATALOG
    }

    @SerializedName(value = "name")
    protected String name;
    @SerializedName(value = "type")
    protected ResourceType type;
    @SerializedName(value = "references")
    protected Map<String, ReferenceType> references = Maps.newHashMap();

    public Resource() {
    }

    public Resource(String name, ResourceType type) {
        this.name = name;
        this.type = type;
    }

    public static Resource fromStmt(CreateResourceStmt stmt) throws DdlException {
        Resource resource = getResourceInstance(stmt.getResourceType(), stmt.getResourceName());
        resource.setProperties(stmt.getProperties());
        return resource;
    }

    public synchronized boolean removeReference(String referenceName, ReferenceType type) {
        String fullName = referenceName + REFERENCE_SPLIT + type.name();
        if (references.remove(fullName) != null) {
            LOG.info("Reference(type={}, name={}) is removed from resource {}, current set: {}",
                    type, referenceName, name, references);
            return true;
        }
        return false;
    }

    public synchronized boolean addReference(String referenceName, ReferenceType type) {
        String fullName = referenceName + REFERENCE_SPLIT + type.name();
        if (references.put(fullName, type) == null) {
            LOG.info("Reference(type={}, name={}) is added to resource {}, current set: {}",
                    type, referenceName, name, references);
            return true;
        }
        return false;
    }

    /**
     * Get resource instance by resource name and type
     * @param type
     * @param name
     * @return
     * @throws DdlException
     */
    private static Resource getResourceInstance(ResourceType type, String name) throws DdlException {
        Resource resource;
        switch (type) {
            case SPARK:
                resource = new SparkResource(name);
                break;
            case ODBC_CATALOG:
                resource = new OdbcCatalogResource(name);
                break;
            case S3:
                resource = new S3Resource(name);
                break;
            case JDBC:
                resource = new JdbcResource(name);
                break;
            case HDFS:
                resource = new HdfsResource(name);
                break;
            case HMS:
                resource = new HMSResource(name);
                break;
            case ES:
                resource = new EsResource(name);
                break;
            default:
                throw new DdlException("Unknown resource type: " + type);
        }

        return resource;
    }

    public String getName() {
        return name;
    }

    public ResourceType getType() {
        return type;
    }

    /**
     * Modify properties in child resources
     * @param properties
     * @throws DdlException
     */
    public void modifyProperties(Map<String, String> properties) throws DdlException {
        notifyUpdate();
    }

    /**
     * Check properties in child resources
     * @param properties
     * @throws AnalysisException
     */
    public void checkProperties(Map<String, String> properties) throws AnalysisException { }

    protected void replaceIfEffectiveValue(Map<String, String> properties, String key, String value) {
        if (!Strings.isNullOrEmpty(value)) {
            properties.put(key, value);
        }
    }

    /**
     * Set and check the properties in child resources
     */
    protected abstract void setProperties(Map<String, String> properties) throws DdlException;

    public abstract Map<String, String> getCopiedProperties();

    public void dropResource() throws DdlException {
        if (!references.isEmpty()) {
            String msg = String.join(", ", references.keySet());
            throw new DdlException(String.format("Resource %s is used by: %s", name, msg));
        }
    }

    /**
     * Fill BaseProcResult with different properties in child resources
     * ResourceMgr.RESOURCE_PROC_NODE_TITLE_NAMES format:
     * | Name | ResourceType | Key | Value |
     */
    protected abstract void getProcNodeData(BaseProcResult result);

    @Override
    public String toString() {
        return GsonUtils.GSON.toJson(this);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    public static Resource read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, Resource.class);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        // Resource is loaded from meta with older version
        if (references == null) {
            references = Maps.newHashMap();
        }
    }

    @Override
    public Resource clone() {
        Resource copied = DeepCopy.copy(this, Resource.class, FeConstants.meta_version);
        if (copied == null) {
            LOG.warn("failed to clone odbc resource: " + getName());
            return null;
        }
        return copied;
    }

    private void notifyUpdate() {
        references.entrySet().stream().collect(Collectors.groupingBy(Entry::getValue)).forEach((type, refs) -> {
            if (type == ReferenceType.POLICY) {
                SystemInfoService systemInfoService = Env.getCurrentSystemInfo();
                AgentBatchTask batchTask = new AgentBatchTask();

                Map<String, String> copiedProperties = getCopiedProperties();

                for (Long beId : systemInfoService.getBackendIds(true)) {
                    for (Map.Entry<String, ReferenceType> ref : refs) {
                        String policyName = ref.getKey().split(REFERENCE_SPLIT)[0];
                        List<Policy> policiesByType = Env.getCurrentEnv().getPolicyMgr()
                                .getCopiedPoliciesByType(PolicyTypeEnum.STORAGE);
                        Optional<Policy> findPolicy = policiesByType.stream()
                                .filter(p -> p.getType() == PolicyTypeEnum.STORAGE
                                        && policyName.equals(p.getPolicyName()))
                                .findAny();
                        LOG.info("find policy in {} ", policiesByType);
                        if (!findPolicy.isPresent()) {
                            return;
                        }
                        // add policy's coolDown ttl、coolDown data、policy name to map
                        Map<String, String> tmpMap = Maps.newHashMap(copiedProperties);
                        StoragePolicy used = (StoragePolicy) findPolicy.get();
                        tmpMap.put(StoragePolicy.COOLDOWN_DATETIME,
                                String.valueOf(used.getCooldownTimestampMs()));

                        final String[] cooldownTtl = {"-1"};
                        Optional.ofNullable(used.getCooldownTtl())
                                .ifPresent(date -> cooldownTtl[0] = used.getCooldownTtl());
                        tmpMap.put(StoragePolicy.COOLDOWN_TTL, cooldownTtl[0]);

                        tmpMap.put(StoragePolicy.MD5_CHECKSUM, used.getMd5Checksum());

                        NotifyUpdateStoragePolicyTask modifyS3ResourcePropertiesTask =
                                new NotifyUpdateStoragePolicyTask(beId, used.getPolicyName(), tmpMap);
                        LOG.info("notify be: {}, policy name: {}, "
                                        + "properties: {} to modify S3 resource batch task.",
                                beId, used.getPolicyName(), tmpMap);
                        batchTask.addTask(modifyS3ResourcePropertiesTask);
                    }
                }
                AgentTaskExecutor.submit(batchTask);
            } else if (type == ReferenceType.CATALOG) {
                for (Map.Entry<String, ReferenceType> ref : refs) {
                    String catalogName = ref.getKey().split(REFERENCE_SPLIT)[0];
                    CatalogIf catalog = Env.getCurrentEnv().getCatalogMgr().getCatalog(catalogName);
                    if (catalog == null) {
                        LOG.warn("Can't find the reference catalog {} for resource {}", catalogName, name);
                        continue;
                    }
                    if (!name.equals(catalog.getResource())) {
                        LOG.warn("Failed to update catalog {} for different resource "
                                + "names(resource={}, catalog.resource={})", catalogName, name, catalog.getResource());
                        continue;
                    }
                    catalog.notifyPropertiesUpdated();
                }
            }
        });
    }
}
