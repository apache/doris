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

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.proc.BaseProcResult;
import org.apache.doris.nereids.trees.plans.commands.CreateResourceCommand;
import org.apache.doris.nereids.trees.plans.commands.info.CreateResourceInfo;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
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
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.locks.ReentrantReadWriteLock;
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
        ES,
        AZURE,
        AI;

        public static ResourceType fromString(String resourceType) {
            if ("jfs".equalsIgnoreCase(resourceType) || "juicefs".equalsIgnoreCase(resourceType)) {
                return HDFS;
            }
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
    @SerializedName(value = "id")
    protected long id = -1;
    @SerializedName(value = "version")
    protected long version = -1;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
    private final Object alterLock = new Object();

    public void writeLock() {
        lock.writeLock().lock();
    }

    public void writeUnlock() {
        lock.writeLock().unlock();
    }

    public void readLock() {
        lock.readLock().lock();
    }

    public void readUnlock() {
        lock.readLock().unlock();
    }

    Object getAlterLock() {
        return alterLock;
    }

    // https://programmerr47.medium.com/gson-unsafe-problem-d1ff29d4696f
    // Resource subclass also MUST define default ctor, otherwise when reloading object from json
    // some not serialized field (i.e. `lock`) will be `null`.
    public Resource() {
    }

    public Resource(String name, ResourceType type) {
        this.name = name;
        this.type = type;
    }

    public static Resource fromCommand(CreateResourceCommand command) throws DdlException {
        CreateResourceInfo info = command.getInfo();
        Resource resource = getResourceInstance(info.getResourceType(), info.getResourceName());
        resource.id = Env.getCurrentEnv().getNextId();
        resource.version = 0;
        resource.setProperties(info.getProperties());
        return resource;
    }

    public long getId() {
        return this.id;
    }

    public long getVersion() {
        return this.version;
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
            case ODBC_CATALOG:
                resource = new OdbcCatalogResource(name);
                break;
            case S3:
                resource = new S3Resource(name);
                break;
            case AZURE:
                resource = new AzureResource(name);
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
                throw new DdlException("ES resource is no longer supported. Please use ES Catalog instead.");
            case AI:
                resource = new AIResource(name);
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
        notifyUpdate(properties);
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
    protected abstract void setProperties(ImmutableMap<String, String> properties) throws DdlException;

    public abstract Map<String, String> getCopiedProperties();

    public void dropResource() throws DdlException {
        synchronized (this) {
            if (!references.isEmpty()) {
                String msg = String.join(", ", references.keySet());
                throw new DdlException(String.format("Resource %s is used by: %s", name, msg));
            }
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
        return toJson();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        Text.writeString(out, toJson());
    }

    public static Resource read(DataInput in) throws IOException {
        String json = Text.readString(in);
        json = addLegacyClazzIfMissing(json);
        return GsonUtils.GSON.fromJson(json, Resource.class);
    }

    // Compatibility for legacy Resource JSON written without RuntimeTypeAdapterFactory's "clazz"
    // discriminator. This can be removed after such old metadata no longer needs to be supported.
    static String addLegacyClazzIfMissing(String json) {
        JsonElement jsonElement = JsonParser.parseString(json);
        if (!jsonElement.isJsonObject()) {
            return json;
        }

        JsonObject jsonObject = jsonElement.getAsJsonObject();
        if (addLegacyClazzIfMissing(jsonObject)) {
            return jsonObject.toString();
        }
        return json;
    }

    static boolean addLegacyClazzIfMissing(JsonObject jsonObject) {
        if (jsonObject.has("clazz") || !jsonObject.has("type")) {
            return false;
        }

        JsonElement typeElement = jsonObject.get("type");
        if (!typeElement.isJsonPrimitive()) {
            return false;
        }

        ResourceType resourceType = ResourceType.fromString(typeElement.getAsString());
        String clazz = getLegacyClazz(resourceType);
        if (clazz == null) {
            return false;
        }
        jsonObject.addProperty("clazz", clazz);
        return true;
    }

    private static String getLegacyClazz(ResourceType resourceType) {
        switch (resourceType) {
            case SPARK:
                return SparkResource.class.getSimpleName();
            case ODBC_CATALOG:
                return OdbcCatalogResource.class.getSimpleName();
            case S3:
                return S3Resource.class.getSimpleName();
            case JDBC:
                return JdbcResource.class.getSimpleName();
            case HDFS:
                return HdfsResource.class.getSimpleName();
            case HMS:
                return HMSResource.class.getSimpleName();
            case ES:
                return EsResource.class.getSimpleName();
            case AZURE:
                return AzureResource.class.getSimpleName();
            case AI:
                return AIResource.class.getSimpleName();
            default:
                return null;
        }
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
        Resource copied = getCopiedResourceSnapshot();
        if (copied == null) {
            LOG.warn("failed to clone odbc resource: " + getName());
            return null;
        }
        return copied;
    }

    final Resource getCopiedResourceSnapshot() {
        readLock();
        try {
            String json;
            synchronized (this) {
                json = GsonUtils.GSON.toJson(this);
            }
            return GsonUtils.GSON.fromJson(json, Resource.class);
        } finally {
            readUnlock();
        }
    }

    private String toJson() {
        readLock();
        try {
            synchronized (this) {
                return GsonUtils.GSON.toJson(this);
            }
        } finally {
            readUnlock();
        }
    }

    private void notifyUpdate(Map<String, String> properties) {
        synchronized (this) {
            references.entrySet().stream().collect(Collectors.groupingBy(Entry::getValue)).forEach((type, refs) -> {
                if (type == ReferenceType.CATALOG) {
                    // No longer support resource in Catalog.
                }
            });
        }
    }

    public void applyDefaultProperties() {}

    public static void registerUsedAIResourceName(String resourceName) {
        ConnectContext ctx = ConnectContext.get();
        if (ctx != null && ctx.getStatementContext() != null) {
            ctx.getStatementContext().registerUsedAIResourceName(resourceName);
        }
    }
}
