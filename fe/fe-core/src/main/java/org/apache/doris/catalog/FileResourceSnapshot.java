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

import org.apache.doris.common.ErrorCode;
import org.apache.doris.datasource.storage.StorageAdapter;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.StatementContext;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TFileResourceSnapshot;
import org.apache.doris.thrift.TFileType;

import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

/** Immutable execution-local resource properties shared by FILE metadata operations. */
public final class FileResourceSnapshot {
    private static final String CACHE_PREFIX = FileResourceSnapshot.class.getName() + ":";

    private final String resourceName;
    private final TFileType fileType;
    private final ImmutableMap<String, String> properties;
    private final ImmutableMap<String, String> backendProperties;

    private FileResourceSnapshot(String resourceName, TFileType fileType, Map<String, String> properties,
            StorageAdapter storage) {
        this.resourceName = resourceName;
        this.fileType = fileType;
        this.properties = ImmutableMap.copyOf(properties);
        Map<String, String> backend = new HashMap<>(properties);
        backend.putAll(storage.getBackendConfigProperties());
        this.backendProperties = ImmutableMap.copyOf(backend);
    }

    /** Local catalog and USAGE checks only: never instantiate or contact a filesystem. */
    public static Resource checkResource(String resourceName, ConnectContext context) {
        Resource resource = Env.getCurrentEnv().getResourceMgr().getResource(resourceName);
        if (resource == null) {
            throw new AnalysisException("Can not find resource: " + resourceName);
        }
        if (!Env.getCurrentEnv().getAccessManager().checkResourcePriv(context, resourceName, PrivPredicate.USAGE)) {
            throw new AnalysisException(ErrorCode.ERR_RESOURCE_ACCESS_DENIED_ERROR.formatErrorMsg(
                    PrivPredicate.USAGE.getPrivs().toString(), resourceName));
        }
        return resource;
    }

    /** Select the resource's filesystem without inspecting the TO_FILE URI argument. */
    public static FileResourceSnapshot resolveForToFile(StatementContext statement, String resourceName) {
        // Memoize the supplier as well as registering it: StatementContext can invoke
        // its original supplier again on a later access within the same execution.
        return statement.getOrRegisterCache(CACHE_PREFIX + resourceName, Suppliers.memoize(() -> {
            Resource resource = checkResource(resourceName, statement.getConnectContext());
            resource.readLock();
            try {
                Map<String, String> properties = resource.getCopiedProperties();
                // Bind configuration once per execution snapshot without creating a client.
                StorageAdapter storage = StorageAdapter.of(new HashMap<>(properties));
                TFileType fileType;
                switch (resource.getType()) {
                    case S3:
                        fileType = TFileType.FILE_S3;
                        break;
                    case HDFS:
                        fileType = TFileType.FILE_HDFS;
                        break;
                    default:
                        fileType = ordinaryFileType(storage);
                        break;
                }
                return new FileResourceSnapshot(resourceName, fileType, properties, storage);
            } finally {
                resource.readUnlock();
            }
        }));
    }

    private static TFileType ordinaryFileType(StorageAdapter storage) {
        // Use the same property binding and filesystem families as the FILE TVF.
        // Binding configuration does not create a client or access a remote file.
        switch (storage.getType()) {
            case S3:
            case OSS:
            case OBS:
            case COS:
            case GCS:
            case MINIO:
            case OZONE:
            case AZURE:
                return TFileType.FILE_S3;
            case HDFS:
            case OSS_HDFS:
                return TFileType.FILE_HDFS;
            case LOCAL:
                return TFileType.FILE_LOCAL;
            case HTTP:
                return TFileType.FILE_HTTP;
            default:
                throw new AnalysisException("Could not find storage_type: " + storage.getStorageName());
        }
    }

    public TFileType getFileType() {
        return fileType;
    }

    /** Raw resource properties retained for FE validation, including LIST_FILE bucket checks. */
    public Map<String, String> getProperties() {
        return properties;
    }

    public TFileResourceSnapshot toThrift() {
        // Thrift objects/maps are mutable; never expose the query's cached map.
        return new TFileResourceSnapshot().setResourceName(resourceName).setFileType(fileType)
                .setProperties(new HashMap<>(backendProperties));
    }
}
