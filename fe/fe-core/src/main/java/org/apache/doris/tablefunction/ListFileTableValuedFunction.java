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

package org.apache.doris.tablefunction;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FileResourceSnapshot;
import org.apache.doris.catalog.FileType;
import org.apache.doris.catalog.Resource;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.storage.S3ResourceCompat;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDataGenFunctionName;
import org.apache.doris.thrift.TDataGenScanRange;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TTVFListFileScanRange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/** Resource-backed S3 directory listing, executed by one backend. */
public class ListFileTableValuedFunction extends DataGenTableValuedFunction {
    public static final String NAME = "list_file";
    private static final ImmutableSet<String> PROPERTIES = ImmutableSet.of("resource", "uri", "recursive");

    private final String resourceName;
    private final String uri;
    private final String bucket;
    private final boolean recursive;

    public ListFileTableValuedFunction(Map<String, String> params) throws AnalysisException {
        Map<String, String> properties = new HashMap<>();
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = entry.getKey().toLowerCase(Locale.ROOT);
            if (!PROPERTIES.contains(key)) {
                throw new AnalysisException("'" + entry.getKey() + "' is invalid property");
            }
            properties.put(key, entry.getValue());
        }
        resourceName = properties.get("resource");
        uri = properties.get("uri");
        if (resourceName == null || resourceName.isEmpty()) {
            throw new AnalysisException("list_file resource is required");
        }
        if (uri == null || uri.isEmpty()) {
            throw new AnalysisException("list_file uri is required");
        }
        String recursiveValue = properties.getOrDefault("recursive", "false");
        if (!"true".equalsIgnoreCase(recursiveValue) && !"false".equalsIgnoreCase(recursiveValue)) {
            throw new AnalysisException("list_file recursive must be true or false");
        }
        recursive = Boolean.parseBoolean(recursiveValue);
        try {
            URI parsed = new URI(uri);
            if (!"s3".equals(parsed.getScheme()) || parsed.getRawAuthority() == null
                    || parsed.getRawAuthority().isEmpty() || parsed.getRawUserInfo() != null || parsed.getPort() != -1
                    || parsed.getRawQuery() != null || parsed.getRawFragment() != null) {
                throw new AnalysisException("list_file uri must be an s3://bucket/directory URI");
            }
            bucket = parsed.getRawAuthority();
        } catch (URISyntaxException e) {
            throw new AnalysisException("Invalid list_file uri: " + e.getMessage(), e);
        }
        Resource resource = checkS3Resource(ConnectContext.get());
        resource.readLock();
        try {
            checkBucket(resource.getCopiedProperties());
        } finally {
            resource.readUnlock();
        }
    }

    private Resource checkS3Resource(ConnectContext context) {
        Resource resource = FileResourceSnapshot.checkResource(resourceName, context);
        if (resource.getType() != Resource.ResourceType.S3) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException("list_file requires an S3 resource");
        }
        return resource;
    }

    private void checkBucket(Map<String, String> properties) {
        String resourceBucket = properties.getOrDefault(S3ResourceCompat.BUCKET,
                properties.get(S3ResourceCompat.Env.BUCKET));
        if (!bucket.equals(resourceBucket)) {
            throw new org.apache.doris.nereids.exceptions.AnalysisException(
                    "list_file uri bucket must match the resource bucket");
        }
    }

    @Override
    public void checkAuth(ConnectContext context) {
        checkS3Resource(context);
    }

    @Override
    public TDataGenFunctionName getDataGenFunctionName() {
        return TDataGenFunctionName.LIST_FILE;
    }

    @Override
    public String getTableName() {
        return "ListFileTableValuedFunction";
    }

    @Override
    public List<Column> getTableColumns() {
        return ImmutableList.of(
                new Column("path", Type.STRING, false),
                new Column("size", Type.BIGINT, false),
                new Column("modification_time", ScalarType.createDatetimeV2Type(3), true),
                new Column("file", FileType.create(), false));
    }

    @Override
    public List<TableValuedFunctionTask> getTasks() throws AnalysisException {
        ConnectContext context = ConnectContext.get();
        checkS3Resource(context);
        FileResourceSnapshot snapshot = FileResourceSnapshot.resolveForToFile(
                context.getStatementContext(), resourceName);
        checkBucket(snapshot.getProperties());
        List<Backend> backends = new ArrayList<>();
        for (Backend backend : Env.getCurrentSystemInfo().getBackendsByCurrentCluster().values()) {
            if (backend.isAlive()) {
                backends.add(backend);
            }
        }
        if (backends.isEmpty()) {
            String hints = Config.isCloudMode() ? ComputeGroupMgr.computeGroupNotFoundPromptMsg(null) : "";
            throw new AnalysisException("No Alive backends" + hints);
        }
        Collections.shuffle(backends);
        TTVFListFileScanRange params = new TTVFListFileScanRange().setResource(snapshot.toThrift())
                .setUri(uri).setRecursive(recursive);
        TScanRange range = new TScanRange().setDataGenScanRange(new TDataGenScanRange().setListFileParams(params));
        return ImmutableList.of(new TableValuedFunctionTask(backends.get(0), range));
    }
}
