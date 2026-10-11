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
import org.apache.doris.catalog.FileType;
import org.apache.doris.catalog.S3Resource;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.runtime.ThriftPlansBuilder;
import org.apache.doris.resource.computegroup.ComputeGroupMgr;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TDataGenFunctionName;
import org.apache.doris.thrift.TDataGenScanRange;
import org.apache.doris.thrift.TS3Resource;
import org.apache.doris.thrift.TScanRange;
import org.apache.doris.thrift.TTVFListFileScanRange;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

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
        if (uri == null) {
            throw new AnalysisException("list_file uri is required");
        }
        String recursiveValue = properties.getOrDefault("recursive", "false");
        if (!"true".equalsIgnoreCase(recursiveValue) && !"false".equalsIgnoreCase(recursiveValue)) {
            throw new AnalysisException("list_file recursive must be true or false");
        }
        recursive = Boolean.parseBoolean(recursiveValue);
        checkS3Resource(ConnectContext.get());
    }

    private S3Resource checkS3Resource(ConnectContext context) {
        return S3Resource.checkResource(resourceName, context);
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
                new Column("file", FileType.create(), false));
    }

    @Override
    public List<TableValuedFunctionTask> getTasks() throws AnalysisException {
        ConnectContext context = ConnectContext.get();
        checkS3Resource(context);
        TS3Resource resource = ThriftPlansBuilder.resolveS3Resource(context, resourceName);
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
        TTVFListFileScanRange params = new TTVFListFileScanRange().setResource(resource)
                .setUri(uri).setRecursive(recursive);
        TScanRange range = new TScanRange().setDataGenScanRange(new TDataGenScanRange().setListFileParams(params));
        return ImmutableList.of(new TableValuedFunctionTask(backends.get(0), range));
    }
}
