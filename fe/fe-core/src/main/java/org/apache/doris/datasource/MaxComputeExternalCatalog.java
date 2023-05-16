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

package org.apache.doris.datasource;

import org.apache.doris.datasource.credentials.CloudCredential;
import org.apache.doris.datasource.property.constants.MCProperties;

import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MaxComputeExternalCatalog extends ExternalCatalog {
    private Odps odps;
    private String tunnelUrl;
    private static final String odpsUrlTemplate = "http://service.{}.maxcompute.aliyun.com/api";
    private static final String tunnelUrlTemplate = "http://dt.{}.maxcompute.aliyun.com";

    public MaxComputeExternalCatalog(long catalogId, String name, String resource, Map<String, String> props) {
        super(catalogId, name, InitCatalogLog.Type.MAX_COMPUTE);
        this.type = "max_compute";
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        Map<String, String> props = catalogProperty.getProperties();
        String region = props.get(MCProperties.REGION);
        String defaultProject = props.get(MCProperties.PROJECT);
        if (Strings.isNullOrEmpty(region)) {
            throw new IllegalArgumentException("Missing required property '" + MCProperties.REGION + "'.");
        }
        if (Strings.isNullOrEmpty(defaultProject)) {
            throw new IllegalArgumentException("Missing required property '" + MCProperties.PROJECT + "'.");
        }
        if (region.startsWith("oss-")) {
            // may use oss-cn-beijing, ensure compatible
            region = region.replace("oss-", "");
        }
        this.tunnelUrl = tunnelUrlTemplate.replace("{}", region);
        CloudCredential credential = MCProperties.getCredential(props);
        Account account = new AliyunAccount(credential.getAccessKey(), credential.getSecretKey());
        this.odps = new Odps(account);
        odps.setEndpoint(odpsUrlTemplate.replace("{}", region));
        odps.setDefaultProject(defaultProject);
    }

    public Odps getClient() {
        makeSureInitialized();
        return odps;
    }

    protected List<String> listDatabaseNames() {
        List<String> result = new ArrayList<>();
        try {
            result.add(odps.projects().get(odps.getDefaultProject()).getName());
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    @Override
    public boolean tableExist(SessionContext ctx, String dbName, String tblName) {
        makeSureInitialized();
        try {
            return odps.tables().exists(tblName);
        } catch (OdpsException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> listTableNames(SessionContext ctx, String dbName) {
        makeSureInitialized();
        List<String> result = new ArrayList<>();
        odps.tables().forEach(e -> result.add(e.getName()));
        return result;
    }

    /**
     * data tunnel url
     * @return tunnelUrl, required by jni scanner.
     */
    public String getTunnelUrl() {
        makeSureInitialized();
        return tunnelUrl;
    }
}
