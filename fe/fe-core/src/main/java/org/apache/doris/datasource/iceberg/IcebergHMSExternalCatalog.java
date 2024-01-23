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

package org.apache.doris.datasource.iceberg;

import org.apache.doris.catalog.AuthType;
import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.common.Config;
import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.HMSClientException;
import org.apache.doris.datasource.hive.HMSCachedClient;
import org.apache.doris.datasource.hive.HMSCachedClientFactory;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.hive.HiveCatalog;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class IcebergHMSExternalCatalog extends IcebergExternalCatalog {

    public IcebergHMSExternalCatalog(long catalogId, String name, String resource, Map<String, String> props,
            String comment) {
        super(catalogId, name, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        icebergCatalogType = ICEBERG_HMS;
        HiveCatalog hiveCatalog = new org.apache.iceberg.hive.HiveCatalog();
        hiveCatalog.setConf(getConfiguration());
        // initialize hive catalog
        Map<String, String> catalogProperties = new HashMap<>();
        String metastoreUris = catalogProperty.getOrDefault(HMSProperties.HIVE_METASTORE_URIS, "");
        catalogProperties.put(CatalogProperties.URI, metastoreUris);
        HiveConf hiveConf = new HiveConf();
        for (Map.Entry<String, String> kv : catalogProperty.getHadoopProperties().entrySet()) {
            hiveConf.set(kv.getKey(), kv.getValue());
        }
        hiveConf.set(HiveConf.ConfVars.METASTORE_CLIENT_SOCKET_TIMEOUT.name(),
                String.valueOf(Config.hive_metastore_client_timeout_second));
        String authentication = catalogProperty.getOrDefault(
                HdfsResource.HADOOP_SECURITY_AUTHENTICATION, "");
        if (AuthType.KERBEROS.getDesc().equals(authentication)) {
            hiveConf.set(HdfsResource.HADOOP_SECURITY_AUTHENTICATION, authentication);
            UserGroupInformation.setConfiguration(hiveConf);
            try {
                /**
                 * Because metastore client is created by using
                 * {@link org.apache.hadoop.hive.metastore.RetryingMetaStoreClient#getProxy}
                 * it will relogin when TGT is expired, so we don't need to relogin manually.
                 */
                UserGroupInformation.loginUserFromKeytab(
                        catalogProperty.getOrDefault(HdfsResource.HADOOP_KERBEROS_PRINCIPAL, ""),
                        catalogProperty.getOrDefault(HdfsResource.HADOOP_KERBEROS_KEYTAB, ""));
            } catch (IOException e) {
                throw new HMSClientException("login with kerberos auth failed for catalog %s", e, this.getName());
            }
        }
        HMSCachedClient cachedClient = HMSCachedClientFactory.createCachedClient(hiveConf, 1, null);
        String location = cachedClient.getCatalogLocation("hive");
        catalogProperties.put(CatalogProperties.WAREHOUSE_LOCATION, location);
        hiveCatalog.initialize(icebergCatalogType, catalogProperties);
        catalog = hiveCatalog;
    }
}
