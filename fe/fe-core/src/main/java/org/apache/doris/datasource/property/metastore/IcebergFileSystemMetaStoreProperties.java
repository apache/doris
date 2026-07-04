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

package org.apache.doris.datasource.property.metastore;

import org.apache.doris.catalog.HdfsResource;
import org.apache.doris.datasource.iceberg.IcebergCatalogConstants;
import org.apache.doris.datasource.property.storage.HdfsProperties;
import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.kerberos.HadoopExecutionAuthenticator;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class IcebergFileSystemMetaStoreProperties extends AbstractIcebergProperties {

    @Override
    public String getIcebergCatalogType() {
        return IcebergCatalogConstants.ICEBERG_HADOOP;
    }

    public IcebergFileSystemMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public Catalog initCatalog(String catalogName, Map<String, String> catalogProps,
                               List<StorageProperties> storagePropertiesList) {
        try {
            Configuration configuration = new Configuration();
            toFileIOProperties(storagePropertiesList, catalogProps, configuration);
            catalogProps.put(CatalogProperties.CATALOG_IMPL, CatalogUtil.ICEBERG_CATALOG_HADOOP);
            buildExecutionAuthenticator(storagePropertiesList);
            return this.executionAuthenticator.execute(() ->
                    buildIcebergCatalog(catalogName, catalogProps, configuration));
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize iceberg filesystem catalog: "
                    + ExceptionUtils.getRootCauseMessage(e), e);
        }
    }

    /**
     * Wires the HDFS Kerberos authenticator on the plugin/cutover path: legacy iceberg only set the real
     * authenticator inside {@link #initCatalog} (dead on the plugin path, where the connector builds its own
     * catalog), so {@code doAs} was silently lost over Kerberized HDFS. {@code
     * PluginDrivenExternalCatalog.initPreExecutionAuthenticator} invokes this hook and then reads {@link
     * #getExecutionAuthenticator()}; reuse the existing {@link #buildExecutionAuthenticator} (Kerberos-only,
     * mirroring legacy — non-Kerberos HDFS keeps the base no-op, which needs no {@code doAs}). HMS sets its
     * authenticator in {@code initNormalizeAndCheckProps}; the cloud flavors have no HDFS UGI. Mirrors paimon
     * {@code Paimon{FileSystem,Jdbc}MetaStoreProperties.initExecutionAuthenticator}.
     */
    @Override
    public void initExecutionAuthenticator(List<StorageProperties> storagePropertiesList) {
        buildExecutionAuthenticator(storagePropertiesList);
    }

    /**
     * Bridges a hadoop-flavor {@code warehouse=hdfs://<ns>/path} to {@code fs.defaultFS=hdfs://<ns>} for
     * storage detection: legacy {@code IcebergHadoopExternalCatalog} parsed this in its constructor (dead on
     * the plugin/cutover path), and the shared HDFS detection ({@code HdfsProperties.guessIsMe}) never reads
     * {@code warehouse}. Without it an HA-nameservice catalog configured with only {@code warehouse} (relying
     * on classpath {@code core-site.xml}/{@code hdfs-site.xml} for the nameservice, no inline {@code uri}/
     * {@code fs.defaultFS}/{@code dfs.*}) would not bind HDFS storage with the warehouse nameservice. Non-hdfs
     * warehouses (and a blank one) derive nothing. The parse and the empty-nameservice message are verbatim
     * from the legacy constructor.
     */
    @Override
    public Map<String, String> getDerivedStorageProperties() {
        if (StringUtils.isBlank(warehouse) || !StringUtils.startsWith(warehouse, HdfsResource.HDFS_PREFIX)) {
            return Collections.emptyMap();
        }
        String nameService = StringUtils.substringBetween(warehouse, HdfsResource.HDFS_FILE_PREFIX, "/");
        if (StringUtils.isEmpty(nameService)) {
            throw new IllegalArgumentException("Unrecognized 'warehouse' location format"
                    + " because name service is required.");
        }
        return Collections.singletonMap(HdfsResource.HADOOP_FS_NAME, HdfsResource.HDFS_FILE_PREFIX + nameService);
    }

    private void buildExecutionAuthenticator(List<StorageProperties> storagePropertiesList) {
        if (storagePropertiesList.size() == 1 && storagePropertiesList.get(0) instanceof HdfsProperties) {
            HdfsProperties hdfsProps = (HdfsProperties) storagePropertiesList.get(0);
            if (hdfsProps.isKerberos()) {
                // NOTE: We deliberately do not install a custom FileIO implementation for Kerberos here.
                // Using a custom FileIO for Kerberos authentication may cause serialization issues when
                // accessing Iceberg system tables (e.g., history, snapshots, manifests).
                this.executionAuthenticator = new HadoopExecutionAuthenticator(hdfsProps.getHadoopAuthenticator());
            }
        }
    }
}
