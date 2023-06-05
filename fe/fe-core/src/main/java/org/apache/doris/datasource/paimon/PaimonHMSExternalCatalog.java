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

package org.apache.doris.datasource.paimon;

import org.apache.doris.datasource.CatalogProperty;
import org.apache.doris.datasource.property.PropertyConverter;
import org.apache.doris.datasource.property.constants.HMSProperties;
import org.apache.doris.datasource.property.constants.PaimonProperties;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.fs.FileIO;
import org.apache.paimon.fs.Path;
import org.apache.paimon.hive.HiveCatalog;
import org.apache.paimon.hive.HiveCatalogOptions;
import org.apache.paimon.options.CatalogOptions;
import org.apache.paimon.options.ConfigOption;
import org.apache.paimon.options.ConfigOptions;
import org.apache.paimon.options.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;




public class PaimonHMSExternalCatalog extends PaimonExternalCatalog {

    private static final Logger LOG = LoggerFactory.getLogger(PaimonHMSExternalCatalog.class);
    public static final String METASTORE = "metastore";
    public static final String METASTORE_HIVE = "hive";
    public static final String URI = "uri";
    private static final ConfigOption<String> METASTORE_CLIENT_CLASS =
            ConfigOptions.key("metastore.client.class")
            .stringType()
            .defaultValue("org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
            .withDescription(
                "Class name of Hive metastore client.\n"
                    + "NOTE: This class must directly implements "
                    + "org.apache.hadoop.hive.metastore.IMetaStoreClient.");

    public PaimonHMSExternalCatalog(long catalogId, String name, String resource,
                                    Map<String, String> props, String comment) {
        super(catalogId, name, comment);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
        paimonCatalogType = PAIMON_HMS;
    }

    @Override
    protected void initLocalObjectsImpl() {
        String metastoreUris = catalogProperty.getOrDefault(HMSProperties.HIVE_METASTORE_URIS, "");
        String warehouse = catalogProperty.getOrDefault(PaimonProperties.WAREHOUSE, "");
        Options options = new Options();
        options.set(PaimonProperties.WAREHOUSE, warehouse);
        // Currently, only supports hive
        options.set(METASTORE, METASTORE_HIVE);
        options.set(URI, metastoreUris);
        CatalogContext context = CatalogContext.create(options, getConfiguration());
        try {
            catalog = create(context);
        } catch (IOException e) {
            LOG.warn("failed to create paimon external catalog ", e);
            throw new RuntimeException(e);
        }
    }

    private Catalog create(CatalogContext context) throws IOException {
        Path warehousePath = new Path(context.options().get(CatalogOptions.WAREHOUSE));
        FileIO fileIO;
        fileIO = FileIO.get(warehousePath, context);
        String uri = context.options().get(CatalogOptions.URI);
        String hiveConfDir = context.options().get(HiveCatalogOptions.HIVE_CONF_DIR);
        String hadoopConfDir = context.options().get(HiveCatalogOptions.HADOOP_CONF_DIR);
        HiveConf hiveConf = HiveCatalog.createHiveConf(hiveConfDir, hadoopConfDir);

        // always using user-set parameters overwrite hive-site.xml parameters
        context.options().toMap().forEach(hiveConf::set);
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
        // set the warehouse location to the hiveConf
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, context.options().get(CatalogOptions.WAREHOUSE));

        String clientClassName = context.options().get(METASTORE_CLIENT_CLASS);

        return new HiveCatalog(fileIO, hiveConf, clientClassName, context.options().toMap());
    }
}
