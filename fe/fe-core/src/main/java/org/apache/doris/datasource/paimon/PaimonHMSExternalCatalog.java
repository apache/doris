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

import org.apache.hadoop.conf.Configuration;
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
import org.apache.paimon.utils.StringUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Map;
import javax.annotation.Nullable;




public class PaimonHMSExternalCatalog extends PaimonExternalCatalog {

    private static final ConfigOption<String> METASTORE_CLIENT_CLASS =
            ConfigOptions.key("metastore.client.class")
            .stringType()
            .defaultValue("org.apache.hadoop.hive.metastore.HiveMetaStoreClient")
            .withDescription(
                "Class name of Hive metastore client.\n"
                    + "NOTE: This class must directly implements "
                    + "org.apache.hadoop.hive.metastore.IMetaStoreClient.");

    public PaimonHMSExternalCatalog(long catalogId, String name, String resource, Map<String, String> props) {
        super(catalogId, name);
        props = PropertyConverter.convertToMetaProperties(props);
        catalogProperty = new CatalogProperty(resource, props);
    }

    @Override
    protected void initLocalObjectsImpl() {
        paimonCatalogType = PAIMON_HMS;
        String metastoreUris = catalogProperty.getOrDefault(HMSProperties.HIVE_METASTORE_URIS, "");
        String warehouse = catalogProperty.getOrDefault(PaimonProperties.PAIMON_WAREHOUSE, "");
        Options options = new Options();
        options.set(PaimonProperties.WAREHOUSE, warehouse);
        // Currently, only supports hive
        options.set(PaimonProperties.METASTORE, PaimonProperties.METASTORE_HIVE);
        options.set(PaimonProperties.URI, metastoreUris);
        CatalogContext context = CatalogContext.create(options, getConfiguration());
        try {
            catalog = create(context);
        } catch (IOException e) {
            e.printStackTrace();

        }
    }

    public Catalog create(CatalogContext context) throws IOException {
        Path warehousePath = new Path(context.options().get(CatalogOptions.WAREHOUSE));
        FileIO fileIO;
        fileIO = FileIO.get(warehousePath, context);
        String uri = context.options().get(CatalogOptions.URI);
        String hiveConfDir = context.options().get(HiveCatalogOptions.HIVE_CONF_DIR);
        String hadoopConfDir = context.options().get(HiveCatalogOptions.HADOOP_CONF_DIR);
        HiveConf hiveConf = createHiveConf(hiveConfDir, hadoopConfDir);

        // always using user-set parameters overwrite hive-site.xml parameters
        context.options().toMap().forEach(hiveConf::set);
        hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri);
        // set the warehouse location to the hiveConf
        hiveConf.set(HiveConf.ConfVars.METASTOREWAREHOUSE.varname, context.options().get(CatalogOptions.WAREHOUSE));

        String clientClassName = context.options().get(METASTORE_CLIENT_CLASS);

        return new HiveCatalog(fileIO, hiveConf, clientClassName, context.options().toMap());
    }

    public static HiveConf createHiveConf(@Nullable String hiveConfDir, @Nullable String hadoopConfDir) {
        Configuration hadoopConf = null;
        if (!StringUtils.isNullOrWhitespaceOnly(hadoopConfDir)) {
            hadoopConf = HiveCatalog.getHadoopConfiguration(hadoopConfDir);
            if (hadoopConf == null) {
                String possiableUsedConfFiles = "core-site.xml | hdfs-site.xml | yarn-site.xml | mapred-site.xml";
                throw new RuntimeException("Failed to load the hadoop conf from specified path:" + hadoopConfDir,
                        new FileNotFoundException("Please check the path none of the conf files ("
                            + possiableUsedConfFiles + ") exist in the folder."));
            }
        }

        if (hadoopConf == null) {
            hadoopConf = new Configuration();
        }

        if (hiveConfDir == null) {
            return new HiveConf(hadoopConf, HiveConf.class);
        } else {
            HiveConf.setHiveSiteLocation((URL) null);
            HiveConf.setLoadMetastoreConfig(false);
            HiveConf.setLoadHiveServer2Config(false);
            HiveConf hiveConf = new HiveConf(hadoopConf, HiveConf.class);
            org.apache.hadoop.fs.Path hiveSite = new org.apache.hadoop.fs.Path(hiveConfDir, "hive-site.xml");
            if (!hiveSite.toUri().isAbsolute()) {
                hiveSite = new org.apache.hadoop.fs.Path((new File(hiveSite.toString())).toURI());
            }

            try {
                InputStream inputStream = hiveSite.getFileSystem(hadoopConf).open(hiveSite);
                Throwable var6 = null;

                try {
                    hiveConf.addResource(inputStream, hiveSite.toString());
                    HiveCatalog.isEmbeddedMetastore(hiveConf);
                } catch (Throwable var16) {
                    var6 = var16;
                    throw var16;
                } finally {
                    if (inputStream != null) {
                        if (var6 != null) {
                            try {
                                inputStream.close();
                            } catch (Throwable var15) {
                                var6.addSuppressed(var15);
                            }
                        } else {
                            inputStream.close();
                        }
                    }

                }
            } catch (IOException var18) {
                throw new RuntimeException("Failed to load hive-site.xml from specified path:" + hiveSite, var18);
            }

            hiveConf.addResource(hiveSite);
            return hiveConf;
        }
    }
}
