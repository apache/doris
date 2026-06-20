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

import org.apache.doris.datasource.property.storage.StorageProperties;
import org.apache.doris.foundation.property.ConnectorProperty;

import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Map;

public class PaimonJdbcMetaStoreProperties extends AbstractPaimonProperties {

    @ConnectorProperty(
            names = {"uri", "paimon.jdbc.uri"},
            required = true,
            description = "JDBC connection URI for the Paimon JDBC catalog."
    )
    private String uri = "";

    @ConnectorProperty(
            names = {"paimon.jdbc.user", "jdbc.user"},
            required = false,
            description = "Username for the Paimon JDBC catalog."
    )
    private String jdbcUser;

    @ConnectorProperty(
            names = {"paimon.jdbc.password", "jdbc.password"},
            required = false,
            sensitive = true,
            description = "Password for the Paimon JDBC catalog."
    )
    private String jdbcPassword;

    @ConnectorProperty(
            names = {"paimon.jdbc.driver_url", "jdbc.driver_url"},
            required = false,
            description = "JDBC driver JAR file path or URL. "
                    + "Can be a local file name (will look in $DORIS_HOME/plugins/jdbc_drivers/) "
                    + "or a full URL (http://, https://, file://)."
    )
    private String driverUrl;

    @ConnectorProperty(
            names = {"paimon.jdbc.driver_class", "jdbc.driver_class"},
            required = false,
            description = "JDBC driver class name. If specified with paimon.jdbc.driver_url, "
                    + "the driver will be loaded dynamically."
    )
    private String driverClass;

    protected PaimonJdbcMetaStoreProperties(Map<String, String> props) {
        super(props);
    }

    @Override
    public String getPaimonCatalogType() {
        return "jdbc";
    }

    @Override
    protected void checkRequiredProperties() {
        super.checkRequiredProperties();
        if (StringUtils.isBlank(warehouse)) {
            throw new IllegalArgumentException("Property warehouse is required.");
        }
    }

    /**
     * Wires the HDFS Kerberos authenticator on the plugin/cutover path (rereview2 M-8): the runtime
     * authenticator would otherwise stay the base no-op and {@code doAs} would be silently lost over
     * Kerberized HDFS. Mirrors HMS, which sets its authenticator in {@code initNormalizeAndCheckProps}.
     */
    @Override
    public void initExecutionAuthenticator(List<StorageProperties> storagePropertiesList) {
        initHdfsExecutionAuthenticator(storagePropertiesList);
    }
}
