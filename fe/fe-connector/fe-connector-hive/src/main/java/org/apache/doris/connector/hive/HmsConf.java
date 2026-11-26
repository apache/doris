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

package org.apache.doris.connector.hive;

import org.apache.doris.connector.spi.ConnectorConf;
import org.apache.doris.connector.spi.ConnectorContext;

/**
 * The deployment-level settings of this plugin: one per FE, not one per catalog. Per-catalog settings
 * are the other half and live in {@link HiveCatalogProperties}.
 *
 * <p>They are read from the plugin's own {@code hms.conf}, each falling back to the {@code fe.conf} key
 * it used to live under, which still works. The file is named after
 * {@code ConnectorProvider.getType()} ({@code "hms"}), NOT after the plugin directory
 * ({@code plugins/connector/hive/}) — and this class is named after the file for that reason. Calling
 * it {@code HiveConf} would also collide by name with Hadoop's {@code org.apache.hadoop.hive.conf
 * .HiveConf}, which this plugin uses to talk to the metastore.
 */
public final class HmsConf {

    /** Storage format for a CREATE TABLE that names none itself. */
    public static final String CONF_DEFAULT_FILE_FORMAT = "default_file_format";

    /** The fe.conf name of {@link #CONF_DEFAULT_FILE_FORMAT}, forwarded through the engine environment. */
    public static final String ENV_HIVE_DEFAULT_FILE_FORMAT = "hive_default_file_format";

    /** Matches the legacy {@code Config.hive_default_file_format} default. */
    public static final String DEFAULT_FILE_FORMAT = "orc";

    /** Whether CREATE TABLE may create a bucketed hive table. */
    public static final String CONF_ENABLE_CREATE_BUCKET_TABLE = "enable_create_bucket_table";

    /** The fe.conf name of {@link #CONF_ENABLE_CREATE_BUCKET_TABLE}. */
    public static final String ENV_ENABLE_CREATE_HIVE_BUCKET_TABLE = "enable_create_hive_bucket_table";

    private HmsConf() {
    }

    /**
     * The storage format a CREATE TABLE falls back to. A statement may still override it per table with
     * the {@code file_format} property, which wins over both this and the fe.conf key.
     */
    public static String defaultFileFormat(ConnectorContext context) {
        return ConnectorConf.get(context, CONF_DEFAULT_FILE_FORMAT, ENV_HIVE_DEFAULT_FILE_FORMAT,
                DEFAULT_FILE_FORMAT);
    }

    public static boolean enableCreateBucketTable(ConnectorContext context) {
        return Boolean.parseBoolean(ConnectorConf.get(context, CONF_ENABLE_CREATE_BUCKET_TABLE,
                ENV_ENABLE_CREATE_HIVE_BUCKET_TABLE, "false"));
    }
}
