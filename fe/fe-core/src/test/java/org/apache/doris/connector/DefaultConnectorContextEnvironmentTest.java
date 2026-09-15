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

package org.apache.doris.connector;

import org.apache.doris.common.Config;
import org.apache.doris.connector.spi.DriverUrlPolicy;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;

public class DefaultConnectorContextEnvironmentTest {

    @Test
    public void forwardsConfiguredHadoopResourceDirectory() {
        String previous = Config.hadoop_config_dir;
        try {
            Config.hadoop_config_dir = "/configured/hadoop";
            DefaultConnectorContext context = new DefaultConnectorContext("test", 1L);
            Assertions.assertEquals("/configured/hadoop",
                    context.getEnvironment().get("hadoop_config_dir"));
        } finally {
            Config.hadoop_config_dir = previous;
        }
    }

    @Test
    public void forwardsTheDriverJarAllowListsUnderThePolicyKeys() {
        // The connectors read these through DriverUrlPolicy.Settings.fromContext, keyed by the fe.conf names;
        // the white list travels as one comma-joined value.
        String previousPath = Config.jdbc_driver_secure_path;
        String[] previousList = Config.jdbc_driver_url_white_list;
        try {
            Config.jdbc_driver_secure_path = "file:///opt/doris/jdbc_drivers";
            Config.jdbc_driver_url_white_list = new String[] {"http://a/x.jar", "http://b/y.jar"};
            Map<String, String> env = new DefaultConnectorContext("test", 1L).getEnvironment();
            Assertions.assertEquals("file:///opt/doris/jdbc_drivers", env.get(DriverUrlPolicy.ENV_DRIVER_SECURE_PATH));
            Assertions.assertEquals("http://a/x.jar,http://b/y.jar",
                    env.get(DriverUrlPolicy.ENV_DRIVER_URL_WHITE_LIST));
            Assertions.assertEquals(Config.jdbc_drivers_dir, env.get(DriverUrlPolicy.ENV_DRIVERS_DIR));
            Assertions.assertEquals(Arrays.asList("http://a/x.jar", "http://b/y.jar"),
                    DriverUrlPolicy.Settings.fromContext(new DefaultConnectorContext("test", 1L), null)
                            .getUrlWhiteList());
        } finally {
            Config.jdbc_driver_secure_path = previousPath;
            Config.jdbc_driver_url_white_list = previousList;
        }
    }

    @Test
    public void hasNoPluginFileStoreOutsideCloudMode() {
        // A non-cloud deployment has no object store to fetch a missing driver jar from: the policy then
        // reports the file as missing rather than the context inventing a path.
        Assertions.assertFalse(new DefaultConnectorContext("test", 1L)
                .fetchPluginFile("jdbc_drivers", "x.jar", "/tmp/x.jar").isPresent());
    }
}
