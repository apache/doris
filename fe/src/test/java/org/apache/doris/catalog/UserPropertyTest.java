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

package org.apache.doris.catalog;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.load.DppConfig;
import org.apache.doris.mysql.privilege.UserProperty;

import com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.List;

public class UserPropertyTest {
    private FakeCatalog fakeCatalog;
    @Test
    public void testNormal() throws IOException, DdlException {
        // mock catalog
        fakeCatalog = new FakeCatalog();
        FakeCatalog.setMetaVersion(FeConstants.meta_version);

        UserProperty property = new UserProperty("root");
        property.getResource().updateGroupShare("low", 991);
        // To image
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outputStream = new DataOutputStream(byteStream);
        property.write(outputStream);
        outputStream.flush();
        DataInputStream inputStream = new DataInputStream(new ByteArrayInputStream(byteStream.toByteArray()));
        UserProperty newProperty = UserProperty.read(inputStream);

        Assert.assertEquals(991, newProperty.getResource().getShareByGroup().get("low").intValue());
    }

    @Test
    public void testUpdate() throws DdlException {
        List<Pair<String, String>> properties = Lists.newArrayList();
        properties.add(Pair.create("MAX_USER_CONNECTIONS", "100"));
        properties.add(Pair.create("resource.cpu_share", "101"));
        properties.add(Pair.create("quota.normal", "102"));
        properties.add(Pair.create("load_cluster.dpp-cluster.hadoop_palo_path", "/user/palo2"));
        properties.add(Pair.create("default_load_cluster", "dpp-cluster"));

        UserProperty userProperty = new UserProperty();
        userProperty.update(properties);
        Assert.assertEquals(100, userProperty.getMaxConn());
        Assert.assertEquals(101, userProperty.getResource().getResource().getByDesc("cpu_share"));
        Assert.assertEquals(102, userProperty.getResource().getShareByGroup().get("normal").intValue());
        Assert.assertEquals("/user/palo2", userProperty.getLoadClusterInfo("dpp-cluster").second.getPaloPath());
        Assert.assertEquals("dpp-cluster", userProperty.getDefaultLoadCluster());

        // fetch property
        List<List<String>> rows = userProperty.fetchProperty();
        for (List<String> row : rows) {
            String key = row.get(0);
            String value = row.get(1);

            if (key.equalsIgnoreCase("max_user_connections")) {
                Assert.assertEquals("100", value);
            } else if (key.equalsIgnoreCase("resource.cpu_share")) {
                Assert.assertEquals("101", value);
            } else if (key.equalsIgnoreCase("quota.normal")) {
                Assert.assertEquals("102", value);
            } else if (key.equalsIgnoreCase("load_cluster.dpp-cluster.hadoop_palo_path")) {
                Assert.assertEquals("/user/palo2", value);
            } else if (key.equalsIgnoreCase("default_load_cluster")) {
                Assert.assertEquals("dpp-cluster", value);
            }
        }

        // get cluster info
        DppConfig dppConfig = userProperty.getLoadClusterInfo("dpp-cluster").second;
        Assert.assertEquals(8070, dppConfig.getHttpPort());

        // set palo path null
        properties.clear();
        properties.add(Pair.create("load_cluster.dpp-cluster.hadoop_palo_path", null));
        userProperty.update(properties);
        Assert.assertEquals(null, userProperty.getLoadClusterInfo("dpp-cluster").second.getPaloPath());

        // remove dpp-cluster
        properties.clear();
        properties.add(Pair.create("load_cluster.dpp-cluster", null));
        Assert.assertEquals("dpp-cluster", userProperty.getDefaultLoadCluster());
        userProperty.update(properties);
        Assert.assertEquals(null, userProperty.getLoadClusterInfo("dpp-cluster").second);
        Assert.assertEquals(null, userProperty.getDefaultLoadCluster());
    }
}
