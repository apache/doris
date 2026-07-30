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

package org.apache.doris.datasource.maxcompute;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.maxcompute.MCProperties;
import org.apache.doris.datasource.ExternalCatalog;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class MaxComputeExternalCatalogTest {
    @Test
    public void testSplitByteSizeErrorMessage() {
        Map<String, String> props = new HashMap<>();
        addRequiredProperties(props);
        props.put(MCProperties.SPLIT_STRATEGY, MCProperties.SPLIT_BY_BYTE_SIZE_STRATEGY);
        props.put(MCProperties.SPLIT_BYTE_SIZE, "1048576");

        MaxComputeExternalCatalog catalog = new MaxComputeExternalCatalog(1L, "mc_catalog", null, props, "");

        DdlException exception = Assert.assertThrows(DdlException.class, catalog::checkProperties);
        Assert.assertTrue(exception.getMessage().contains(
                MCProperties.SPLIT_BYTE_SIZE + " must be greater than or equal to 10485760"));
        Assert.assertFalse(exception.getMessage().contains(MCProperties.SPLIT_ROW_COUNT));
    }

    @Test
    public void testCheckWhenCreatingSkipsValidationByDefault() throws DdlException {
        Map<String, String> props = createRequiredProperties(true);
        TestMaxComputeExternalCatalog catalog = new TestMaxComputeExternalCatalog(props);

        catalog.checkWhenCreating();

        Assert.assertNull(catalog.checkedProjectName);
        Assert.assertNull(catalog.checkedNamespaceSchemaProjectName);
    }

    @Test
    public void testCheckWhenCreatingValidatesProjectWhenValidationEnabled() throws DdlException {
        Map<String, String> props = createRequiredProperties(false);
        props.put(ExternalCatalog.TEST_CONNECTION, "true");
        TestMaxComputeExternalCatalog catalog = new TestMaxComputeExternalCatalog(props);

        catalog.checkWhenCreating();

        Assert.assertEquals("mc_project", catalog.checkedProjectName);
        Assert.assertNull(catalog.checkedNamespaceSchemaProjectName);
    }

    @Test
    public void testCheckWhenCreatingValidatesSchemaWhenNamespaceSchemaEnabled() throws DdlException {
        Map<String, String> props = createRequiredProperties(true);
        props.put(ExternalCatalog.TEST_CONNECTION, "true");
        TestMaxComputeExternalCatalog catalog = new TestMaxComputeExternalCatalog(props);

        catalog.checkWhenCreating();

        Assert.assertNull(catalog.checkedProjectName);
        Assert.assertEquals("mc_project", catalog.checkedNamespaceSchemaProjectName);
    }

    @Test
    public void testCheckWhenCreatingReportsInaccessibleProject() {
        Map<String, String> props = createRequiredProperties(false);
        props.put(ExternalCatalog.TEST_CONNECTION, "true");
        TestMaxComputeExternalCatalog catalog = new TestMaxComputeExternalCatalog(props);
        catalog.projectExists = false;

        DdlException exception = Assert.assertThrows(DdlException.class, catalog::checkWhenCreating);

        Assert.assertTrue(exception.getMessage().contains("Failed to validate MaxCompute project 'mc_project'"));
        Assert.assertTrue(exception.getMessage().contains("does not exist or is not accessible"));
        Assert.assertNull(catalog.checkedNamespaceSchemaProjectName);
    }

    @Test
    public void testCheckWhenCreatingReportsInaccessibleNamespaceSchema() {
        Map<String, String> props = createRequiredProperties(true);
        props.put(ExternalCatalog.TEST_CONNECTION, "true");
        TestMaxComputeExternalCatalog catalog = new TestMaxComputeExternalCatalog(props);
        catalog.threeTierModel = false;

        DdlException exception = Assert.assertThrows(DdlException.class, catalog::checkWhenCreating);

        Assert.assertTrue(exception.getMessage().contains("Failed to validate MaxCompute project 'mc_project'"));
        Assert.assertTrue(exception.getMessage().contains("schema list is accessible"));
    }

    private static Map<String, String> createRequiredProperties(boolean enableNamespaceSchema) {
        Map<String, String> props = new HashMap<>();
        addRequiredProperties(props);
        props.put(MCProperties.ENABLE_NAMESPACE_SCHEMA, Boolean.toString(enableNamespaceSchema));
        return props;
    }

    private static void addRequiredProperties(Map<String, String> props) {
        props.put(MCProperties.PROJECT, "mc_project");
        props.put(MCProperties.ENDPOINT, "http://service.cn-beijing.maxcompute.aliyun-inc.com/api");
        props.put(MCProperties.ACCESS_KEY, "access_key");
        props.put(MCProperties.SECRET_KEY, "secret_key");
    }

    private static class TestMaxComputeExternalCatalog extends MaxComputeExternalCatalog {
        private boolean projectExists = true;
        private boolean threeTierModel = true;
        private String checkedProjectName;
        private String checkedNamespaceSchemaProjectName;

        private TestMaxComputeExternalCatalog(Map<String, String> props) {
            super(1L, "mc_catalog", null, props, "");
        }

        @Override
        protected boolean maxComputeProjectExists(String projectName) {
            checkedProjectName = projectName;
            return projectExists;
        }

        @Override
        protected void validateMaxComputeNamespaceSchemaAccess(String projectName) {
            checkedNamespaceSchemaProjectName = projectName;
            if (!threeTierModel) {
                throw new RuntimeException("schema list is not accessible");
            }
        }
    }
}
