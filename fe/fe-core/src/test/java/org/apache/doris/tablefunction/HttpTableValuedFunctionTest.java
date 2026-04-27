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

package org.apache.doris.tablefunction;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.FileFormatConstants;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class HttpTableValuedFunctionTest {
    @Test
    public void testRejectUnsafeEndpointByDefault() {
        String[] oldAllowlist = Config.http_tvf_allowed_private_endpoint_list;
        Config.http_tvf_allowed_private_endpoint_list = new String[] {};
        try {
            Map<String, String> properties = new HashMap<>();
            properties.put("uri", "http://127.0.0.1/data.csv");
            properties.put(FileFormatConstants.PROP_FORMAT, FileFormatConstants.FORMAT_CSV);

            AnalysisException exception = Assert.assertThrows(
                    AnalysisException.class, () -> new HttpTableValuedFunction(properties));
            Assert.assertTrue(exception.getMessage().contains("unsafe address"));
        } finally {
            Config.http_tvf_allowed_private_endpoint_list = oldAllowlist;
        }
    }
}
