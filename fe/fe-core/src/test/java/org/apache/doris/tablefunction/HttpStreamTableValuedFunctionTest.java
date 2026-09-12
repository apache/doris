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
import org.apache.doris.common.util.FileFormatConstants;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

public class HttpStreamTableValuedFunctionTest {
    @Test
    public void testRejectPathPartitionKeys() {
        Map<String, String> properties = Maps.newHashMap();
        properties.put(FileFormatConstants.PROP_FORMAT, FileFormatConstants.FORMAT_CSV);
        properties.put(FileFormatConstants.PROP_PATH_PARTITION_KEYS, "pt");

        AnalysisException exception = Assert.assertThrows(
                AnalysisException.class, () -> new HttpStreamTableValuedFunction(properties));

        Assert.assertTrue(exception.getMessage().contains("http_stream does not support path_partition_keys"));
    }
}
