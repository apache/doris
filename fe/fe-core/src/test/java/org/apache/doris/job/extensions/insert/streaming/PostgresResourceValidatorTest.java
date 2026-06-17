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

package org.apache.doris.job.extensions.insert.streaming;

import org.apache.doris.job.cdc.DataSourceConfigKeys;
import org.apache.doris.job.exception.JobException;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class PostgresResourceValidatorTest {

    // A 22-char CJK database name is length()==22 but 66 bytes in UTF-8; PG truncates it to 63 bytes.
    // The byte-based check must reject it before connecting (validate fails on the very first line).
    @Test
    public void testRejectMultibyteOverLongDatabaseName() {
        String dbName = StringUtils.repeat("库", 22);
        Assert.assertEquals(22, dbName.length());
        Map<String, String> props = new HashMap<>();
        props.put(DataSourceConfigKeys.DATABASE, dbName);
        JobException e = Assert.assertThrows(JobException.class,
                () -> PostgresResourceValidator.validate(props, "1", Collections.emptyList()));
        Assert.assertTrue(e.getMessage(), e.getMessage().contains("bytes"));
    }
}
