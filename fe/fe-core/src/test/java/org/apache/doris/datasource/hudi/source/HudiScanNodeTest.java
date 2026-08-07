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

package org.apache.doris.datasource.hudi.source;

import org.apache.doris.analysis.PartitionValue;
import org.apache.doris.catalog.ListPartitionItem;
import org.apache.doris.catalog.PartitionKey;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class HudiScanNodeTest {
    @Test
    public void testGetPartitionValuesPreservesNullLiteral() throws AnalysisException {
        List<PartitionValue> values = Arrays.asList(
                new PartitionValue("__HIVE_DEFAULT_PARTITION__", true),
                new PartitionValue("NULL"));
        List<Type> types = Arrays.asList(ScalarType.STRING, ScalarType.STRING);
        PartitionKey key = PartitionKey.createListPartitionKeyWithTypes(values, types, false);
        ListPartitionItem item = new ListPartitionItem(ImmutableList.of(key));

        Assert.assertEquals(Arrays.asList(null, "NULL"), HudiScanNode.getPartitionValues(item));
    }
}
