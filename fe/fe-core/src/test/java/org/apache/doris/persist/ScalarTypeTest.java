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

package org.apache.doris.persist;

import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.persist.gson.GsonUtils;

import org.junit.Assert;
import org.junit.Test;

public class ScalarTypeTest {
    @Test
    public void testScalarType() {
        ScalarType scalarType = new ScalarType(PrimitiveType.VARIANT);
        String json = GsonUtils.GSON.toJson(scalarType);
        System.out.println(json);
        ScalarType scalarType2 = GsonUtils.GSON.fromJson(json, ScalarType.class);
        Assert.assertFalse(scalarType2 instanceof VariantType);
        Assert.assertEquals(scalarType.getPrimitiveType(), scalarType2.getPrimitiveType());
        Assert.assertEquals(scalarType.getVariantMaxSubcolumnsCount(), 0);
        Assert.assertEquals(scalarType.getVariantEnableTypedPathsToSparse(), false);
        Assert.assertEquals(scalarType.getVariantMaxSparseColumnStatisticsSize(), 0);
    }
}
