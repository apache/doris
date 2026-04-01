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

package org.apache.doris.datasource;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.PrimitiveType;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Optional;

public class ExternalTableSchemaCacheDelegationTest {

    @Test
    public void testGetFullSchemaDelegatesToGetSchemaCacheValue() {
        List<Column> schema = Lists.newArrayList(new Column("c1", PrimitiveType.INT));
        ExternalTable table = new DelegatingExternalTable(Optional.of(new SchemaCacheValue(schema)));
        Assert.assertEquals(schema, table.getFullSchema());
    }

    @Test
    public void testGetFullSchemaReturnsNullWhenSchemaCacheMissing() {
        ExternalTable table = new DelegatingExternalTable(Optional.empty());
        Assert.assertNull(table.getFullSchema());
    }

    private static final class DelegatingExternalTable extends ExternalTable {
        private final Optional<SchemaCacheValue> schemaCacheValue;

        private DelegatingExternalTable(Optional<SchemaCacheValue> schemaCacheValue) {
            this.schemaCacheValue = schemaCacheValue;
        }

        @Override
        public Optional<SchemaCacheValue> getSchemaCacheValue() {
            return schemaCacheValue;
        }
    }
}
