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

package org.apache.doris.connector.iceberg;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests for {@link IcebergColumnHandle} (mirrors the paimon connector's {@code PaimonColumnHandle}). */
public class IcebergColumnHandleTest {

    @Test
    public void carriesNameAndFieldId() {
        IcebergColumnHandle handle = new IcebergColumnHandle("name", 42);
        Assertions.assertEquals("name", handle.getName());
        Assertions.assertEquals(42, handle.getFieldId());
    }

    @Test
    public void equalityIsByNameOnly() {
        // WHY: PluginDrivenScanNode.buildColumnHandles looks a handle up by slot NAME, so identity is the name
        // (the same key getColumnHandles keys the map by). Two handles with the same name but different field
        // ids are equal. MUTATION: include fieldId in equals/hashCode -> the two below compare unequal -> red.
        Assertions.assertEquals(new IcebergColumnHandle("c", 1), new IcebergColumnHandle("c", 2));
        Assertions.assertEquals(
                new IcebergColumnHandle("c", 1).hashCode(), new IcebergColumnHandle("c", 2).hashCode());
        Assertions.assertNotEquals(new IcebergColumnHandle("a", 1), new IcebergColumnHandle("b", 1));
    }
}
