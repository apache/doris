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

import org.apache.doris.persist.CreateTableInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.Collections;

public class ViewReplayTest {
    @Test
    public void testColumnLookupAfterDeserialization() throws Exception {
        View view = new View(1L, "v", Collections.singletonList(
                new Column("user_name", ScalarType.createVarcharType(20))));
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        view.write(new DataOutputStream(bytes));
        Table imageView = Table.read(new DataInputStream(new ByteArrayInputStream(bytes.toByteArray())));
        bytes.reset();
        new CreateTableInfo("test", 2L, view).write(new DataOutputStream(bytes));
        Table journalView = CreateTableInfo.read(new DataInputStream(
                new ByteArrayInputStream(bytes.toByteArray()))).getTable();
        for (Table restored : new Table[] {imageView, journalView}) {
            Assertions.assertInstanceOf(View.class, restored);
            Assertions.assertEquals(1, restored.getFullSchema().size());
            Assertions.assertSame(restored.getFullSchema().get(0), restored.getColumn("user_name"));
            Assertions.assertSame(restored.getColumn("user_name"), restored.getColumn("USER_NAME"));
            Assertions.assertNull(restored.getColumn("missing"));
        }
    }
}
