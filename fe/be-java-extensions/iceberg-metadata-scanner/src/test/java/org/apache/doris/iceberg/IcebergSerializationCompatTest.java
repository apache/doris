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

package org.apache.doris.iceberg;

import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;
import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.Base64;

public class IcebergSerializationCompatTest {
    // An empty StaticDataTask serialized by Iceberg 1.10.1. It carries the old Schema serialVersionUID while
    // keeping all data neutral and local, so the fixture remains stable and safe to commit.
    private static final String ICEBERG_1_10_1_TASK = "rO0ABXNyACFvcmcuYXBhY2hlLmljZWJlcmcuU3RhdGljRGF0YVRhc2t2PvVIpr/rlAIABEwADG1ldGFkYXRh"
            + "RmlsZXQAHUxvcmcvYXBhY2hlL2ljZWJlcmcvRGF0YUZpbGU7TAAPcHJvamVjdGVkU2NoZW1hdAAbTG9yZy9h"
            + "cGFjaGUvaWNlYmVyZy9TY2hlbWE7WwAEcm93c3QAIFtMb3JnL2FwYWNoZS9pY2ViZXJnL1N0cnVjdExpa2U7"
            + "TAALdGFibGVTY2hlbWFxAH4AAnhwcHNyABlvcmcuYXBhY2hlLmljZWJlcmcuU2NoZW1hXonoLcvZFnYCAARJ"
            + "AA5oaWdoZXN0RmllbGRJZEkACHNjaGVtYUlkWwASaWRlbnRpZmllckZpZWxkSWRzdAACW0lMAAZzdHJ1Y3R0"
            + "ACtMb3JnL2FwYWNoZS9pY2ViZXJnL3R5cGVzL1R5cGVzJFN0cnVjdFR5cGU7eHAAAAABAAAAAHVyAAJbSU26"
            + "YCZ26rKlAgAAeHAAAAAAc3IAKW9yZy5hcGFjaGUuaWNlYmVyZy50eXBlcy5UeXBlcyRTdHJ1Y3RUeXBlY2OW"
            + "YF+O53QCAAFbAAZmaWVsZHN0AC1bTG9yZy9hcGFjaGUvaWNlYmVyZy90eXBlcy9UeXBlcyROZXN0ZWRGaWVs"
            + "ZDt4cgAob3JnLmFwYWNoZS5pY2ViZXJnLnR5cGVzLlR5cGUkTmVzdGVkVHlwZWpUXj112XcBAgAAeHB1cgAt"
            + "W0xvcmcuYXBhY2hlLmljZWJlcmcudHlwZXMuVHlwZXMkTmVzdGVkRmllbGQ7A428r/O1h1gCAAB4cAAAAAFz"
            + "cgAqb3JnLmFwYWNoZS5pY2ViZXJnLnR5cGVzLlR5cGVzJE5lc3RlZEZpZWxkRnIcmOgj/wICAAdJAAJpZFoA"
            + "CmlzT3B0aW9uYWxMAANkb2N0ABJMamF2YS9sYW5nL1N0cmluZztMAA5pbml0aWFsRGVmYXVsdHQAKExvcmcv"
            + "YXBhY2hlL2ljZWJlcmcvZXhwcmVzc2lvbnMvTGl0ZXJhbDtMAARuYW1lcQB+ABJMAAR0eXBldAAfTG9yZy9h"
            + "cGFjaGUvaWNlYmVyZy90eXBlcy9UeXBlO0wADHdyaXRlRGVmYXVsdHEAfgATeHAAAAABAHBwdAACaWRzcgAs"
            + "b3JnLmFwYWNoZS5pY2ViZXJnLnR5cGVzLlByaW1pdGl2ZUxpa2VIb2xkZXLbKTPuyM89cAIAAUwADHR5cGVB"
            + "c1N0cmluZ3EAfgASeHB0AANpbnRwdXIAIFtMb3JnL2FwYWNoZS5pY2ViZXJnLlN0cnVjdExpa2U7MJKGbVap"
            + "uFMCAAB4cAAAAABxAH4ACA==";

    @Test
    public void deserializesIceberg1101SystemTableTask() {
        FileScanTask task = IcebergSerializationCompat.deserializeFromBase64(ICEBERG_1_10_1_TASK);

        Assert.assertEquals("id", task.schema().columns().get(0).name());
    }

    @Test
    public void preservesCurrentIcebergSerialization() throws IOException {
        Schema expected = new Schema(Types.NestedField.required(1, "id", Types.IntegerType.get()));

        Schema actual = IcebergSerializationCompat.deserializeFromBase64(
                serializeToBase64(expected));

        Assert.assertTrue(expected.sameSchema(actual));
    }

    private static String serializeToBase64(Object value) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        try (ObjectOutputStream objectOutput = new ObjectOutputStream(output)) {
            objectOutput.writeObject(value);
        }
        return Base64.getEncoder().encodeToString(output.toByteArray());
    }
}
