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

package org.apache.doris.datasource.lance;

import org.apache.doris.datasource.lance.job.LanceIndexSchemaContract;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.lance.Dataset;
import org.lance.WriteParams;
import org.lance.schema.LanceField;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Pins the fixed-size-list contract semantics against a real on-disk dataset created through
 * the pinned lance-core JNI bindings (the jar bundles the native library, and the surefire
 * argLine already carries the opens the bindings need). A schema-only dataset is enough:
 * the manifest round trip behaves identically with and without data. What this fixture pins:
 * the LanceField tree of a fixed-size list never carries children (the element is collapsed
 * into the manifest logical-type string), the reconstructed Arrow view synthesizes the
 * element back, and the contract copies the element facts from that synthesized child —
 * including the always-true element nullability the manifest cannot store. Top-level
 * nullability and the scalar columns round trip exactly.
 */
public class LanceSchemaContractBuilderRealDatasetTest {
    @TempDir
    private Path tempDir;

    private BufferAllocator allocator;

    @AfterEach
    public void tearDown() {
        // The allocator backs the JNI export during create; close it deterministically.
        if (allocator != null) {
            allocator.close();
            allocator = null;
        }
    }

    @Test
    public void fixedListContractMatchesTheReconstructedSchemaOfARealDataset() throws Exception {
        Schema schema = new Schema(Arrays.asList(
                new Field("v_f32", FieldType.notNullable(new ArrowType.FixedSizeList(4)),
                        Collections.singletonList(Field.nullable("item",
                                new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE)))),
                new Field("v_f16", FieldType.nullable(new ArrowType.FixedSizeList(3)),
                        Collections.singletonList(Field.nullable("item",
                                new ArrowType.FloatingPoint(FloatingPointPrecision.HALF)))),
                Field.notNullable("c", new ArrowType.Int(32, true))));
        String uri = "file://" + tempDir.resolve("contract.lance").toAbsolutePath();

        allocator = new RootAllocator(Long.MAX_VALUE);
        try (Dataset created = Dataset.create(allocator, uri, schema,
                new WriteParams.Builder().build())) {
            Assertions.assertTrue(created.version() >= 1);
        }

        try (Dataset dataset = Dataset.open(uri)) {
            List<LanceField> fields = dataset.getLanceSchema().fields();

            // The storage-format fact the contract works around: fixed-size lists come back
            // with an empty LanceField tree — the element exists only in the logical-type
            // string and in the synthesized child of the reconstructed Arrow view.
            for (LanceField field : fields) {
                if (field.getType() instanceof ArrowType.FixedSizeList) {
                    Assertions.assertTrue(field.getChildren() == null
                            || field.getChildren().isEmpty(),
                            "fixed-size list field must not carry Lance children");
                }
            }

            LanceIndexSchemaContract.IndexedField f32 = indexedField(fields, "v_f32");
            Assertions.assertEquals("fixed_size_list", f32.getNormalizedType());
            Assertions.assertFalse(f32.isNullable());
            Assertions.assertEquals(4, f32.getFixedSizeListDimension());
            Assertions.assertEquals("float32", f32.getVectorElementType());
            Assertions.assertEquals(Boolean.TRUE, f32.getVectorElementNullable());

            LanceIndexSchemaContract.IndexedField f16 = indexedField(fields, "v_f16");
            Assertions.assertEquals("fixed_size_list", f16.getNormalizedType());
            Assertions.assertTrue(f16.isNullable());
            Assertions.assertEquals(3, f16.getFixedSizeListDimension());
            Assertions.assertEquals("float16", f16.getVectorElementType());
            Assertions.assertEquals(Boolean.TRUE, f16.getVectorElementNullable());

            LanceIndexSchemaContract.IndexedField scalar = indexedField(fields, "c");
            Assertions.assertEquals("int<32>", scalar.getNormalizedType());
            Assertions.assertFalse(scalar.isNullable());
            Assertions.assertNull(scalar.getFixedSizeListDimension());
            Assertions.assertNull(scalar.getVectorElementType());
            Assertions.assertNull(scalar.getVectorElementNullable());
        }
    }

    private static LanceIndexSchemaContract.IndexedField indexedField(List<LanceField> fields,
            String storedColumnName) throws Exception {
        LanceIndexSchemaContract contract = LanceSchemaContractBuilder.build(fields,
                storedColumnName);
        Assertions.assertEquals(1, contract.getFields().size());
        return contract.getFields().get(0);
    }
}
