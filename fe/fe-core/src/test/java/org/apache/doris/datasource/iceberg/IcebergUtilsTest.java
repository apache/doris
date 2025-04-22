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

package org.apache.doris.datasource.iceberg;

import org.apache.iceberg.GenericManifestFile;
import org.apache.iceberg.GenericPartitionFieldSummary;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFile.PartitionFieldSummary;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.expressions.UnboundPredicate;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.types.Conversions;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.types.Types.LongType;
import org.apache.iceberg.types.Types.StructType;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IcebergUtilsTest {
    @Test
    public void testParseTableName() {
        try {
            IcebergHMSExternalCatalog c1 =
                    new IcebergHMSExternalCatalog(1, "name", null, new HashMap<>(), "");
            HiveCatalog i1 = IcebergUtils.createIcebergHiveCatalog(c1, "i1");
            Assert.assertTrue(getListAllTables(i1));

            IcebergHMSExternalCatalog c2 =
                    new IcebergHMSExternalCatalog(1, "name", null,
                            new HashMap<String, String>() {{
                                    put("list-all-tables", "true");
                                }},
                            "");
            HiveCatalog i2 = IcebergUtils.createIcebergHiveCatalog(c2, "i1");
            Assert.assertTrue(getListAllTables(i2));

            IcebergHMSExternalCatalog c3 =
                    new IcebergHMSExternalCatalog(1, "name", null,
                            new HashMap<String, String>() {{
                                    put("list-all-tables", "false");
                                }},
                        "");
            HiveCatalog i3 = IcebergUtils.createIcebergHiveCatalog(c3, "i1");
            Assert.assertFalse(getListAllTables(i3));
        } catch (Exception e) {
            Assert.fail();
        }
    }

    private boolean getListAllTables(HiveCatalog hiveCatalog) throws IllegalAccessException, NoSuchFieldException {
        Field declaredField = hiveCatalog.getClass().getDeclaredField("listAllTables");
        declaredField.setAccessible(true);
        return declaredField.getBoolean(hiveCatalog);
    }

    @Test
    public void testGetMatchingManifest() {

        // partition : 100 - 200
        GenericManifestFile f1 = getGenericManifestFileForDataTypeWithPartitionSummary(
                "manifest_f1.avro",
                Collections.singletonList(new GenericPartitionFieldSummary(
                    false, false, getByteBufferForLong(100), getByteBufferForLong(200))));

        // partition : 300 - 400
        GenericManifestFile f2 = getGenericManifestFileForDataTypeWithPartitionSummary(
                "manifest_f2.avro",
                Collections.singletonList(new GenericPartitionFieldSummary(
                    false, false, getByteBufferForLong(300), getByteBufferForLong(400))));

        // partition : 500 - 600
        GenericManifestFile f3 = getGenericManifestFileForDataTypeWithPartitionSummary(
                "manifest_f3.avro",
                    Collections.singletonList(new GenericPartitionFieldSummary(
                        false, false, getByteBufferForLong(500), getByteBufferForLong(600))));

        List<ManifestFile> manifestFiles = new ArrayList<ManifestFile>() {{
                add(f1);
                add(f2);
                add(f3);
            }};

        Schema schema = new Schema(
                StructType.of(
                        Types.NestedField.required(1, "id", LongType.get()),
                        Types.NestedField.required(2, "data", LongType.get()),
                        Types.NestedField.required(3, "par", LongType.get()))
                    .fields());

        // test empty partition spec
        HashMap<Integer, PartitionSpec> emptyPartitionSpecsById = new HashMap<Integer, PartitionSpec>() {{
                put(0, PartitionSpec.builderFor(schema).build());
            }};
        assertManifest(manifestFiles, emptyPartitionSpecsById, Expressions.alwaysTrue(), manifestFiles);

        // test long partition spec
        HashMap<Integer, PartitionSpec> longPartitionSpecsById = new HashMap<Integer, PartitionSpec>() {{
                put(0, PartitionSpec.builderFor(schema).identity("par").build());
            }};
        // 1. par > 10
        UnboundPredicate<Long> e1 = Expressions.greaterThan("par", 10L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(Expressions.alwaysTrue(), e1), manifestFiles);

        // 2. 10 < par < 90
        UnboundPredicate<Long> e2 = Expressions.greaterThan("par", 90L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e2), manifestFiles);

        // 3. 10 < par < 300
        UnboundPredicate<Long> e3 = Expressions.lessThan("par", 300L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e3), Collections.singletonList(f1));

        // 4. 10 < par < 400
        UnboundPredicate<Long> e4 = Expressions.lessThan("par", 400L);
        ArrayList<ManifestFile> expect1 = new ArrayList<ManifestFile>() {{
                add(f1);
                add(f2);
            }};
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e4), expect1);

        // 5. 10 < par < 501
        UnboundPredicate<Long> e5 = Expressions.lessThan("par", 501L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e1, e5), manifestFiles);

        // 6. 200 < par < 501
        UnboundPredicate<Long> e6 = Expressions.greaterThan("par", 200L);
        ArrayList<ManifestFile> expect2 = new ArrayList<ManifestFile>() {{
                add(f2);
                add(f3);
            }};
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(e6, e5), expect2);

        // 7. par > 600
        UnboundPredicate<Long> e7 = Expressions.greaterThan("par", 600L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(Expressions.alwaysTrue(), e7), Collections.emptyList());

        // 8. par < 100
        UnboundPredicate<Long> e8 = Expressions.lessThan("par", 100L);
        assertManifest(manifestFiles, longPartitionSpecsById, Expressions.and(Expressions.alwaysTrue(), e8), Collections.emptyList());
    }

    private void assertManifest(List<ManifestFile> dataManifests,
                                Map<Integer, PartitionSpec> specsById,
                                Expression dataFilter,
                                List<ManifestFile> expected) {
        CloseableIterable<ManifestFile> matchingManifest =
                IcebergUtils.getMatchingManifest(dataManifests, specsById, dataFilter);
        List<ManifestFile> ret = new ArrayList<>();
        matchingManifest.forEach(ret::add);
        ret.sort(Comparator.comparing(ManifestFile::path));
        Assert.assertEquals(expected, ret);
    }

    private ByteBuffer getByteBufferForLong(long num) {
        return Conversions.toByteBuffer(Types.LongType.get(), num);
    }

    private GenericManifestFile getGenericManifestFileForDataTypeWithPartitionSummary(
            String path,
            List<PartitionFieldSummary> partitionFieldSummaries) {
        return new GenericManifestFile(
            path,
            1024L,
            0,
            ManifestContent.DATA,
            1,
            1,
            123456789L,
            2,
            100,
            0,
            0,
            0,
            0,
            partitionFieldSummaries,
            null);
    }
}
