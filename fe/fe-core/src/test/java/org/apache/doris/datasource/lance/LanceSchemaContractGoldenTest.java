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
import org.apache.doris.persist.gson.GsonUtils;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.lance.Dataset;
import org.lance.ReadOptions;
import org.lance.schema.LanceField;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * The FE half of the schema-contract cross-language lock (the BE half is
 * LanceIndexWorkerContractTest in be/test/lance/). Every golden fixture under
 * test/resources/lance/schema_contract_golden/ was produced by running this
 * builder over the persisted dataset next to it; this test reopens each dataset
 * pinned through the real JNI bindings and asserts the builder still reproduces
 * the recorded contract byte-for-byte (ordered, slot-for-slot equality).
 *
 * <p>Amendment G11: a missing fixture is a hard FAIL, never an assumption skip —
 * the presence check runs unconditionally, before any JNI guard. The BE unit
 * tests read the very same files from the repository checkout.
 */
public class LanceSchemaContractGoldenTest {

    /**
     * The locked fixture inventory: name, dataset_dir, version, column. The BE
     * contract test drives the same list (plus the verdict each fixture carries).
     */
    private static final Object[][] FIXTURES = {
            {"flat_s1_a", "s1.lance", 1, "a"},
            {"flat_s1_b", "s1.lance", 1, "b"},
            {"flat_s2_c", "s2.lance", 2, "c"},
            {"struct_s5_s", "s5.lance", 1, "s"},
            {"struct_s5_b", "s5.lance", 1, "b"},
            {"nested_struct_s18_b", "s18.lance", 1, "b"},
            {"list_s19_l", "s19.lance", 1, "l"},
            {"fsl_f32_s14_v", "s14.lance", 1, "v"},
            {"fsl_nullable_elem_s15_v", "s15.lance", 1, "v"},
            {"fsl_added_s16_v", "s16.lance", 2, "v"},
            {"fsl_f16_s22_v", "s22.lance", 1, "v"},
            {"struct_fsl_mix_r6_v", "r6.lance", 1, "v"},
            {"drop_hole_s11_c", "s11.lance", 2, "c"},
            {"drop_add_s21_d", "s21.lance", 3, "d"},
            {"restore_add_s20_d", "s20.lance", 4, "d"},
            {"restore_pinned_s20_v3_b", "s20.lance", 3, "b"},
            {"rename_s12_b2", "s12.lance", 2, "b2"},
            {"map_m1_m", "m1.lance", 2, "m"},
            {"largelist_m2_ll", "m2.lance", 2, "ll"},
            {"list_struct_m3_l", "m3.lance", 2, "l"},
            {"scalars_bool", "edge_scalars.lance", 1, "c_bool"},
            {"scalars_int8", "edge_scalars.lance", 1, "c_i8"},
            {"scalars_int16", "edge_scalars.lance", 1, "c_i16"},
            {"scalars_int32", "edge_scalars.lance", 1, "c_i32"},
            {"scalars_int64", "edge_scalars.lance", 1, "c_i64"},
            {"scalars_uint8", "edge_scalars.lance", 1, "c_u8"},
            {"scalars_uint16", "edge_scalars.lance", 1, "c_u16"},
            {"scalars_uint32", "edge_scalars.lance", 1, "c_u32"},
            {"scalars_uint64", "edge_scalars.lance", 1, "c_u64"},
            {"scalars_float16", "edge_scalars.lance", 1, "c_f16"},
            {"scalars_float32", "edge_scalars.lance", 1, "c_f32"},
            {"scalars_float64", "edge_scalars.lance", 1, "c_f64"},
            {"scalars_utf8", "edge_scalars.lance", 1, "c_utf8"},
            {"scalars_large_utf8", "edge_scalars.lance", 1, "c_large_utf8"},
            {"scalars_date32", "edge_scalars.lance", 1, "c_date32"},
            {"scalars_date64", "edge_scalars.lance", 1, "c_date64"},
            {"decimal128", "edge_decimal.lance", 1, "dec128"},
            {"decimal256", "edge_decimal.lance", 1, "dec256"},
            {"decimal_default_bw", "edge_decimal.lance", 1, "decdef"},
            {"timestamp_sec_nozone", "edge_timestamp.lance", 1, "ts_s"},
            {"timestamp_ms_utc", "edge_timestamp.lance", 1, "ts_ms_utc"},
            {"timestamp_us_shanghai", "edge_timestamp.lance", 1, "ts_us_sh"},
            {"timestamp_ns_la", "edge_timestamp.lance", 1, "ts_ns_la"},
            {"binary", "edge_binary.lance", 1, "c_bin"},
            {"largebinary", "edge_binary.lance", 1, "c_lbin"},
            {"fixedsizebinary", "edge_binary.lance", 1, "c_fsb"},
            {"time32_sec", "edge_time.lance", 1, "t32s"},
            {"time32_ms", "edge_time.lance", 1, "t32m"},
            {"time64_us", "edge_time.lance", 1, "t64u"},
            {"time64_ns", "edge_time.lance", 1, "t64n"},
            {"duration_sec", "edge_duration.lance", 1, "dur_s"},
            {"duration_ms", "edge_duration.lance", 1, "dur_m"},
            {"duration_us", "edge_duration.lance", 1, "dur_u"},
            {"duration_ns", "edge_duration.lance", 1, "dur_n"},
            {"null_column", "edge_null.lance", 1, "c_null"},
            {"empty_dataset_a", "empty_ds.lance", 1, "a"},
            {"fsl_uint8_unsupported", "fsl_uint8.lance", 1, "v"},
            {"fsl_int8_unsupported", "fsl_int8.lance", 1, "v"},
            {"nonascii_column_unsupported", "nonascii_col.lance", 1, "向量"},
            {"fold_duplicate_unsupported", "fold_dup.lance", 1, "Vec"},
    };

    /**
     * Amendment G11: every fixture file and every referenced dataset must be on the
     * classpath. This check never depends on the JNI bindings, so a missing asset
     * fails even on hosts where the native library cannot load.
     */
    @Test
    public void allGoldenFixturesAndDatasetsArePresent() {
        Assertions.assertTrue(FIXTURES.length >= 60,
                "the fixture inventory must cover the full binding-model matrix");
        for (Object[] fixture : FIXTURES) {
            String name = (String) fixture[0];
            String datasetDir = (String) fixture[1];
            URL golden = LanceSchemaContractGoldenTest.class
                    .getResource("/lance/schema_contract_golden/" + name + ".json");
            Assertions.assertNotNull(golden,
                    "missing golden fixture " + name + ".json (amendment G11: never skip)");
            URL dataset = LanceSchemaContractGoldenTest.class
                    .getResource("/lance/datasets/" + datasetDir + "/_versions");
            Assertions.assertNotNull(dataset,
                    "missing dataset " + datasetDir + " for fixture " + name);
        }
    }

    @Test
    public void feBuilderReproducesEveryGoldenContract() throws Exception {
        // Existence is asserted unconditionally above; only the JNI replay is guarded.
        LanceJniTestSupport.assumeJniBindingsLoadable();
        for (Object[] fixture : FIXTURES) {
            String name = (String) fixture[0];
            String datasetDir = (String) fixture[1];
            long version = ((Integer) fixture[2]).longValue();
            String column = (String) fixture[3];

            JsonObject wrapper;
            try (InputStream in = LanceSchemaContractGoldenTest.class
                    .getResourceAsStream("/lance/schema_contract_golden/" + name + ".json")) {
                Assertions.assertNotNull(in, "missing golden fixture " + name);
                wrapper = JsonParser.parseReader(
                        new InputStreamReader(in, StandardCharsets.UTF_8)).getAsJsonObject();
            }
            Assertions.assertEquals(datasetDir, wrapper.get("dataset_dir").getAsString(),
                    name + ": dataset_dir drifted from the fixture inventory");
            Assertions.assertEquals(version, wrapper.get("version").getAsLong(),
                    name + ": pinned version drifted from the fixture inventory");
            Assertions.assertEquals(column, wrapper.get("column").getAsString(),
                    name + ": column drifted from the fixture inventory");
            LanceIndexSchemaContract expected = GsonUtils.GSON.fromJson(
                    wrapper.getAsJsonObject("expected_contract"), LanceIndexSchemaContract.class);

            Path datasetPath = Paths.get(LanceSchemaContractGoldenTest.class
                    .getResource("/lance/datasets/" + datasetDir).toURI());
            String uri = "file://" + datasetPath.toAbsolutePath();
            try (Dataset dataset = Dataset.open(uri,
                    new ReadOptions.Builder().setVersion(version).build())) {
                Assertions.assertEquals(version, dataset.version(),
                        name + ": pinned open returned a different version");
                List<LanceField> fields = dataset.getLanceSchema().fields();
                LanceIndexSchemaContract rebuilt = LanceSchemaContractBuilder.build(fields, column);
                Assertions.assertEquals(expected, rebuilt,
                        name + ": FE builder no longer reproduces the golden contract "
                                + GsonUtils.GSON.toJson(expected));
            }
        }
    }
}
