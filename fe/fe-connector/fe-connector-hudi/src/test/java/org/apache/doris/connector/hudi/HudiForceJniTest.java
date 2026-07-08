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

package org.apache.doris.connector.hudi;

import org.apache.doris.connector.api.ConnectorSession;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

/**
 * Tests the {@code force_jni_scanner} escape hatch (legacy {@code HudiScanNode.canUseNativeReader()} /
 * {@code setScanParams} parity): reading the session flag and suppressing the no-delta-log native downgrade so a
 * native-eligible slice still reads via the JNI reader.
 */
public class HudiForceJniTest {

    @Test
    public void sessionFlagTrueEnablesForceJni() {
        // Pins the EXACT session key ("force_jni_scanner", same key the paimon connector reads and byte-identical
        // to SessionVariable.FORCE_JNI_SCANNER). MUTATION: wrong key -> red.
        Assertions.assertTrue(HudiScanPlanProvider.isForceJniScannerEnabled(
                sessionWithProps(Collections.singletonMap("force_jni_scanner", "true"))));
    }

    @Test
    public void sessionFlagUnsetDefaultsFalse() {
        // Default false (legacy default): normal reads must be unaffected. MUTATION: defaulting true -> red.
        Assertions.assertFalse(HudiScanPlanProvider.isForceJniScannerEnabled(
                sessionWithProps(Collections.emptyMap())));
    }

    @Test
    public void sessionFlagExplicitFalseIsFalse() {
        Assertions.assertFalse(HudiScanPlanProvider.isForceJniScannerEnabled(
                sessionWithProps(Collections.singletonMap("force_jni_scanner", "false"))));
    }

    @Test
    public void nullSessionDefaultsFalse() {
        Assertions.assertFalse(HudiScanPlanProvider.isForceJniScannerEnabled(null));
    }

    @Test
    public void forceJniSuppressesNoDeltaLogNativeDowngrade() {
        // A no-delta-log slice with a parquet base file that would normally downgrade to the native reader must
        // STAY on JNI when force_jni was engaged at plan time. This is the escape hatch's whole point.
        HudiScanRange range = noDeltaLogParquetRange(true);

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);

        Assertions.assertNotEquals(TFileFormatType.FORMAT_PARQUET, rangeDesc.getFormatType(),
                "force_jni must NOT downgrade a no-log slice to the native parquet reader");
        Assertions.assertTrue(formatDesc.getHudiParams().isSetColumnNames(),
                "JNI fileDesc fields must be set when the slice stays on JNI");
        Assertions.assertEquals("20240101000000000", formatDesc.getHudiParams().getInstantTime());
    }

    @Test
    public void withoutForceJniNoDeltaLogDowngradesToNative() {
        // The paired contrast: SAME inputs, force_jni off -> the no-log slice downgrades to native parquet and
        // sets no JNI fields. Together these pin that the ONLY difference is the force_jni flag.
        HudiScanRange range = noDeltaLogParquetRange(false);

        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(formatDesc, rangeDesc);

        Assertions.assertEquals(TFileFormatType.FORMAT_PARQUET, rangeDesc.getFormatType(),
                "without force_jni a no-log parquet slice downgrades to the native reader");
        Assertions.assertFalse(formatDesc.getHudiParams().isSetColumnNames(),
                "native downgrade must not set JNI fileDesc fields");
    }

    private static HudiScanRange noDeltaLogParquetRange(boolean forceJni) {
        return new HudiScanRange.Builder()
                .path("s3://bucket/t/base.parquet")
                .fileFormat("jni")
                .instantTime("20240101000000000")
                .serde("serde")
                .inputFormat("fmt")
                .basePath("s3://bucket/t")
                .dataFilePath("s3://bucket/t/base.parquet")
                .dataFileLength(456L)
                .columnNames(Arrays.asList("x"))
                .columnTypes(Arrays.asList("int"))
                .forceJni(forceJni)
                .build();
    }

    private static ConnectorSession sessionWithProps(Map<String, String> sessionProps) {
        return new ConnectorSession() {
            @Override
            public String getQueryId() {
                return "q";
            }

            @Override
            public String getUser() {
                return "u";
            }

            @Override
            public String getTimeZone() {
                return "UTC";
            }

            @Override
            public String getLocale() {
                return "en_US";
            }

            @Override
            public long getCatalogId() {
                return 0;
            }

            @Override
            public String getCatalogName() {
                return "c";
            }

            @Override
            public <T> T getProperty(String name, Class<T> type) {
                return null;
            }

            @Override
            public Map<String, String> getCatalogProperties() {
                return Collections.emptyMap();
            }

            @Override
            public Map<String, String> getSessionProperties() {
                return sessionProps;
            }
        };
    }
}
