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

package org.apache.doris.connector.paimon;

import org.apache.doris.thrift.TFileRangeDesc;
import org.apache.doris.thrift.TPaimonReaderType;
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * FIX-READER-TYPE (upstream 3645dc94306, "[feature](be) Add file scanner v2 readers") — pins that
 * {@link PaimonScanRange#populateRangeParams} sets the BE thrift {@code TPaimonFileDesc.reader_type} so
 * BE's file-scanner-v2 selects the matching paimon reader stack:
 * <ul>
 *   <li>a JNI split (serialized {@code paimon.split} present) &rarr; {@link TPaimonReaderType#PAIMON_JNI};</li>
 *   <li>a native ORC/Parquet split &rarr; {@link TPaimonReaderType#PAIMON_NATIVE}.</li>
 * </ul>
 *
 * <p>WHY this matters: legacy {@code PaimonScanNode.setPaimonParams} set reader_type on every arm, but the
 * SPI migration to {@code PaimonScanRange} dropped it (the thrift {@code TPaimonFileDesc} was built without
 * reader_type), so BE could not tell which paimon reader stack a split wanted.
 *
 * <p>There is deliberately NO {@link TPaimonReaderType#PAIMON_CPP} arm: upstream #66008 removed it from
 * {@code PaimonScanNode.setPaimonParams} because a logical {@code DataSplit} may span several files and
 * file-scanner-v2 has no split-aware paimon-cpp adapter. Under the default {@code enable_file_scanner_v2
 * = true}, a PAIMON_CPP range is HARD-REJECTED ({@code is_supported_jni_table_format} &rarr;
 * {@code _validate_scan_range} &rarr; "FileScannerV2 does not support table format paimon") with no
 * per-range fallback to the V1 scanner that still implements {@code PaimonCppReader}. So the JNI arm must
 * answer PAIMON_JNI unconditionally, and {@code enable_paimon_cpp_reader} is a plan-path no-op
 * (see {@code PaimonScanPlanProviderTest.cppReaderSessionFlagNoLongerChangesThePlan}).
 */
public class PaimonScanRangeReaderTypeTest {

    private static TTableFormatFileDesc populate(PaimonScanRange range) {
        TTableFormatFileDesc formatDesc = new TTableFormatFileDesc();
        range.populateRangeParams(formatDesc, new TFileRangeDesc());
        return formatDesc;
    }

    @Test
    public void jniSplitSetsReaderTypeJniAndNoPaimonTable() {
        // Any JNI split (a Java-object-serialized DataSplit, or a non-DataSplit system split).
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("orc")
                .paimonSplit("java-serialized-split")      // JNI marker (paimon.split prop present)
                .build();

        // MUTATION: dropping setReaderType, or reinstating a cpp arm, turns this red — with reader_type
        // absent BE's V2 paimon reader can still infer JNI from paimon_split, but a PAIMON_CPP answer
        // fails the query outright (see the class javadoc).
        TTableFormatFileDesc formatDesc = populate(range);
        Assertions.assertTrue(formatDesc.getPaimonParams().isSetReaderType(),
                "a JNI split must set reader_type so BE can pick the reader stack");
        Assertions.assertEquals(TPaimonReaderType.PAIMON_JNI,
                formatDesc.getPaimonParams().getReaderType());
        // paimon_table (the table root path) is read ONLY by the V1 PaimonCppReader, so #66008 stopped
        // shipping it. MUTATION: re-adding setPaimonTable -> red.
        Assertions.assertFalse(formatDesc.getPaimonParams().isSetPaimonTable(),
                "paimon_table is cpp-reader-only state and must not be shipped");
    }

    @Test
    public void nativeSplitSetsReaderTypeNative() {
        // A native ORC/Parquet split: no paimonSplit marker -> native reader branch, always PAIMON_NATIVE.
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("orc")
                .path("s3://bkt/a/part-0.orc")
                .originalFilePath("oss://bucket.endpoint/a/part-0.orc")
                .schemaId(1L)
                .build();

        TTableFormatFileDesc formatDesc = populate(range);
        Assertions.assertTrue(formatDesc.getPaimonParams().isSetReaderType());
        Assertions.assertEquals(TPaimonReaderType.PAIMON_NATIVE,
                formatDesc.getPaimonParams().getReaderType());
        Assertions.assertEquals("oss://bucket.endpoint/a/part-0.orc",
                formatDesc.getPaimonParams().getOriginalFilePath(),
                "metadata columns must expose RawFile.path(), not the normalized BE read path");
    }
}
