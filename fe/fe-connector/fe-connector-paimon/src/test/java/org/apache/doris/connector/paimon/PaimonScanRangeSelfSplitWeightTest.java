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
import org.apache.doris.thrift.TTableFormatFileDesc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * FIX-A3 (P6 deviation) — pins that {@link PaimonScanRange} emits the BE thrift profile property
 * {@code paimon.self_split_weight} for every JNI split <b>including a genuine weight of 0</b>,
 * matching legacy {@code PaimonScanNode.setPaimonParams:274} (which calls {@code setSelfSplitWeight}
 * unconditionally on the JNI branch and never on the native branch).
 *
 * <p>The pre-fix {@code selfSplitWeight > 0} gate dropped a weight-0 JNI split (a non-DataSplit
 * system split with {@code rowCount()==0}, or a DataSplit whose files sum to {@code fileSize==0}), so
 * BE read the {@code -1} "unset" sentinel ({@code paimon_jni_reader.cpp:95}) instead of {@code 0} and
 * the {@code _max_time_split_weight_counter} profile counter was wrong. Profile-only — never touches
 * rows / counts / predicates / schema / routing.
 */
public class PaimonScanRangeSelfSplitWeightTest {

    private static TFileRangeDesc populate(PaimonScanRange range) {
        TFileRangeDesc rangeDesc = new TFileRangeDesc();
        range.populateRangeParams(new TTableFormatFileDesc(), rangeDesc);
        return rangeDesc;
    }

    @Test
    public void jniSplitWithZeroWeightEmitsZero() {
        // A JNI split whose genuine self-split weight is 0 (e.g. a rowCount()==0 system split).
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("orc")
                .paimonSplit("serialized-split")        // JNI marker
                .selfSplitWeight(0L)
                .build();

        // BE-visible (load-bearing): populateRangeParams must SET the thrift weight to 0 so BE reads 0
        // instead of its -1 unset default. MUTATION: restoring the old `selfSplitWeight > 0` gate ->
        // prop absent -> setSelfSplitWeight never called -> isSetSelfSplitWeight() false -> BE -1 -> red.
        TFileRangeDesc desc = populate(range);
        Assertions.assertTrue(desc.isSetSelfSplitWeight(),
                "a weight-0 JNI split must still set the thrift self_split_weight (legacy :274 parity)");
        Assertions.assertEquals(0L, desc.getSelfSplitWeight());
        Assertions.assertEquals("0", range.getProperties().get("paimon.self_split_weight"));
    }

    @Test
    public void jniSplitWithPositiveWeightEmitsWeight() {
        // Positive-case coverage (NEW — no prior test asserted self_split_weight).
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("orc")
                .paimonSplit("serialized-split")
                .selfSplitWeight(4096L)
                .build();

        TFileRangeDesc desc = populate(range);
        Assertions.assertTrue(desc.isSetSelfSplitWeight());
        Assertions.assertEquals(4096L, desc.getSelfSplitWeight());
        Assertions.assertEquals("4096", range.getProperties().get("paimon.self_split_weight"));
    }

    @Test
    public void nativeSplitNeverCarriesWeight() {
        // A native (ORC/Parquet) split: no paimonSplit marker; weight defaults to 0.
        PaimonScanRange range = new PaimonScanRange.Builder()
                .fileFormat("parquet")
                .path("s3://bkt/a/part-0.parquet")
                .build();

        // BE-visible parity: the native branch of populateRangeParams sets only the format, never the
        // weight, exactly like legacy PaimonScanNode's native branch (no setSelfSplitWeight call).
        TFileRangeDesc desc = populate(range);
        Assertions.assertFalse(desc.isSetSelfSplitWeight(),
                "native splits must not carry self_split_weight (legacy native branch never sets it)");

        // Gate-choice pin (NOT BE-visible): the JNI-marker gate must not add the key to a native
        // split's props map. MUTATION: switching the gate to `selfSplitWeight >= 0` makes a native
        // split (default weight 0) gain a `paimon.self_split_weight=0` key here -> red.
        Assertions.assertFalse(range.getProperties().containsKey("paimon.self_split_weight"),
                "the JNI-marker gate must not emit self_split_weight for a native split");
    }
}
