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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;

/**
 * Pins the family comparison rule of design section 3.4 (case-folded, underscore-insensitive)
 * and the case-only collision handling of section 4.1 used by admission's IF preflight.
 */
public class LanceIndexFamiliesTest {

    @Test
    public void testNormalizeFoldsCaseAndDropsUnderscores() {
        Assertions.assertEquals("ivfpq", LanceIndexFamilies.normalize("IVF_PQ"));
        Assertions.assertEquals("ivfpq", LanceIndexFamilies.normalize("ivf_pq"));
        Assertions.assertEquals("ivfpq", LanceIndexFamilies.normalize("Ivf_Pq"));
        Assertions.assertEquals("btree", LanceIndexFamilies.normalize("BTree"));
        Assertions.assertEquals("btree", LanceIndexFamilies.normalize("BTREE"));
        Assertions.assertEquals("bitmap", LanceIndexFamilies.normalize("BITMAP"));
        Assertions.assertEquals("labellist", LanceIndexFamilies.normalize("LABEL_LIST"));
        Assertions.assertEquals("labellist", LanceIndexFamilies.normalize("LabelList"));
        Assertions.assertEquals("vector", LanceIndexFamilies.normalize("VECTOR"));
        Assertions.assertEquals("scalar", LanceIndexFamilies.normalize("Scalar"));
        Assertions.assertEquals("", LanceIndexFamilies.normalize(""));
    }

    @Test
    public void testNormalizeUsesRootLocaleForConditionalMappings() {
        // İ (capital I with dot above) folds to i + combining dot above under ROOT, never to the
        // Turkish dotless ı, so family comparison is environment-independent.
        String dottedCapitalI = "İ";
        Assertions.assertEquals("i̇", LanceIndexFamilies.normalize(dottedCapitalI));
        Assertions.assertEquals(dottedCapitalI.toLowerCase(Locale.ROOT),
                LanceIndexFamilies.normalize(dottedCapitalI));
        Assertions.assertNotEquals(dottedCapitalI.toLowerCase(new Locale("tr")),
                LanceIndexFamilies.normalize(dottedCapitalI));
    }

    @Test
    public void testNormalizeRejectsNull() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexFamilies.normalize(null));
    }

    @Test
    public void testVectorUmbrellaAcceptsOnlySupportedVectorAlgorithms() {
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("IVF_PQ", "VECTOR"));
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("ivf_pq", "Vector"));
        // A concrete physical algorithm matches its own logical spelling.
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("IVF_PQ", "IVF_PQ"));
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("ivf_pq", "IVF_PQ"));
        // Unsupported vector algorithms never match the umbrella.
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("IVF_FLAT", "VECTOR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("IVF_SQ", "VECTOR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("IVF_HNSW_PQ", "VECTOR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("IVF_HNSW_SQ", "VECTOR"));
    }

    @Test
    public void testScalarUmbrellaAcceptsOnlySupportedScalarAlgorithms() {
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("BTREE", "SCALAR"));
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("BTree", "SCALAR"));
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("BITMAP", "SCALAR"));
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("bitmap", "Scalar"));
        // A concrete physical algorithm matches its own logical spelling.
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("BTREE", "BTREE"));
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("BTree", "BTREE"));
        Assertions.assertTrue(LanceIndexFamilies.isCompatible("BITMAP", "BITMAP"));
        // Unsupported scalar algorithms never match the umbrella.
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("LABEL_LIST", "SCALAR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("LabelList", "SCALAR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("ZONEMAP", "SCALAR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("BLOOM_FILTER", "SCALAR"));
    }

    @Test
    public void testCrossFamilyNeverMatches() {
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("BTREE", "VECTOR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("BITMAP", "VECTOR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("IVF_PQ", "SCALAR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("BTREE", "BITMAP"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("BITMAP", "BTREE"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("VECTOR", "SCALAR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("SCALAR", "VECTOR"));
    }

    @Test
    public void testDorisInternalIndexFamiliesNeverMatch() {
        // Doris internal index type names are outside the Lance family vocabulary; there is no
        // mapping between the two, so none of them may match a Lance physical family.
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("ANN", "VECTOR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("ANN", "SCALAR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("NGRAM_BF", "SCALAR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("BLOOMFILTER", "SCALAR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("INVERTED", "VECTOR"));
        Assertions.assertFalse(LanceIndexFamilies.isCompatible("INVERTED", "SCALAR"));
    }

    @Test
    public void testIsAmbiguousCaseCollision() {
        Assertions.assertTrue(LanceIndexFamilies.isAmbiguousCaseCollision(
                Arrays.asList("MyIdx", "myidx"), "myidx"));
        Assertions.assertTrue(LanceIndexFamilies.isAmbiguousCaseCollision(
                Arrays.asList("MyIdx", "MYIDX", "other"), "myidx"));
        Assertions.assertFalse(LanceIndexFamilies.isAmbiguousCaseCollision(
                Collections.singletonList("MyIdx"), "myidx"));
        Assertions.assertFalse(LanceIndexFamilies.isAmbiguousCaseCollision(
                Collections.singletonList("myidx"), "myidx"));
        Assertions.assertFalse(LanceIndexFamilies.isAmbiguousCaseCollision(
                Collections.singletonList("other"), "myidx"));
        Assertions.assertFalse(LanceIndexFamilies.isAmbiguousCaseCollision(
                Collections.emptyList(), "myidx"));
        // Exact duplicates of one display name are one stored name, not an ambiguity.
        Assertions.assertFalse(LanceIndexFamilies.isAmbiguousCaseCollision(
                Arrays.asList("myidx", "myidx"), "myidx"));
        // Underscores stay significant for names; only case folds.
        Assertions.assertFalse(LanceIndexFamilies.isAmbiguousCaseCollision(
                Arrays.asList("my_idx", "myidx"), "myidx"));
    }

    @Test
    public void testUniqueMatch() {
        Assertions.assertEquals("MyIdx", LanceIndexFamilies.uniqueMatch(
                Arrays.asList("MyIdx", "other"), "myidx"));
        Assertions.assertEquals("myidx", LanceIndexFamilies.uniqueMatch(
                Collections.singletonList("myidx"), "myidx"));
        Assertions.assertNull(LanceIndexFamilies.uniqueMatch(
                Arrays.asList("MyIdx", "myidx"), "myidx"));
        Assertions.assertNull(LanceIndexFamilies.uniqueMatch(
                Collections.singletonList("other"), "myidx"));
        Assertions.assertNull(LanceIndexFamilies.uniqueMatch(
                Collections.emptyList(), "myidx"));
        Assertions.assertEquals("myidx", LanceIndexFamilies.uniqueMatch(
                Arrays.asList("myidx", "myidx"), "myidx"));
    }
}
