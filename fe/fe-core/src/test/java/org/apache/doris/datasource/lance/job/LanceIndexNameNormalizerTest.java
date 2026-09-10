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

package org.apache.doris.datasource.lance.job;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.Locale;

/**
 * Unit coverage for index-name normalization v1: the fence-key identity is exactly
 * {@code toLowerCase(Locale.ROOT)}, so the tests pin the locale-sensitive corners
 * (the Turkish dotted-I family) in addition to plain ASCII case folding and the
 * persisted-name byte bound.
 */
public class LanceIndexNameNormalizerTest {

    @Test
    public void normalizeLowercasesAscii() {
        Assertions.assertEquals("my_index", LanceIndexNameNormalizer.normalize("My_Index"));
        Assertions.assertEquals("idx", LanceIndexNameNormalizer.normalize("IDX"));
        Assertions.assertEquals("idx", LanceIndexNameNormalizer.normalize("idx"));
        Assertions.assertEquals("", LanceIndexNameNormalizer.normalize(""));
    }

    @Test
    public void normalizeKeepsDigitsAndUnderscores() {
        Assertions.assertEquals("idx_2024_v2", LanceIndexNameNormalizer.normalize("Idx_2024_V2"));
    }

    @Test
    public void normalizeUsesRootLocaleForConditionalMappings() {
        // İ (capital I with dot above) folds to i + combining dot above under the ROOT/default
        // mapping, never to the Turkish locale's dotless ı. This pins normalization v1 as
        // environment-independent.
        String dottedCapitalI = "İ";
        String normalized = LanceIndexNameNormalizer.normalize(dottedCapitalI);
        Assertions.assertEquals("i̇", normalized);
        Assertions.assertEquals(dottedCapitalI.toLowerCase(Locale.ROOT), normalized);
        Assertions.assertNotEquals(dottedCapitalI.toLowerCase(new Locale("tr")), normalized);
    }

    @Test
    public void normalizeHandlesUnicodeLettersAndLeavesCjkUntouched() {
        Assertions.assertEquals("äöü", LanceIndexNameNormalizer.normalize("ÄÖÜ"));
        Assertions.assertEquals("索引", LanceIndexNameNormalizer.normalize("索引"));
        Assertions.assertEquals("ß", LanceIndexNameNormalizer.normalize("ß"));
    }

    @Test
    public void normalizeRejectsNull() {
        Assertions.assertThrows(IllegalArgumentException.class, () -> LanceIndexNameNormalizer.normalize(null));
    }

    @Test
    public void isCaseOnlyDuplicateTrueOnlyForPureCaseDifference() {
        Assertions.assertTrue(LanceIndexNameNormalizer.isCaseOnlyDuplicate("MyIdx", "myidx"));
        Assertions.assertTrue(LanceIndexNameNormalizer.isCaseOnlyDuplicate("MYIDX", "myidx"));
        Assertions.assertFalse(LanceIndexNameNormalizer.isCaseOnlyDuplicate("myidx", "myidx"));
        Assertions.assertFalse(LanceIndexNameNormalizer.isCaseOnlyDuplicate("idxA", "idxB"));
        Assertions.assertFalse(LanceIndexNameNormalizer.isCaseOnlyDuplicate(null, "idx"));
        Assertions.assertFalse(LanceIndexNameNormalizer.isCaseOnlyDuplicate("idx", null));
    }

    @Test
    public void validateDisplayNameAcceptsBoundarySizes() {
        LanceIndexNameNormalizer.validateDisplayName("i");
        LanceIndexNameNormalizer.validateDisplayName(
                StringUtils.repeat("a", LanceIndexNameNormalizer.MAX_INDEX_NAME_BYTES));
        // 512 two-byte characters are exactly at the byte bound.
        String exactMultibyte = StringUtils.repeat("é", 512);
        Assertions.assertEquals(1024, exactMultibyte.getBytes(StandardCharsets.UTF_8).length);
        LanceIndexNameNormalizer.validateDisplayName(exactMultibyte);
    }

    @Test
    public void validateDisplayNameRejectsNullEmptyAndOversize() {
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexNameNormalizer.validateDisplayName(null));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexNameNormalizer.validateDisplayName(""));
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexNameNormalizer.validateDisplayName(
                        StringUtils.repeat("a", LanceIndexNameNormalizer.MAX_INDEX_NAME_BYTES + 1)));
        // 513 two-byte characters exceed the byte bound even though the char count is small.
        Assertions.assertThrows(IllegalArgumentException.class,
                () -> LanceIndexNameNormalizer.validateDisplayName(StringUtils.repeat("é", 513)));
    }

    @Test
    public void boundIsPinnedAt1024Utf8Bytes() {
        Assertions.assertEquals(1024, LanceIndexNameNormalizer.MAX_INDEX_NAME_BYTES);
    }
}
