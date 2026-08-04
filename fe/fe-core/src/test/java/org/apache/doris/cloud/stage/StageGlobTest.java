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

package org.apache.doris.cloud.stage;

import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Covers the glob analysis behind {@code COPY INTO ... FROM @stage('<glob>')}.
 *
 * <p>{@code analyzeGlob} turns a user glob into (prefix, hasWildcard) pairs: the prefix is what
 * gets listed from the object store, and the flag says whether the listing still has to be
 * filtered. Both matter for correctness, not just performance -- too short a prefix lists a whole
 * bucket, too long a prefix silently skips files the user asked for.
 *
 * <p>The expectations are literals rather than a comparison against hadoop's {@code GlobExpander}
 * / {@code GlobFilter}, which {@link GlobPatterns} was ported from. Equivalence to those classes
 * was established with a differential run over the cases below plus 20k fuzzed globs (matching
 * results, exception types and exception messages); pinning literals here keeps that behaviour
 * from drifting without making fe-core tests depend on hadoop again.
 */
public class StageGlobTest {

    private static String render(List<Pair<String, Boolean>> pairs) {
        return pairs.stream().map(p -> p.first + "|" + p.second).collect(Collectors.joining(", "));
    }

    private static String analyze(String glob) throws DdlException {
        return render(StageUtil.analyzeGlob("qid", glob));
    }

    @Test
    public void literalGlobListsExactlyThatKey() throws DdlException {
        // No wildcard anywhere: the whole path is the prefix and no filtering is needed.
        Assertions.assertEquals("a/b/c.csv|false", analyze("a/b/c.csv"));
        // A backslash escape makes '*' an ordinary character, so this is still a literal key.
        Assertions.assertEquals("a*b|false", analyze("a\\*b"));
    }

    @Test
    public void prefixStopsAtTheFirstWildcardCharacter() throws DdlException {
        // The prefix must keep every leading literal component, otherwise the listing widens to
        // the whole bucket.
        Assertions.assertEquals("data/|true", analyze("data/*.parquet"));
        Assertions.assertEquals("dt=2026-07-31/|true", analyze("dt=2026-07-31/*"));
        // ... and it must also keep the literal head *inside* the first wildcard component.
        Assertions.assertEquals("part-|true", analyze("part-?????.orc"));
        Assertions.assertEquals("dir/sub/pre|true", analyze("dir/sub/pre*post/x"));
        // Wildcard in the very first component leaves nothing to narrow the listing with.
        Assertions.assertEquals("|true", analyze("*.csv"));
        Assertions.assertEquals("|true", analyze("[abc]/x"));
    }

    @Test
    public void nullAndEmptyGlobsDifferInWhetherFilteringIsNeeded() throws DdlException {
        // No pattern at all -> list everything under the stage and keep it all.
        Assertions.assertEquals("|true", analyze(null));
        // An empty pattern is a literal, so nothing needs filtering.
        Assertions.assertEquals("|false", analyze(""));
    }

    @Test
    public void braceGroupWithSlashBecomesOneListingPerAlternative() throws DdlException {
        // This is the reason brace expansion runs before prefix analysis: each alternative is a
        // different object-store prefix, so one glob must produce two listings.
        Assertions.assertEquals("logs/2026/01/|true, logs/2026/02/|true",
                analyze("logs/{2026/01,2026/02}/*.csv"));
        Assertions.assertEquals("a/b|false, c/d|false", analyze("{a/b,c/d}"));
    }

    @Test
    public void braceGroupWithoutSlashStaysInsideOneComponent() throws DdlException {
        // No slash inside the braces -> no expansion; the component is simply wildcard-bearing.
        Assertions.assertEquals("pre|true", analyze("pre{a,b}post"));
        Assertions.assertEquals("|true", analyze("{a,b}.csv"));
    }

    @Test
    public void malformedGlobsAreRejectedRatherThanListedFromAWrongPrefix() {
        // Validation parity is the point: without it these would silently degrade into a prefix
        // listing instead of failing the statement.
        assertRejected("a[b", "Unclosed character class");
        assertRejected("a{b", "Unclosed group");
        assertRejected("a\\", "An escaped character does not present");
        // Character-class errors are left to the regex engine, exactly as hadoop did.
        assertRejected("[z-a]", "invalid character class range");
    }

    private static void assertRejected(String glob, String expectedFragment) {
        DdlException thrown = Assertions.assertThrows(DdlException.class,
                () -> StageUtil.analyzeGlob("qid", glob));
        Assertions.assertTrue(thrown.getMessage().contains(expectedFragment),
                "expected <" + expectedFragment + "> in: " + thrown.getMessage());
        Assertions.assertTrue(thrown.getMessage().contains("Failed to analyze glob: " + glob),
                thrown.getMessage());
    }

    @Test
    public void hasWildcardCountsOnlyTheFourGlobMetacharacters() throws Exception {
        Assertions.assertFalse(GlobPatterns.hasWildcard("abc"));
        Assertions.assertFalse(GlobPatterns.hasWildcard(""));
        Assertions.assertTrue(GlobPatterns.hasWildcard("a*"));
        Assertions.assertTrue(GlobPatterns.hasWildcard("a?"));
        Assertions.assertTrue(GlobPatterns.hasWildcard("a[b]"));
        Assertions.assertTrue(GlobPatterns.hasWildcard("a{b}"));
        // An escaped metacharacter is a literal ...
        Assertions.assertFalse(GlobPatterns.hasWildcard("a\\*b"));
        // ... and the closing/separator characters are not wildcards on their own.
        Assertions.assertFalse(GlobPatterns.hasWildcard("a,b"));
        Assertions.assertFalse(GlobPatterns.hasWildcard("a}b"));
        Assertions.assertFalse(GlobPatterns.hasWildcard("a]b"));
    }

    @Test
    public void expandFlattensOnlyBraceGroupsContainingASlash() throws Exception {
        // The four examples hadoop's GlobExpander javadoc pins, kept as the port's contract.
        Assertions.assertEquals(List.of("pa/bs", "pc/ds"), GlobPatterns.expand("p{a/b,c/d}s"));
        Assertions.assertEquals(List.of("a/b", "c/d", "{e,f}"), GlobPatterns.expand("{a/b,c/d,{e,f}}"));
        Assertions.assertEquals(List.of("{a,b}/b", "{a,b}/c/d", "{a,b}/e/f"),
                GlobPatterns.expand("{a,b}/{b,{c/d,e/f}}"));
        Assertions.assertEquals(List.of("{a,b}/c/d"), GlobPatterns.expand("{a,b}/{c/\\d}"));
        // Slash-free groups are left for per-component wildcard handling.
        Assertions.assertEquals(List.of("{a,b}.csv"), GlobPatterns.expand("{a,b}.csv"));
        Assertions.assertEquals(List.of("no-braces"), GlobPatterns.expand("no-braces"));
    }

    /**
     * Pins a PRE-EXISTING defect so the hadoop-removal port is provably behaviour-preserving --
     * this is documentation of current behaviour, not an endorsement of it.
     *
     * <p>{@code x/{y,z}} should yield two prefixes but yields only {@code x/y}: the group has no
     * slash, so it survives expansion and reaches {@code StageUtil.splitByComma}, whose closing
     * {@code if (start != str.length() - 1)} drops the final alternative whenever it is a single
     * character. The bug is in Doris's own helper, not in the ported hadoop code, and predates
     * this change; fixing it changes which files COPY INTO reads and belongs in its own commit.
     */
    @Test
    public void knownDefectSingleCharTrailingAlternativeIsDropped() throws DdlException {
        Assertions.assertEquals("x/y|false", analyze("x/{y,z}"));
        // Two characters instead of one and the alternative survives, confirming the cause.
        Assertions.assertEquals("x/y|false, x/zz|false", analyze("x/{y,zz}"));
    }
}
