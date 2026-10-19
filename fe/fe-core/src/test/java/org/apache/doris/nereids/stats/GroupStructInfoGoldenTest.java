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

package org.apache.doris.nereids.stats;

import org.apache.doris.analysis.SearchDslParser;
import org.apache.doris.catalog.DatabaseIf;
import org.apache.doris.catalog.Function.NullableMode;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.catalog.FunctionVolatility;
import org.apache.doris.datasource.CatalogIf;
import org.apache.doris.datasource.ExternalTable;
import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.IsNull;
import org.apache.doris.nereids.trees.expressions.LessThan;
import org.apache.doris.nereids.trees.expressions.LessThanEqual;
import org.apache.doris.nereids.trees.expressions.MatchAll;
import org.apache.doris.nereids.trees.expressions.Mod;
import org.apache.doris.nereids.trees.expressions.Multiply;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.OrderExpression;
import org.apache.doris.nereids.trees.expressions.SearchExpression;
import org.apache.doris.nereids.trees.expressions.Slot;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.functions.Udf;
import org.apache.doris.nereids.trees.expressions.functions.agg.Count;
import org.apache.doris.nereids.trees.expressions.functions.agg.Max;
import org.apache.doris.nereids.trees.expressions.functions.agg.Min;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeSplit;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.functions.scalar.ArrayCount;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Lambda;
import org.apache.doris.nereids.trees.expressions.functions.udf.JavaUdf;
import org.apache.doris.nereids.trees.expressions.functions.window.Ntile;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.RelationId;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.RepeatType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalCTEConsumer;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFileScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalPartitionTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalSort;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.BooleanType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.IntegerType;
import org.apache.doris.nereids.types.VarcharType;
import org.apache.doris.nereids.util.MutableState;
import org.apache.doris.nereids.util.PlanConstructor;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.hash.Hashing;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Characterization ("golden") corpus of the hbo struct info rendering: the canonical string, the
 * fingerprint input (shape), the sha256 fingerprint, the scan tokens, the simplified display string
 * and the text based mappings ({@code toNoLiteral}, {@code stripScanBaseline},
 * {@code encodeLiteralValue} / {@code decodeLiteralValue}).
 *
 * <p><b>Why a corpus of exact strings.</b> {@code sha256(shape)} is the hbo cache key: it is stored
 * in {@code __internal_schema.hbo_plan_statistics}, printed by {@code EXPLAIN} (with
 * {@code set show_hbo_fingerprint=true}) and pasted back by users as
 * {@code HBO SET STATISTICS ... STRUCT='...'}. The suites written so far ({@code GroupStructInfo*}
 * {@code Test}, {@code HboStructFreshnessTest}, ...) assert relational properties only ("the shape
 * contains a top-n bound", "two keys differ", "the child does not appear twice"), so every one of them
 * stays green when a refactor moves <b>every</b> key by one byte - and the keys of the entries a user
 * pinned in a cluster, or that a running FE learned, would silently stop matching. Only a corpus of
 * exact strings and exact hashes can notice that. This class is that corpus: every case is rendered
 * by the implementation and compared byte for byte with
 * {@code src/test/resources/hbo-struct-info-golden.txt}. A change of the rendering must therefore be
 * a deliberate change of the resource file - regenerated with {@code -Dhbo.golden.dump=true} and
 * reviewed line by line as what it is: a change of the key of the affected shapes.
 *
 * <p><b>What the file holds.</b>
 * <ul>
 *   <li>the PLAN section: plans built in process (memo level, the same way
 *       {@code GroupStructInfoCompositionTest} builds them), each with both literal modes, the
 *       canonical string, the shape, the fingerprint, the scans in render order and the data state
 *       of every scan;</li>
 *   <li>the TEXT section: table driven pure function cases - {@code toNoLiteral} on the corners the
 *       folder was hardened for (a data type with parentheses, a protected header, an unbalanced
 *       parenthesis), {@code stripScanBaseline} (a token with and without a data state, a token
 *       carrying {@code #pt:} / {@code #tb:} / {@code #ix:}, escaped identifier text which must not
 *       be read as a token), the literal value codec, {@code SimpleStructInfo.render} and the
 *       {@code HboScanDescriptor} parse / render round trip.</li>
 * </ul>
 *
 * <p>The prefix of every key is frozen as well: the existing {@code hbo_persist_internal_db_test}
 * hardcodes one canonical string end to end, and this corpus complements it instead of replacing it
 * (that suite pins what is stored in the internal table, this one pins every shape the traversal can
 * render from a hand-built memo - the storage layer aggregate below a scan is the one rendering this
 * suite cannot build, because the memo refuses a physical plan, so its form
 * ({@code A{sl,op=X}(S{...})}) is asserted by the rendering code and its callers rather than by a case
 * here).
 *
 * <p><b>The CLUSTER section.</b> {@code hbo-struct-info-golden-cluster.txt} is <b>captured data</b>,
 * not generated: it holds the annotation lines a live FE printed for the shapes in the task
 * (filter, join chain, outer join, top-n, limit, window, generate, repeat, set operations,
 * partition / tablet selection, and the skip reasons). The test never compares it byte for byte
 * (the data state in it moves with the cluster), it asserts the invariants that tie the recorded
 * strings to the recorded hashes: the two literal modes of one node fold into each other, the
 * sha256 of the stripped struct is the fingerprint printed next to it, the scan tokens parse and
 * their identity does not depend on the literal mode, and every skip reason is one of the frozen
 * texts. Regenerate that file only deliberately, by re-running the capture documented in its header.
 *
 * <p><b>Generating this file.</b>
 * <pre>
 *   cd fe
 *   mvn -o -pl fe-core -am test -Dtest='GroupStructInfoGoldenTest' -DfailIfNoTests=false \
 *     -Dskip.doc=true -Dhbo.golden.dump=true
 *   # add -Dmaven.repo.local=PATH when the environment resolves offline from a local repository
 *   # and then, to prove the corpus asserts clean:
 *   mvn -o -pl fe-core -am test -Dtest='GroupStructInfoGoldenTest' -DfailIfNoTests=false \
 *     -Dskip.doc=true
 * </pre>
 * The dump rewrites {@code hbo-struct-info-golden.txt} only; the cluster file is never written by a
 * test.
 */
public class GroupStructInfoGoldenTest {
    private static final GroupStructInfo.LiteralMode WITH_LITERAL = GroupStructInfo.LiteralMode.WITH_LITERAL;
    private static final GroupStructInfo.LiteralMode NO_LITERAL = GroupStructInfo.LiteralMode.NO_LITERAL;

    private static final String GOLDEN_FILE_NAME = "hbo-struct-info-golden.txt";
    private static final String CLUSTER_FILE_NAME = "hbo-struct-info-golden-cluster.txt";
    private static final String DUMP_PROPERTY = "hbo.golden.dump";

    /** The license header of the corpus, written by the generator so the committed file carries it. */
    private static final List<String> LICENSE_HEADER = ImmutableList.of(
            "# Licensed to the Apache Software Foundation (ASF) under one",
            "# or more contributor license agreements.  See the NOTICE file",
            "# distributed with this work for additional information",
            "# regarding copyright ownership.  The ASF licenses this file",
            "# to you under the Apache License, Version 2.0 (the",
            "# \"License\"); you may not use this file except in compliance",
            "# with the License.  You may obtain a copy of the License at",
            "#",
            "#   http://www.apache.org/licenses/LICENSE-2.0",
            "#",
            "# Unless required by applicable law or agreed to in writing,",
            "# software distributed under the License is distributed on an",
            "# \"AS IS\" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY",
            "# KIND, either express or implied.  See the License for the",
            "# specific language governing permissions and limitations",
            "# under the License.",
            "#");
    private static final Path RESOURCE_DIR = Paths.get("src", "test", "resources");

    /**
     * The two skip reasons of a node whose key is looked up by the aggregation above it, and the
     * reason a partition top-n is refused. They are the texts the live corpus recorded, and they are
     * frozen here so that a reworded message cannot pass unnoticed.
     */
    private static final String STORAGE_LAYER_SKIP =
            "storage layer aggregate: the read side applies the row count of the aggregation above it";
    private static final String PARTIAL_STAGE_SKIP =
            "local (partial) aggregation stage: the read side applies the row count of the final"
                    + " aggregation above it";
    private static final String PARTITION_TOPN_SKIP = "unsupported=LogicalPartitionTopN";
    /** A logical CTE consumer whose sub tree hangs off the CTE id, not off a memo edge. */
    private static final String CTE_CONSUMER_WITHOUT_MEMO_EDGE_SKIP = "arity=0 of LogicalCTEConsumer";

    /** One plan case of the PLAN section: a name, a one line description and the described plan. */
    private static final class PlanCase {
        private final String name;
        private final String description;
        private final PlanBuilder builder;

        PlanCase(String name, String description, PlanBuilder builder) {
            this.name = name;
            this.description = description;
            this.builder = builder;
        }
    }

    /** Builds the struct info of one case (a fresh memo per call). */
    private interface PlanBuilder {
        GroupStructInfo build();
    }

    @Test
    void testTheGeneratedCorpusMatchesTheCommittedFile() throws IOException {
        String generated = goldenFileContent();
        if (Boolean.getBoolean(DUMP_PROPERTY)) {
            Path file = RESOURCE_DIR.resolve(GOLDEN_FILE_NAME);
            // the regenerating run cannot assert (it is the run which defines the expectation), so it
            // reports what moved instead: a blessed behaviour change has to be visible in the output of
            // the very command which blessed it, not only in a diff somebody may not read
            List<String> before = Files.exists(file) ? readResourceLines(GOLDEN_FILE_NAME) : ImmutableList.of();
            reportWhatMoved(before, ImmutableList.copyOf(generated.split("\n", -1)));
            Files.write(file, (generated + "\n").getBytes(StandardCharsets.UTF_8));
            System.out.println("[hbo-golden] wrote " + file.toAbsolutePath()
                    + " - this run asserted nothing; review the resource diff");
            return;
        }
        assertSameLines(readResourceLines(GOLDEN_FILE_NAME), generated);
    }

    @Test
    void testLiteralsWhichDecideRowsStayInTheConstantAgnosticKey() {
        // a filter: two conjuncts which fold to one text are two predicates, not one, so the conjunction
        // and one of its conjuncts must not share a key (a pinned count may not cross between them)
        GroupStructInfo two = built("filter_two_conjuncts_which_fold_alike");
        GroupStructInfo one = built("filter_one_of_those_conjuncts");
        Assertions.assertNotEquals(one.getFingerprint(NO_LITERAL), two.getFingerprint(NO_LITERAL),
                "a conjunction and one of its conjuncts share a constant agnostic key");
        Assertions.assertTrue(two.getCanonical(NO_LITERAL).contains(";"),
                "the second conjunct was folded away: " + two.getCanonical(NO_LITERAL));
        // a top-n and a window whose definition carries a literal decide which rows (and which values)
        // reach the node above, so the literal is part of the identity: the two variants differ in both
        // modes and the fold of the literal carrying text is the constant agnostic text
        String[][] pairs = {
                {"topn_with_a_literal_in_the_ordering_key", "topn_with_another_literal_in_the_ordering_key"},
                {"window_with_a_literal_in_the_ordering_key", "window_with_another_literal_in_the_ordering_key"},
                // the function of a window is the one identity site a real plan reaches today
                {"window_function_with_a_literal", "window_function_with_another_literal"},
                // two definitions whose aliases are swapped: the same definitions, different outputs
                {"two_window_outputs_x_is_row_number", "two_window_outputs_x_is_ntile"},
        };
        for (String[] pair : pairs) {
            GroupStructInfo left = built(pair[0]);
            GroupStructInfo right = built(pair[1]);
            Assertions.assertNotEquals(left.getFingerprint(NO_LITERAL), right.getFingerprint(NO_LITERAL),
                    pair[0] + " and " + pair[1] + " share a constant agnostic key");
            for (GroupStructInfo info : ImmutableList.of(left, right)) {
                Assertions.assertEquals(info.getCanonical(NO_LITERAL),
                        GroupStructInfo.toNoLiteral(info.getCanonical(WITH_LITERAL)),
                        "the fold of the literal carrying text is not the constant agnostic text");
            }
        }
    }

    @Test
    void testTwoUdfsWhichOnlyDifferByIdentityAreTwoPredicates() {
        // a filter whose only predicate is a udf call: the wrapper class is the same for every java or
        // python udf, so a key built from the class name and the arguments alone would let the four
        // cases below share one pinned row count. The corpus records the texts; this pins the rule: a
        // different function name, database or overload id is a different predicate
        GroupStructInfo active = built("filter_udf_is_active");
        Assertions.assertTrue(active.getCanonical(WITH_LITERAL).contains("JavaUdf<db1.is_active#1>"),
                "the udf identity is not in the canonical: " + active.getCanonical(WITH_LITERAL));
        // a function of the global function manager has no database: its scope is rendered explicitly
        // instead of failing the render (the name of the database is null for it)
        GroupStructInfo global = built("filter_global_udf_is_active");
        Assertions.assertTrue(global.getCanonical(WITH_LITERAL).contains("JavaUdf<global:is_active#3>"),
                "the scope of a global udf is not in the canonical: " + global.getCanonical(WITH_LITERAL));
        Assertions.assertNotEquals(global.getFingerprint(WITH_LITERAL), active.getFingerprint(WITH_LITERAL),
                "a global udf and a database scoped udf share a key");
        String[][] distinct = {
                {"filter_udf_is_active", "filter_udf_is_deleted"},
                {"filter_udf_is_active", "filter_udf_is_active_in_another_database"},
                {"filter_udf_is_active", "filter_udf_is_active_other_overload"},
        };
        for (String[] pair : distinct) {
            GroupStructInfo left = built(pair[0]);
            GroupStructInfo right = built(pair[1]);
            for (GroupStructInfo.LiteralMode mode
                    : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
                Assertions.assertNotEquals(left.getFingerprint(mode), right.getFingerprint(mode),
                        pair[0] + " and " + pair[1] + " share a key in mode " + mode);
            }
        }
    }

    @Test
    void testAComputedProjectOutputIsDefinedInTheKeyOfTheNodeAbove() {
        // `GROUP BY a % n` is normalized into an aggregation over a transparent project which names the
        // value: the definition of that alias has to be part of the key, because the two moduli keep a
        // different number of groups and a pinned count of one would be applied to the other
        GroupStructInfo byTwo = built("aggregate_grouping_by_a_computed_project");
        GroupStructInfo byThree = built("aggregate_grouping_by_another_computed_project");
        Assertions.assertNotEquals(byTwo.getFingerprint(WITH_LITERAL), byThree.getFingerprint(WITH_LITERAL),
                "two group by constants share a key because the project below them is transparent");
        Assertions.assertTrue(byTwo.getCanonical(WITH_LITERAL).contains("Mod("),
                "the definition of the alias is not in the key: " + byTwo.getCanonical(WITH_LITERAL));
        Assertions.assertFalse(byTwo.getCanonical(WITH_LITERAL).contains("col(x)"),
                "the aggregation still names the alias: " + byTwo.getCanonical(WITH_LITERAL));
        // the definition is the expression the alias wraps, not the alias: the plan the normalization
        // produces and the same grouping written inline must keep one key (they read the same rows)
        GroupStructInfo inline = built("aggregate_grouping_by_the_same_expression_directly");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertEquals(byTwo.getFingerprint(mode), inline.getFingerprint(mode),
                    "the normalized form and the inline form differ in mode " + mode);
        }
        // a plain rename is a definition as well: the filter above it reads the column it renames
        GroupStructInfo renamed = built("filter_above_a_renamed_project_output");
        Assertions.assertFalse(renamed.getCanonical(WITH_LITERAL).contains("col(x)"),
                "the rename is not resolved: " + renamed.getCanonical(WITH_LITERAL));
    }

    @Test
    void testAnAggregateOutputIsDefinedInTheKeyOfTheNodeAbove() {
        // the header of an aggregation keeps the grouping keys only, because the row count of the node
        // is the number of groups: a predicate above it which reads an aggregate function has to carry
        // the function, otherwise MIN and MAX under one alias share a key and one row count
        GroupStructInfo min = built("filter_above_an_aggregate_min");
        GroupStructInfo max = built("filter_above_an_aggregate_max");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertNotEquals(min.getFingerprint(mode), max.getFingerprint(mode),
                    "MIN and MAX share a key in mode " + mode);
        }
        Assertions.assertTrue(min.getCanonical(WITH_LITERAL).startsWith("F{EqualTo(Min("),
                "the aggregate function is not in the filter key: " + min.getCanonical(WITH_LITERAL));
        Assertions.assertFalse(min.getCanonical(WITH_LITERAL).contains("col(s)"),
                "the filter still names the aggregate's alias: " + min.getCanonical(WITH_LITERAL));
    }

    @Test
    void testAMatchPredicateKeepsItsAnalyzer() {
        // the analyzer is the tokenizer execution selects, so two MATCH predicates over the same
        // argument are two predicates although their children are equal
        GroupStructInfo a1 = built("filter_match_using_analyzer_a1");
        GroupStructInfo a2 = built("filter_match_using_analyzer_a2");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertNotEquals(a1.getFingerprint(mode), a2.getFingerprint(mode),
                    "two analyzers share a key in mode " + mode);
        }
        Assertions.assertTrue(a1.getCanonical(WITH_LITERAL).contains("MatchAll<a1>"),
                "the analyzer is not in the key: " + a1.getCanonical(WITH_LITERAL));
    }

    @Test
    void testASearchPredicateKeepsItsDsl() {
        // the dsl is what execution parses into the search query: two terms match a different number
        // of rows although the slot children of the two predicates are equal
        GroupStructInfo apple = built("filter_search_exact_apple");
        GroupStructInfo banana = built("filter_search_exact_banana");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertNotEquals(apple.getFingerprint(mode), banana.getFingerprint(mode),
                    "two search terms share a key in mode " + mode);
        }
        Assertions.assertTrue(apple.getCanonical(WITH_LITERAL).contains("EXACT"),
                "the dsl is not in the key: " + apple.getCanonical(WITH_LITERAL));
    }

    @Test
    void testASetOperationBranchKeepsItsColumnMapping() {
        // the branches are the same two scans and the selecting projects are transparent, so only the
        // mapping of the second branch says which column feeds the union - while the two queries produce
        // a different number of distinct rows
        GroupStructInfo selectingA = built("union_two_scans_selected_a");
        GroupStructInfo selectingB = built("union_two_scans_selected_b");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertNotEquals(selectingA.getFingerprint(mode), selectingB.getFingerprint(mode),
                    "two branch mappings share a key in mode " + mode);
        }
        Assertions.assertTrue(selectingA.getCanonical(WITH_LITERAL).contains("[col(db.u.id)]"),
                "the mapping of the second branch is not in the key: "
                        + selectingA.getCanonical(WITH_LITERAL));
        Assertions.assertTrue(selectingB.getCanonical(WITH_LITERAL).contains("[col(db.u.name)]"),
                "the mapping of the second branch is not in the key: "
                        + selectingB.getCanonical(WITH_LITERAL));
    }

    @Test
    void testAScanNamesTheIdOfItsTable() {
        // DROP TABLE t1 and CREATE of another t1 with the same row count leaves the name, the partitions
        // and the visible version unchanged, so only the id of the table tells the two scans apart
        GroupStructInfo original = built("filter_of_a_table");
        GroupStructInfo recreated = built("filter_of_a_recreated_table");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertNotEquals(original.getFingerprint(mode), recreated.getFingerprint(mode),
                    "two tables of one name share a key in mode " + mode);
        }
        Assertions.assertTrue(original.getCanonical(WITH_LITERAL).contains("#ti:0"),
                "the id of the table is not in the key: " + original.getCanonical(WITH_LITERAL));
        Assertions.assertTrue(recreated.getCanonical(WITH_LITERAL).contains("#ti:7"),
                "the id of the table is not in the key: " + recreated.getCanonical(WITH_LITERAL));
    }

    @Test
    void testAFractionalSecondIsPartOfTheKey() {
        // DateTimeV2Literal#getValue is only yyyyMMddHHmmss: printing it would fold .000001 and .000002
        // into one literal, although execution compares the microseconds
        GroupStructInfo one = built("filter_datetime_with_microsecond_one");
        GroupStructInfo two = built("filter_datetime_with_another_microsecond");
        Assertions.assertNotEquals(one.getFingerprint(WITH_LITERAL), two.getFingerprint(WITH_LITERAL),
                "two fractional seconds share a key");
        Assertions.assertTrue(one.getCanonical(WITH_LITERAL).contains("000001"),
                "the fraction of the second is not in the key: " + one.getCanonical(WITH_LITERAL));
        Assertions.assertTrue(two.getCanonical(WITH_LITERAL).contains("000002"),
                "the fraction of the second is not in the key: " + two.getCanonical(WITH_LITERAL));
    }

    @Test
    void testAComputedProjectBelowAJoinIsNotSkippedByTheChain() {
        // the join chain flattens the projects it passes; a computed one is a leaf, so the definition of
        // its alias reaches the condition of the join above it and the two moduli differ
        GroupStructInfo byTwo = built("join_over_a_computed_project");
        GroupStructInfo byThree = built("join_over_another_computed_project");
        // the modulus is a literal of the definition: the exact key differs, the constant agnostic form
        // folds it like every other literal of a predicate
        Assertions.assertNotEquals(byTwo.getFingerprint(WITH_LITERAL), byThree.getFingerprint(WITH_LITERAL),
                "two moduli share an exact key");
        Assertions.assertTrue(byTwo.getCanonical(WITH_LITERAL).contains("Mod("),
                "the project definition is not in the key: " + byTwo.getCanonical(WITH_LITERAL));
        Assertions.assertFalse(byTwo.getCanonical(WITH_LITERAL).contains("col(x)"),
                "the join still names the alias: " + byTwo.getCanonical(WITH_LITERAL));
    }

    @Test
    void testTheDistinctFlagOfAnAggregateFunctionIsIdentity() {
        // COUNT(DISTINCT v) and COUNT(v) have equal children, while a v of [1, 1] satisfies `s = 1` for
        // the second only: the flag has to be part of the expression identity
        GroupStructInfo count = built("filter_above_an_aggregate_count");
        GroupStructInfo distinct = built("filter_above_an_aggregate_count_distinct");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertNotEquals(count.getFingerprint(mode), distinct.getFingerprint(mode),
                    "a distinct and a plain count share a key in mode " + mode);
        }
        Assertions.assertTrue(distinct.getCanonical(WITH_LITERAL).contains("Count<distinct>"),
                "the distinct flag is not in the key: " + distinct.getCanonical(WITH_LITERAL));
    }

    @Test
    void testALambdaKeepsItsArgumentBindings() {
        // the body of the two lambdas renders the same text, while the argument names decide which array
        // each parameter reads: the predicate is true for (a=[1], b=[2]) with one binding and false with
        // the other
        GroupStructInfo xy = built("filter_lambda_arguments_x_y");
        GroupStructInfo swapped = built("filter_lambda_arguments_swapped");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertNotEquals(xy.getFingerprint(mode), swapped.getFingerprint(mode),
                    "two lambda bindings share a key in mode " + mode);
        }
        Assertions.assertTrue(xy.getCanonical(WITH_LITERAL).contains("Lambda<x,y>"),
                "the argument names are not in the key: " + xy.getCanonical(WITH_LITERAL));
        Assertions.assertTrue(swapped.getCanonical(WITH_LITERAL).contains("Lambda<y,x>"),
                "the argument names are not in the key: " + swapped.getCanonical(WITH_LITERAL));
    }

    @Test
    void testTheOutputsOfAGeneratorAreBoundToTheirFunctions() {
        // the same two functions in the same order: only the name of the column each one fills separates
        // the two plans, and a predicate above the generate reads exactly one of them
        GroupStructInfo xy = built("generate_outputs_x_y");
        GroupStructInfo swapped = built("generate_outputs_swapped");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertNotEquals(xy.getFingerprint(mode), swapped.getFingerprint(mode),
                    "two output bindings share a key in mode " + mode);
        }
        Assertions.assertTrue(xy.getCanonical(WITH_LITERAL).contains("out:[x,y]"),
                "the outputs are not in the key: " + xy.getCanonical(WITH_LITERAL));
        Assertions.assertTrue(swapped.getCanonical(WITH_LITERAL).contains("out:[y,x]"),
                "the outputs are not in the key: " + swapped.getCanonical(WITH_LITERAL));
    }

    @Test
    void testAnAliasIsBoundToItsRelation() {
        // the predicates name the aliases x and y while the leaves are sorted by their tables: only the
        // binding of an alias to its relation separates the two plans, and the ordered comparison keeps a
        // different number of rows
        GroupStructInfo xy = built("join_with_aliases_x_y");
        GroupStructInfo swapped = built("join_with_aliases_swapped");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertNotEquals(xy.getFingerprint(mode), swapped.getFingerprint(mode),
                    "two alias bindings share a key in mode " + mode);
        }
        Assertions.assertTrue(xy.getCanonical(WITH_LITERAL).contains("#ra:x"),
                "the alias is not in the key: " + xy.getCanonical(WITH_LITERAL));
        Assertions.assertTrue(xy.getCanonical(WITH_LITERAL).contains("#ra:y"),
                "the alias is not in the key: " + xy.getCanonical(WITH_LITERAL));
    }

    @Test
    void testEveryCanonicalFoldsToItsConstantAgnosticForm() {
        // the constant agnostic fingerprint of a pasted struct info is computed by folding its text (see
        // GroupStructInfo#toNoLiteral), so the fold has to reproduce exactly the text the renderer writes
        // for NO_LITERAL - for every node kind, including the collections which are sorted per mode: the
        // conjuncts of a join are sorted and de-duplicated, the grouping sets of a repeat are sorted set by
        // set, while a literal substitution alone would keep the order of the literal carrying form
        for (PlanCase planCase : planCases()) {
            GroupStructInfo info = planCase.builder.build();
            if (!info.isValid()) {
                continue;
            }
            Assertions.assertEquals(info.getCanonical(NO_LITERAL),
                    GroupStructInfo.toNoLiteral(info.getCanonical(WITH_LITERAL)),
                    "the folded canonical of " + planCase.name + " is not its constant agnostic form");
        }
    }

    @Test
    void testTheJoinChainIsAssociatedTheSameWayBothWays() {
        // (t1 join t2) join t3 and t1 join (t2 cross join t3) carry the same two conditions over the
        // same three scans: a chain is associative, so the two plans must render one descriptor - the
        // corpus records both texts, this asserts that they really are the same text
        GroupStructInfo leftDeep = built("join_inner_chain_three_left_deep");
        GroupStructInfo rightDeep = built("join_inner_chain_three_right_deep");
        for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
            Assertions.assertEquals(leftDeep.getCanonical(mode), rightDeep.getCanonical(mode),
                    "the canonical form of an inner join chain depends on how it is written");
            Assertions.assertEquals(leftDeep.getShape(mode), rightDeep.getShape(mode));
            Assertions.assertEquals(leftDeep.getFingerprint(mode), rightDeep.getFingerprint(mode));
        }
        Assertions.assertEquals(3, leftDeep.getScans().size());
    }

    @Test
    void testTheShapesTheCorpusPinsAreStillTheShapesItClaims() {
        // a scan token which names the whole table and nothing else: the mock tables are not in a
        // catalog, so their qualifier is null.null
        GroupStructInfo scanOnly = built("scan_only");
        Assertions.assertTrue(scanOnly.isValid(), scanOnly.getSkipReason());
        Assertions.assertEquals(1, scanOnly.getScans().size());

        // filter: the two literal modes differ by the literal value only
        String filterWithLiteral = built("filter_literal").getCanonical(WITH_LITERAL);
        String filterNoLiteral = built("filter_literal").getCanonical(NO_LITERAL);
        Assertions.assertNotEquals(filterWithLiteral, filterNoLiteral);
        Assertions.assertEquals(filterNoLiteral, GroupStructInfo.toNoLiteral(filterWithLiteral));

        // filter with slot = slot: no literal, so the two modes are the same text
        Assertions.assertEquals(built("filter_slot_equal_slot").getCanonical(WITH_LITERAL),
                built("filter_slot_equal_slot").getCanonical(NO_LITERAL));

        // project / sort are transparent: the project case is the scan alone
        Assertions.assertEquals(scanOnly.getCanonical(NO_LITERAL),
                built("project_above_scan").getCanonical(NO_LITERAL));

        // left outer join: the join type and the memo order are kept
        Assertions.assertTrue(built("join_left_outer").getCanonical(NO_LITERAL)
                .startsWith("J{LEFT_OUTER_JOIN,c:["));

        // limit and top-n keep their bound in both modes, the top-n keeps its ordering as well
        Assertions.assertTrue(built("limit").getCanonical(WITH_LITERAL).contains("L{5,0}"));
        Assertions.assertEquals(built("limit").getFingerprint(WITH_LITERAL),
                built("limit").getFingerprint(NO_LITERAL));
        Assertions.assertTrue(built("topn").getCanonical(NO_LITERAL).contains("T{3,0;ord:["));
        Assertions.assertTrue(built("topn").getCanonical(NO_LITERAL).contains(":asc:nulls_first]"));

        // an aggregation is keyed by its grouping keys: with keys, and without any
        Assertions.assertTrue(built("aggregate_group_by_keys").getCanonical(NO_LITERAL).contains("A{gb:col("));
        Assertions.assertTrue(built("aggregate_no_keys").getCanonical(NO_LITERAL).startsWith("A{gb:}("));

        // a window keeps its definition: the partition keys and the frame are part of the canonical
        String windowCanonical = built("window_under_a_filter").getCanonical(NO_LITERAL);
        Assertions.assertTrue(windowCanonical.contains("W{rn=RowNumber;part:[col("), windowCanonical);
        Assertions.assertTrue(windowCanonical.contains("ord:[]"), windowCanonical);
        Assertions.assertTrue(windowCanonical.contains("frame:["), windowCanonical);

        // a generate keeps its table function arguments in both modes: the delimiter is identity
        String generate = built("generate_explode_split").getCanonical(NO_LITERAL);
        Assertions.assertTrue(generate.contains("G{tf:[ExplodeSplit("), generate);
        Assertions.assertTrue(generate.contains("lit(%2C:VARCHAR(1))"), generate);

        // a repeat keeps its grouping sets, in order
        Assertions.assertTrue(built("repeat_grouping_sets").getCanonical(NO_LITERAL).contains("R{gs:["));

        // a union carries its constant branches in the header, an intersect and an except their kind
        Assertions.assertTrue(built("union_constant_branch").getCanonical(NO_LITERAL).contains("const:["));
        Assertions.assertTrue(built("intersect_two_scans").getCanonical(NO_LITERAL).contains("I{all,cols:["));
        Assertions.assertTrue(built("except_two_scans").getCanonical(NO_LITERAL).contains("E{all,cols:["));

        // a self join of one table writes the same scan token twice, but the table is read once
        String selfJoin = built("self_join_one_table").getCanonical(NO_LITERAL);
        int firstToken = selfJoin.indexOf("S{");
        Assertions.assertTrue(firstToken >= 0, selfJoin);
        String token = selfJoin.substring(firstToken, selfJoin.indexOf('}', firstToken) + 1);
        Assertions.assertTrue(selfJoin.indexOf(token, firstToken + token.length()) > firstToken,
                "the scan token of a self join is written twice: " + selfJoin);
        Assertions.assertEquals(1, built("self_join_one_table").getScans().size(),
                "a self join reads its table once");
    }

    @Test
    void testTheTooWideSubTreeIsRefusedWithTheFrozenReason() {
        GroupStructInfo tooWide = built("invalid_too_many_scans");
        Assertions.assertFalse(tooWide.isValid());
        Assertions.assertEquals("subtree scans=" + (GroupStructInfo.DEFAULT_MAX_SCANS_PER_GROUP + 1)
                + " > hbo_max_scans_per_group=" + GroupStructInfo.DEFAULT_MAX_SCANS_PER_GROUP,
                tooWide.getSkipReason());
        // an invalid description renders the empty string, and its fingerprint is the sha256 of it
        Assertions.assertEquals("", tooWide.getCanonical(WITH_LITERAL));
        Assertions.assertEquals(tooWide.getFingerprint(NO_LITERAL),
                GroupStructInfo.INVALID.getFingerprint(NO_LITERAL));
        Assertions.assertTrue(tooWide.getScans().isEmpty());
    }

    @Test
    void testTheSkipReasonsOfTheRecordedCorpusAreTheProductionTexts() {
        // the storage layer aggregate is refused before its group is even resolved
        PhysicalStorageLayerAggregate storageLayerAggregate = Mockito.mock(PhysicalStorageLayerAggregate.class);
        Mockito.when(storageLayerAggregate.getGroupExpression()).thenReturn(Optional.empty());
        Assertions.assertEquals(Optional.of(STORAGE_LAYER_SKIP),
                GroupStructInfo.skipReasonOfPlanNode(storageLayerAggregate, null));
        // a group without a logical expression which holds a hash aggregation is the local (partial)
        // stage SplitAggWithoutDistinct inserted: it shares the key of the final aggregation above it
        Map<Integer, Group> groupsById = ImmutableMap.of(7, physicalOnlyGroup());
        Assertions.assertEquals(Optional.of(PARTIAL_STAGE_SKIP),
                GroupStructInfo.skipReasonOfPlanNode(partialStage(), groupsById));
    }

    private static Group physicalOnlyGroup() {
        Group group = Mockito.mock(Group.class);
        Mockito.when(group.getLogicalExpressions()).thenReturn(ImmutableList.of());
        return group;
    }

    private static PhysicalHashAggregate<?> partialStage() {
        PhysicalHashAggregate<?> partialStage = Mockito.mock(PhysicalHashAggregate.class);
        Mockito.when(partialStage.getGroupExpression()).thenReturn(Optional.empty());
        Mockito.when(partialStage.getMutableState(MutableState.KEY_GROUP)).thenReturn(Optional.of("7"));
        return partialStage;
    }

    @Test
    void testTheCapturedClusterCorpusKeepsItsInvariants() throws IOException {
        List<String> lines = readResourceLines(CLUSTER_FILE_NAME);
        String raw = String.join("\n", lines);
        // the two reasons below are constructed by the traversal rather than declared as a constant, so
        // the frozen text is tied to the formula the code uses: a renamed plan class or a changed prefix
        // cannot pass unnoticed
        Assertions.assertEquals(GroupStructInfo.UNSUPPORTED_REASON_PREFIX
                + LogicalPartitionTopN.class.getSimpleName(), PARTITION_TOPN_SKIP);
        Assertions.assertEquals(GroupStructInfo.ARITY_REASON_PREFIX + 0 + GroupStructInfo.ARITY_REASON_MIDDLE
                + LogicalCTEConsumer.class.getSimpleName(), CTE_CONSUMER_WITHOUT_MEMO_EDGE_SKIP);
        Set<String> knownSkips = ImmutableSet.of(STORAGE_LAYER_SKIP, PARTIAL_STAGE_SKIP, PARTITION_TOPN_SKIP,
                CTE_CONSUMER_WITHOUT_MEMO_EDGE_SKIP);
        List<String> skips = new ArrayList<>();
        String caseName = "<no case>";
        ClusterNode node = null;
        int cases = 0;
        int recorded = 0;
        Map<String, ClusterNode> nodes = new LinkedHashMap<>();
        for (String line : lines) {
            if (line.startsWith("# node ")) {
                // the two literal modes of one node are printed under the same `# node` header, so a node
                // is looked up by that header (within its case - two cases can describe the same plan
                // shape): keying a fresh node per header is what used to leave every node with one mode
                // and made the cross mode invariant below unreachable
                String nodeKey = caseName + "|" + line;
                node = nodes.get(nodeKey);
                if (node == null) {
                    node = new ClusterNode(caseName, line);
                    nodes.put(nodeKey, node);
                }
                continue;
            }
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("[CLUSTER ")) {
                node = null;
                caseName = line.substring("[CLUSTER ".length(), line.length() - 1);
                cases++;
                continue;
            }
            if (line.startsWith("sql = ") || line.startsWith("set = ") || line.startsWith("note = ")) {
                continue;
            }
            if (line.startsWith("skip = ")) {
                String skip = line.substring("skip = ".length());
                Assertions.assertTrue(knownSkips.contains(skip),
                        caseName + " records an unknown skip reason: " + skip);
                Assertions.assertTrue(node != null && node.header.contains(" skipped"),
                        caseName + " records a skip reason on a node which is not marked as skipped: " + skip);
                skips.add(skip);
                continue;
            }
            Assertions.assertNotNull(node, caseName + " records a struct before any node header: " + line);
            node.add(line, caseName);
        }
        int foldChecks = 0;
        for (ClusterNode clusterNode : nodes.values()) {
            recorded += clusterNode.check(clusterNode.caseName, raw);
            foldChecks += clusterNode.getFoldChecks();
        }
        Assertions.assertTrue(cases >= 15, "the cluster corpus holds only " + cases + " cases");
        Assertions.assertTrue(recorded >= 15, "the cluster corpus records only " + recorded + " entries");
        // the pairs which print both modes: without a floor a future corpus could lose them all and the
        // cross mode invariant would silently stop running again
        Assertions.assertTrue(foldChecks >= 10,
                "only " + foldChecks + " nodes of the cluster corpus were checked across both literal modes");
        for (String expected : knownSkips) {
            Assertions.assertTrue(skips.contains(expected),
                    "the cluster corpus does not record the skip reason: " + expected);
        }
        assertClusterCaseCount(lines, cases);
    }

    private static void assertClusterCaseCount(List<String> lines, int cases) {
        for (String line : lines) {
            if (line.startsWith("# case count: ")) {
                Assertions.assertEquals(Integer.parseInt(line.substring("# case count: ".length())), cases,
                        "the header of the cluster corpus does not name the number of cases it holds");
                return;
            }
        }
        Assertions.fail("the cluster corpus has no '# case count:' header line");
    }

    /** One node of a captured case: the struct of each literal mode and the fingerprints printed. */
    private static final class ClusterNode {
        private final String caseName;
        private final String header;
        private final Map<String, String> structs = new LinkedHashMap<>();
        private final Map<String, String> fingerprints = new LinkedHashMap<>();
        /** how often the cross mode invariant ran for this node (see the floor in the test) */
        private int foldChecks;

        ClusterNode(String caseName, String header) {
            this.caseName = caseName;
            this.header = header;
        }

        int getFoldChecks() {
            return foldChecks;
        }

        void add(String line, String caseName) {
            if (line.startsWith("wl = ")) {
                addStruct("with_literal", line.substring("wl = ".length()), caseName);
            } else if (line.startsWith("nl = ")) {
                addStruct("no_literal", line.substring("nl = ".length()), caseName);
            } else if (line.startsWith("fp = ")) {
                String value = line.substring("fp = ".length());
                int colon = value.indexOf(':');
                Assertions.assertTrue(colon > 0, caseName + " has a malformed fingerprint line: " + line);
                fingerprints.put(value.substring(0, colon), value.substring(colon + 1));
            } else {
                Assertions.fail("unknown line in the cluster corpus of " + caseName + ": " + line);
            }
        }

        private void addStruct(String mode, String struct, String caseName) {
            Assertions.assertNull(structs.put(mode, struct),
                    caseName + " records the " + mode + " struct of one node twice");
        }

        /**
         * Assert the invariants this node has to satisfy and return how many fingerprint lines were
         * cross-checked. A {@code join_expansion} node carries the canonical of a join condition, not
         * a struct info, so only its hash is checked (the join conditions are hashed directly).
         */
        int check(String caseName, String raw) {
            boolean joinExpansion = header.contains("type=join_expansion");
            String withLiteral = structs.get("with_literal");
            String noLiteral = structs.get("no_literal");
            if (!joinExpansion) {
                if (withLiteral != null && noLiteral != null) {
                    foldChecks++;
                    Assertions.assertEquals(noLiteral, GroupStructInfo.toNoLiteral(withLiteral),
                            caseName + ": toNoLiteral did not fold the literal carrying struct into the"
                                    + " constant agnostic one recorded next to it");
                    // the literals of the two texts differ by design, so the invariant is about the scan
                    // tokens: both modes describe the same scans, in the same order, with the same identity
                    List<HboScanDescriptor> withLiteralScans = HboScanDescriptor.parseAll(withLiteral);
                    List<HboScanDescriptor> noLiteralScans = HboScanDescriptor.parseAll(noLiteral);
                    Assertions.assertEquals(withLiteralScans.size(), noLiteralScans.size(),
                            caseName + ": the number of scan tokens depends on the literal mode");
                    for (int i = 0; i < withLiteralScans.size(); i++) {
                        Assertions.assertEquals(withLiteralScans.get(i).getShapeIdentity(),
                                noLiteralScans.get(i).getShapeIdentity(),
                                caseName + ": the identity of scan " + i + " depends on the literal mode");
                    }
                }
                if (noLiteral != null) {
                    Assertions.assertEquals(noLiteral, GroupStructInfo.toNoLiteral(noLiteral),
                            caseName + ": the constant agnostic struct is not a fixed point of the fold");
                }
            }
            for (String struct : structs.values()) {
                // the boundary rule every reader of a canonical string uses finds exactly the tokens the
                // parser reads, so a text which merely looks like a token is never counted
                int tokenStarts = 0;
                for (int at = HboScanDescriptor.nextScanTokenStart(struct, 0); at >= 0;
                        at = HboScanDescriptor.nextScanTokenStart(struct, at + 2)) {
                    tokenStarts++;
                }
                Assertions.assertEquals(tokenStarts, HboScanDescriptor.parseAll(struct).size(),
                        caseName + ": the boundary rule and the parser disagree about the scan tokens of "
                                + struct);
                for (HboScanDescriptor descriptor : HboScanDescriptor.parseAll(struct)) {
                    String rendered = descriptor.render();
                    Assertions.assertEquals(rendered, HboScanDescriptor.parse(rendered).render(),
                            caseName + ": a scan token of the corpus does not survive a parse / render round"
                                    + " trip: " + rendered);
                    Assertions.assertFalse(descriptor.getShapeIdentity().isEmpty(), caseName);
                }
            }
            int checked = 0;
            for (Map.Entry<String, String> entry : fingerprints.entrySet()) {
                String struct = structs.get(entry.getKey());
                Assertions.assertNotNull(struct, caseName + " records a " + entry.getKey()
                        + " fingerprint without the struct it belongs to: " + raw);
                Assertions.assertEquals(sha256(GroupStructInfo.stripScanBaseline(struct)), entry.getValue(),
                        caseName + ": the sha256 of the stripped struct is not the fingerprint the FE printed"
                                + " next to it");
                checked++;
            }
            return checked;
        }
    }

    // ------------------------------------------------------------------------------------------
    // the golden file
    // ------------------------------------------------------------------------------------------

    private static String goldenFileContent() {
        StringBuilder sb = new StringBuilder();
        for (String headerLine : LICENSE_HEADER) {
            sb.append(headerLine).append('\n');
        }
        sb.append("# HBO struct info golden corpus - PLAN and TEXT sections (GENERATED).\n");
        sb.append("#\n");
        sb.append("# GENERATED FILE - DO NOT EDIT BY HAND. Every line below is produced by\n");
        sb.append("# GroupStructInfoGoldenTest from the current implementation; regenerate it with:\n");
        sb.append("#\n");
        sb.append("#   cd fe\n");
        sb.append("#   mvn -o -pl fe-core -am test \\\n");
        sb.append("#     -Dtest='GroupStructInfoGoldenTest' -DfailIfNoTests=false -Dskip.doc=true \\\n");
        sb.append("#     -Dhbo.golden.dump=true\n");
        sb.append("#   # then run the same command without -Dhbo.golden.dump to prove the corpus asserts clean\n");
        sb.append("#\n");
        sb.append("# What it freezes: the canonical string, the shape (the fingerprint input), the sha256\n");
        sb.append("# fingerprint and the scan tokens of the hbo struct info, the simplified display string\n");
        sb.append("# and the text based mappings (toNoLiteral / stripScanBaseline / the literal codec).\n");
        sb.append("# sha256(shape) is the hbo cache key: it lives in __internal_schema.hbo_plan_statistics,\n");
        sb.append("# is printed by EXPLAIN and pasted back into HBO SET STATISTICS ... STRUCT='...'. Every\n");
        sb.append("# line here is a contract: a diff means the key of the affected shapes moved.\n");
        sb.append("#\n");
        sb.append("# Sections:\n");
        sb.append("#   [PLAN <name>]  a plan built in process (memo level), with both literal modes\n");
        sb.append("#   [TEXT <kind>/<name>]  a pure function case: `in =` is the argument, `out =` / `err =`\n");
        sb.append("#                         the result (an exception is recorded as its class and message)\n");
        sb.append("#\n");
        sb.append("# plan cases: ").append(planCases().size())
                .append(", text cases: ").append(textCases().size())
                .append(" (the PLAN cases are sorted by name, the TEXT cases by kind and then by name)").append('\n');
        sb.append('\n');
        sb.append(planSection());
        sb.append('\n');
        sb.append(textSection());
        // no line of the corpus ends in a blank: an empty value would otherwise leave one behind
        return sb.toString().replaceAll("(?m)[ \\t]+$", "").stripTrailing();
    }

    private static String planSection() {
        Map<String, GroupStructInfo> built = new LinkedHashMap<>();
        Map<String, String> descriptions = new LinkedHashMap<>();
        for (PlanCase planCase : planCases()) {
            built.put(planCase.name, planCase.builder.build());
            descriptions.put(planCase.name, planCase.description);
        }
        List<String> names = new ArrayList<>(built.keySet());
        names.sort(String::compareTo);
        StringBuilder sb = new StringBuilder();
        for (String name : names) {
            GroupStructInfo info = built.get(name);
            sb.append("[PLAN ").append(name).append("]\n");
            sb.append("desc = ").append(descriptions.get(name)).append('\n');
            sb.append("valid = ").append(info.isValid()).append('\n');
            sb.append("skipReason =").append(info.getSkipReason().isEmpty() ? "" : " " + info.getSkipReason())
                    .append('\n');
            for (GroupStructInfo.LiteralMode mode : new GroupStructInfo.LiteralMode[] {WITH_LITERAL, NO_LITERAL}) {
                sb.append(mode).append(".canonical = ").append(info.getCanonical(mode)).append('\n');
                sb.append(mode).append(".shape = ").append(info.getShape(mode)).append('\n');
                sb.append(mode).append(".fingerprint = ").append(info.getFingerprint(mode)).append('\n');
            }
            sb.append("scans = ").append(info.getScans().size()).append('\n');
            for (int i = 0; i < info.getScans().size(); i++) {
                sb.append("scan[").append(i).append("] = ").append(renderScan(info.getScans().get(i))).append('\n');
            }
        }
        return sb.toString();
    }

    private static String renderScan(HboScanDescriptor scan) {
        return "table=" + scan.getTable()
                + " shapeIdentity=" + scan.getShapeIdentity()
                + " render=" + scan.render()
                + " visibleVersion=" + scan.getVisibleVersion()
                + " scanRows=" + scan.getScanRows()
                + " rowsKind=" + scan.getRowsKind()
                + " selectedPartitions=" + scan.getSelectedPartitions()
                + " totalPartitions=" + scan.getTotalPartitions()
                + " partitionIdentity=" + scan.getPartitionIdentity()
                + " tabletIdentity=" + scan.getTabletIdentity()
                + " indexIdentity=" + scan.getIndexIdentity();
    }

    private static String textSection() {
        List<TextCase> cases = textCases();
        // a deterministic order: by kind, then by case name
        cases.sort((left, right) -> (left.kind + "/" + left.name).compareTo(right.kind + "/" + right.name));
        StringBuilder sb = new StringBuilder();
        for (TextCase textCase : cases) {
            sb.append(renderTextCase(textCase));
        }
        return sb.toString();
    }

    private static String renderTextCase(TextCase textCase) {
        StringBuilder sb = new StringBuilder();
        sb.append("[TEXT ").append(textCase.kind).append('/').append(textCase.name).append("]\n");
        sb.append("in = ").append(textCase.input).append('\n');
        StringBuilder body = new StringBuilder();
        try {
            switch (textCase.kind) {
                case "toNoLiteral":
                    body.append("out = ").append(GroupStructInfo.toNoLiteral(textCase.input)).append('\n');
                    break;
                case "stripScanBaseline":
                    body.append("out = ").append(GroupStructInfo.stripScanBaseline(textCase.input)).append('\n');
                    break;
                case "encodeLiteralValue": {
                    String encoded = GroupStructInfo.encodeLiteralValue(textCase.input);
                    body.append("out = ").append(encoded).append('\n');
                    body.append("decoded = ").append(GroupStructInfo.decodeLiteralValue(encoded)).append('\n');
                    break;
                }
                case "decodeLiteralValue":
                    body.append("out = ").append(GroupStructInfo.decodeLiteralValue(textCase.input)).append('\n');
                    break;
                case "SimpleStructInfo.render":
                    body.append("out = ").append(SimpleStructInfo.render(textCase.input)).append('\n');
                    break;
                case "HboScanDescriptor": {
                    HboScanDescriptor descriptor = HboScanDescriptor.parse(textCase.input);
                    body.append("render = ").append(descriptor.render()).append('\n');
                    body.append("shapeIdentity = ").append(descriptor.getShapeIdentity()).append('\n');
                    body.append("table = ").append(descriptor.getTable()).append('\n');
                    body.append("visibleVersion = ").append(descriptor.getVisibleVersion()).append('\n');
                    body.append("scanRows = ").append(descriptor.getScanRows()).append('\n');
                    body.append("rowsKind = ").append(descriptor.getRowsKind()).append('\n');
                    body.append("selectedPartitions = ").append(descriptor.getSelectedPartitions()).append('\n');
                    body.append("totalPartitions = ").append(descriptor.getTotalPartitions()).append('\n');
                    body.append("partitionIdentity = ").append(descriptor.getPartitionIdentity()).append('\n');
                    body.append("tabletIdentity = ").append(descriptor.getTabletIdentity()).append('\n');
                    body.append("indexIdentity = ").append(descriptor.getIndexIdentity()).append('\n');
                    body.append("hasVisibleVersion = ").append(descriptor.hasVisibleVersion()).append('\n');
                    body.append("hasScanRows = ").append(descriptor.hasScanRows()).append('\n');
                    body.append("partitionSelectionComplete = ").append(descriptor.isPartitionSelectionComplete())
                            .append('\n');
                    body.append("reparsedRender = ")
                            .append(HboScanDescriptor.parse(descriptor.render()).render()).append('\n');
                    break;
                }
                default:
                    throw new AssertionError("unknown text case kind " + textCase.kind);
            }
        } catch (RuntimeException e) {
            return sb.append("err = ").append(e.getClass().getName()).append(": ").append(e.getMessage())
                    .append('\n').toString();
        }
        return sb.append(body).toString();
    }

    /** One pure function case of the TEXT section. */
    private static final class TextCase {
        private final String kind;
        private final String name;
        private final String input;

        TextCase(String kind, String name, String input) {
            this.kind = kind;
            this.name = name;
            this.input = input;
        }
    }

    // ------------------------------------------------------------------------------------------
    // the plan cases
    // ------------------------------------------------------------------------------------------

    private static List<PlanCase> planCases() {
        List<PlanCase> cases = new ArrayList<>();
        cases.add(new PlanCase("scan_only", "one olap scan", GroupStructInfoGoldenTest::scanOnly));
        cases.add(new PlanCase("filter_literal", "filter with one literal comparison over a scan",
                GroupStructInfoGoldenTest::filterLiteral));
        cases.add(new PlanCase("filter_of_a_table",
                "a filter over the table t1, which names the id of that table",
                GroupStructInfoGoldenTest::filterOfATable));
        cases.add(new PlanCase("filter_of_a_recreated_table",
                "the same filter over another table of the same name (dropped and recreated): a different key",
                GroupStructInfoGoldenTest::filterOfARecreatedTable));
        cases.add(new PlanCase("filter_datetime_with_microsecond_one",
                "a filter comparing a datetime literal with one microsecond",
                GroupStructInfoGoldenTest::filterDatetimeWithMicrosecondOne));
        cases.add(new PlanCase("filter_datetime_with_another_microsecond",
                "the same filter with two microseconds: a different key although the second is the same",
                GroupStructInfoGoldenTest::filterDatetimeWithAnotherMicrosecond));
        cases.add(new PlanCase("filter_lambda_arguments_x_y",
                "a filter whose lambda binds x to the first array and y to the second (x > y)",
                GroupStructInfoGoldenTest::filterLambdaArgumentsXY));
        cases.add(new PlanCase("filter_lambda_arguments_swapped",
                "the same body with the parameters swapped (y, x): the opposite comparison",
                GroupStructInfoGoldenTest::filterLambdaArgumentsSwapped));
        cases.add(new PlanCase("filter_slot_equal_slot",
                "filter comparing two slots of one scan (both literal modes coincide)",
                GroupStructInfoGoldenTest::filterSlotEqualSlot));
        cases.add(new PlanCase("project_above_scan", "project over a scan (transparent)",
                GroupStructInfoGoldenTest::projectAboveScan));
        cases.add(new PlanCase("sort_above_a_filter",
                "sort over a filter (transparent): the sort adds no rows and no identity of its own",
                GroupStructInfoGoldenTest::sortAboveAFilter));
        cases.add(new PlanCase("join_inner_two_scans", "inner join of two scans with one equality condition",
                GroupStructInfoGoldenTest::joinInnerTwoScans));
        cases.add(new PlanCase("join_inner_chain_three_left_deep",
                "inner join chain of three scans written (t1 join t2) join t3",
                GroupStructInfoGoldenTest::joinInnerChainThreeLeftDeep));
        cases.add(new PlanCase("join_inner_chain_three_right_deep",
                "the same three scans written t1 join (t2 cross join t3), both conditions on the top join",
                GroupStructInfoGoldenTest::joinInnerChainThreeRightDeep));
        cases.add(new PlanCase("cross_join_two_scans",
                "cross join of two scans: a chain without conditions renders as inner",
                GroupStructInfoGoldenTest::crossJoinTwoScans));
        cases.add(new PlanCase("join_over_a_computed_project",
                "an inner join whose condition reads an alias a computed project below it produces (a % 2)",
                GroupStructInfoGoldenTest::joinOverAComputedProject));
        cases.add(new PlanCase("join_over_another_computed_project",
                "the same join over the same project with another modulo (a % 3): a different key",
                GroupStructInfoGoldenTest::joinOverAnotherComputedProject));
        cases.add(new PlanCase("join_left_outer",
                "left outer join of two scans (the join type and the memo order are kept)",
                GroupStructInfoGoldenTest::joinLeftOuter));
        cases.add(new PlanCase("join_with_aliases_x_y",
                "inner join whose predicates name the aliases of its two scans (t AS x, u AS y)",
                GroupStructInfoGoldenTest::joinWithAliasesXY));
        cases.add(new PlanCase("join_with_aliases_swapped",
                "the same predicates over the same tables with the aliases swapped: a different key",
                GroupStructInfoGoldenTest::joinWithAliasesSwapped));
        cases.add(new PlanCase("limit", "limit 5 offset 0 over a scan",
                GroupStructInfoGoldenTest::limit));
        cases.add(new PlanCase("topn", "top-n of 3 rows ordered by one key, asc nulls first",
                GroupStructInfoGoldenTest::topn));
        cases.add(new PlanCase("aggregate_group_by_keys", "aggregation with one group by key over a scan",
                GroupStructInfoGoldenTest::aggregateGroupByKeys));
        cases.add(new PlanCase("aggregate_grouping_by_a_computed_project",
                "aggregation over a transparent project which groups by a computed alias (a % 2 AS x)",
                GroupStructInfoGoldenTest::aggregateGroupingByAComputedProject));
        cases.add(new PlanCase("aggregate_grouping_by_another_computed_project",
                "the same aggregation over the same project with another modulo (a % 3 AS x): a different key",
                GroupStructInfoGoldenTest::aggregateGroupingByAnotherComputedProject));
        cases.add(new PlanCase("aggregate_grouping_by_the_same_expression_directly",
                "the same aggregation written without the project (GROUP BY a % 2): the same key",
                GroupStructInfoGoldenTest::aggregateGroupingByTheSameExpressionDirectly));
        cases.add(new PlanCase("filter_above_a_renamed_project_output",
                "a filter on the alias of a plain rename (a AS x): the definition of x is the column a",
                GroupStructInfoGoldenTest::filterAboveARenamedProjectOutput));
        cases.add(new PlanCase("filter_above_an_aggregate_min",
                "a filter on an aggregate function's output (MIN(v) AS s): s is the minimum of v",
                GroupStructInfoGoldenTest::filterAboveAnAggregateMin));
        cases.add(new PlanCase("filter_above_an_aggregate_count",
                "a filter on a count's output (COUNT(v) AS s)",
                GroupStructInfoGoldenTest::filterAboveAnAggregateCount));
        cases.add(new PlanCase("filter_above_an_aggregate_count_distinct",
                "the same filter on COUNT(DISTINCT v) AS s: the distinct flag is identity",
                GroupStructInfoGoldenTest::filterAboveAnAggregateCountDistinct));
        cases.add(new PlanCase("filter_above_an_aggregate_max",
                "the same filter with MAX(v) AS s: a different key although the aggregate header is the same",
                GroupStructInfoGoldenTest::filterAboveAnAggregateMax));
        cases.add(new PlanCase("filter_match_using_analyzer_a1",
                "a filter with MATCH ... USING ANALYZER a1 over a scan",
                GroupStructInfoGoldenTest::filterMatchUsingAnalyzerA1));
        cases.add(new PlanCase("filter_match_using_analyzer_a2",
                "the same match predicate with a2: the analyzer tokenizes the argument differently",
                GroupStructInfoGoldenTest::filterMatchUsingAnalyzerA2));
        cases.add(new PlanCase("filter_search_exact_apple",
                "a filter with search('name:EXACT(apple)') over a scan: the dsl is the predicate",
                GroupStructInfoGoldenTest::filterSearchExactApple));
        cases.add(new PlanCase("filter_search_exact_banana",
                "the same search predicate for another term: a different key although the children are equal",
                GroupStructInfoGoldenTest::filterSearchExactBanana));
        cases.add(new PlanCase("aggregate_no_keys", "aggregation without group by keys over a scan",
                GroupStructInfoGoldenTest::aggregateNoKeys));
        cases.add(new PlanCase("self_join_one_table",
                "self join of one scan: the token is written twice, the table is read once",
                GroupStructInfoGoldenTest::selfJoinOneTable));
        cases.add(new PlanCase("union_constant_branch",
                "union of two scans and one constant branch (the constants are in the header)",
                GroupStructInfoGoldenTest::unionConstantBranch));
        cases.add(new PlanCase("intersect_two_scans", "intersect of two scans",
                GroupStructInfoGoldenTest::intersectTwoScans));
        cases.add(new PlanCase("except_two_scans", "except of two scans (the first branch is the minuend)",
                GroupStructInfoGoldenTest::exceptTwoScans));
        cases.add(new PlanCase("union_two_scans_selected_a",
                "union of two scans which selects a from both: the second branch feeds the column a",
                GroupStructInfoGoldenTest::unionTwoScansSelectedA));
        cases.add(new PlanCase("union_two_scans_selected_b",
                "the same union selecting u.b in the second branch: a different mapping, a different key",
                GroupStructInfoGoldenTest::unionTwoScansSelectedB));
        cases.add(new PlanCase("window_under_a_filter",
                "filter over a window partitioned by one column (no order by)",
                GroupStructInfoGoldenTest::windowUnderAFilter));
        cases.add(new PlanCase("generate_unnest", "generate of unnest(1) over a scan",
                GroupStructInfoGoldenTest::generateUnnest));
        cases.add(new PlanCase("generate_with_an_on_condition",
                "a lateral ON condition over the generated slot",
                GroupStructInfoGoldenTest::generateWithAnOnCondition));
        cases.add(new PlanCase("generate_with_another_on_condition",
                "the same generate with another ON bound: a different key",
                GroupStructInfoGoldenTest::generateWithAnotherOnCondition));
        cases.add(new PlanCase("generate_explode_split",
                "generate of explode_split(cast(a as varchar), ',') over a scan: the delimiter is identity",
                GroupStructInfoGoldenTest::generateExplodeSplit));
        cases.add(new PlanCase("generate_outputs_x_y",
                "generate of two functions whose outputs are named x and y, in that order",
                GroupStructInfoGoldenTest::generateOutputsXY));
        cases.add(new PlanCase("generate_outputs_swapped",
                "the same two functions with the names of their outputs swapped: a different key",
                GroupStructInfoGoldenTest::generateOutputsSwapped));
        cases.add(new PlanCase("repeat_grouping_sets", "repeat of the grouping sets ((a), ()) over a scan",
                GroupStructInfoGoldenTest::repeatGroupingSets));
        cases.add(new PlanCase("invalid_too_many_scans",
                "cross join chain of 21 scans: more than hbo_max_scans_per_group",
                GroupStructInfoGoldenTest::invalidTooManyScans));
        // a dot inside a column name and a dot inside an alias must not render one identity
        cases.add(new PlanCase("a_table_whose_name_carries_a_brace",
                "a scan of a table whose name carries an opening brace",
                GroupStructInfoGoldenTest::scanOfTableWithABrace));
        cases.add(new PlanCase("a_column_whose_name_carries_a_dot",
                "a filter on the column `a.b` of the table t",
                GroupStructInfoGoldenTest::filterOnColumnWithADot));
        cases.add(new PlanCase("a_table_whose_alias_carries_a_dot",
                "a filter on the column b of the table aliased t.a",
                GroupStructInfoGoldenTest::filterOnAliasedTableWithADot));
        cases.add(new PlanCase("filter_one_of_those_conjuncts",
                "a filter with one conjunct: the shape a two conjunct filter must not share a key with",
                GroupStructInfoGoldenTest::filterOneOfThoseConjuncts));
        cases.add(new PlanCase("filter_two_conjuncts_which_fold_alike",
                "a filter with two conjuncts on one slot which fold to one text: both stay in the key",
                GroupStructInfoGoldenTest::filterTwoConjunctsWhichFoldAlike));
        cases.add(new PlanCase("filter_udf_is_active",
                "a filter whose only predicate is the java udf is_active over a scan",
                GroupStructInfoGoldenTest::filterUdfIsActive));
        cases.add(new PlanCase("filter_udf_is_deleted",
                "the same argument passed to another udf of the same database: a different key",
                GroupStructInfoGoldenTest::filterUdfIsDeleted));
        cases.add(new PlanCase("filter_udf_is_active_in_another_database",
                "the same udf name and catalog id in another database: a different key",
                GroupStructInfoGoldenTest::filterUdfIsActiveInAnotherDatabase));
        cases.add(new PlanCase("filter_global_udf_is_active",
                "a filter calling a udf of the global function manager (it has no database)",
                GroupStructInfoGoldenTest::filterGlobalUdfIsActive));
        cases.add(new PlanCase("filter_udf_is_active_other_overload",
                "the same udf name in the same database, another overload (catalog id): a different key",
                GroupStructInfoGoldenTest::filterUdfIsActiveOtherOverload));
        cases.add(new PlanCase("topn_with_a_literal_in_the_ordering_key",
                "top-n ordered by slot * 1: the factor decides which rows survive",
                GroupStructInfoGoldenTest::topnWithOneInTheOrderingKey));
        cases.add(new PlanCase("topn_with_another_literal_in_the_ordering_key",
                "the same top-n ordered by slot * -1: a different constant agnostic key",
                GroupStructInfoGoldenTest::topnWithMinusOneInTheOrderingKey));
        cases.add(new PlanCase("two_window_outputs_x_is_row_number",
                "two window definitions, x is the row_number: the output association is part of the key",
                GroupStructInfoGoldenTest::twoWindowOutputsXIsRowNumber));
        cases.add(new PlanCase("two_window_outputs_x_is_ntile",
                "the same two definitions with the aliases swapped: a different key",
                GroupStructInfoGoldenTest::twoWindowOutputsXIsNtile));
        cases.add(new PlanCase("window_function_with_a_literal",
                "window whose function carries a literal: it decides the values a node above it reads",
                GroupStructInfoGoldenTest::windowFunctionWithFourBuckets));
        cases.add(new PlanCase("window_function_with_another_literal",
                "the same window with another bucket count: a different constant agnostic key",
                GroupStructInfoGoldenTest::windowFunctionWithTenBuckets));
        cases.add(new PlanCase("window_with_two_order_keys_and_a_bounded_frame",
                "window over two ordering keys with a bounded frame: the separator and the offsets are part"
                        + " of the key",
                GroupStructInfoGoldenTest::windowWithTwoOrderKeysAndABoundedFrame));
        cases.add(new PlanCase("window_with_a_literal_in_the_ordering_key",
                "window ordered by slot * 1 under a filter: the factor decides the values above it",
                GroupStructInfoGoldenTest::windowWithOneInTheOrderingKey));
        cases.add(new PlanCase("window_with_another_literal_in_the_ordering_key",
                "the same window ordered by slot * -1: a different constant agnostic key",
                GroupStructInfoGoldenTest::windowWithMinusOneInTheOrderingKey));
        cases.add(new PlanCase("filter_above_a_file_scan",
                "empty filter over an external table scan: the token carries the qualified table and no"
                        + " data state",
                GroupStructInfoGoldenTest::filterAboveAFileScan));
        cases.add(new PlanCase("join_residual_conditions_per_mode_order",
                "inner join with two residual conditions whose literal carrying order differs from the"
                        + " constant agnostic order: each mode is rendered, sorted and de-duplicated on its own",
                GroupStructInfoGoldenTest::joinResidualConditionsPerModeOrder));
        return cases;
    }

    /** Build one plan case by name (a fresh memo: nothing is shared with the golden file build). */
    private static GroupStructInfo built(String name) {
        for (PlanCase planCase : planCases()) {
            if (planCase.name.equals(name)) {
                return planCase.builder.build();
            }
        }
        throw new AssertionError("no plan case named " + name);
    }

    private static LogicalOlapScan scan(long tableId, String tableName) {
        return PlanConstructor.newLogicalOlapScan(tableId, tableName, 0);
    }

    private static GroupStructInfo describe(Plan plan) {
        return GroupStructInfo.describeGroup(new Memo(null, plan).getRoot());
    }

    private static GroupStructInfo scanOnly() {
        return describe(scan(0, "t1"));
    }

    private static GroupStructInfo filterLiteral() {
        LogicalOlapScan t1 = scan(0, "t1");
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(t1.getOutput().get(0), new IntegerLiteral(1))), t1));
    }

    private static GroupStructInfo filterOfATable() {
        return filterOfTableWithId(0);
    }

    private static GroupStructInfo filterOfARecreatedTable() {
        return filterOfTableWithId(7);
    }

    /**
     * A filter over the table named t1, read from the table with the given id. Two tables of one name
     * have the same name, the same partitions and the same row count: only the id makes the scans
     * different, and a pinned entry of one must not be applied to the other.
     */
    private static GroupStructInfo filterOfTableWithId(long tableId) {
        LogicalOlapScan t1 = scan(tableId, "t1");
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new IsNull(t1.getOutput().get(0))), t1));
    }

    private static GroupStructInfo filterDatetimeWithMicrosecondOne() {
        return filterDatetimeWithMicrosecond(1);
    }

    private static GroupStructInfo filterDatetimeWithAnotherMicrosecond() {
        return filterDatetimeWithMicrosecond(2);
    }

    /**
     * A predicate comparing a datetime literal whose fractional part differs: the value of a datetime
     * literal is only yyyyMMddHHmmss, while execution compares the microseconds, so the printed literal
     * has to carry the fraction.
     */
    private static GroupStructInfo filterDatetimeWithMicrosecond(int microSecond) {
        LogicalOlapScan t1 = scan(0, "t1");
        DateTimeV2Literal literal = new DateTimeV2Literal(DateTimeV2Type.of(6),
                2020, 1, 1, 0, 0, 0, microSecond);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(t1.getOutput().get(0), literal)), t1));
    }

    private static GroupStructInfo filterLambdaArgumentsXY() {
        return filterLambdaArguments(false);
    }

    private static GroupStructInfo filterLambdaArgumentsSwapped() {
        return filterLambdaArguments(true);
    }

    /**
     * A filter whose predicate binds a lambda's parameters to two arrays: {@code (x,y) -> x > y} and
     * {@code (y,x) -> x > y} render the same body, while the first compares the first array with the second
     * and the second compares the second with the first - the argument names are the binding.
     */
    private static GroupStructInfo filterLambdaArguments(boolean swap) {
        LogicalOlapScan t1 = scan(0, "t1");
        Expression firstArray = new Cast(t1.getOutput().get(0), ArrayType.of(IntegerType.INSTANCE));
        Expression secondArray = new Cast(t1.getOutput().get(1), ArrayType.of(IntegerType.INSTANCE));
        ArrayItemReference first = new ArrayItemReference(swap ? "y" : "x", firstArray);
        ArrayItemReference second = new ArrayItemReference(swap ? "x" : "y", secondArray);
        ArrayItemReference xBinding = swap ? second : first;
        ArrayItemReference yBinding = swap ? first : second;
        Lambda lambda = new Lambda(swap ? ImmutableList.of("y", "x") : ImmutableList.of("x", "y"),
                new GreaterThan(xBinding.toSlot(), yBinding.toSlot()), ImmutableList.of(first, second));
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(new ArrayCount(lambda), new IntegerLiteral(0))), t1));
    }

    private static GroupStructInfo joinResidualConditionsPerModeOrder() {
        LogicalOlapScan t1 = scan(0, "t1");
        LogicalOlapScan t2 = scan(1, "t2");
        // `1 < t2.x` sorts before `2 < t1.y` when the literals are kept, while both render as
        // `lit(*) < col(...)` in the constant agnostic mode and then sort by the column: the two modes of
        // one join therefore print their conditions in a different order, which the identity has to keep
        return describe(new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(t1.getOutput().get(0), t2.getOutput().get(0))),
                ImmutableList.<Expression>of(new LessThan(new IntegerLiteral(1), t2.getOutput().get(0)),
                        new LessThan(new IntegerLiteral(2), t1.getOutput().get(0))),
                t1, t2, null));
    }

    private static GroupStructInfo scanOfTableWithABrace() {
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new IsNull(new SlotReference("a", IntegerType.INSTANCE, true, ImmutableList.of("t")))),
                scan(0, "a{b")));
    }

    private static GroupStructInfo filterOnColumnWithADot() {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference dotted = new SlotReference("a.b", IntegerType.INSTANCE, true, ImmutableList.of("t"));
        return describe(new LogicalFilter<>(ImmutableSet.of(new IsNull(dotted)), t1));
    }

    private static GroupStructInfo filterOnAliasedTableWithADot() {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference plain = new SlotReference("b", IntegerType.INSTANCE, true, ImmutableList.of("t.a"));
        return describe(new LogicalFilter<>(ImmutableSet.of(new IsNull(plain)), t1));
    }

    private static GroupStructInfo filterOneOfThoseConjuncts() {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference slot = (SlotReference) t1.getOutput().get(0);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(slot, new IntegerLiteral(1))), t1));
    }

    private static GroupStructInfo filterTwoConjunctsWhichFoldAlike() {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference slot = (SlotReference) t1.getOutput().get(0);
        // both conjuncts render as `EqualTo(col(...), lit(*))` in the constant agnostic mode and as two
        // different texts with their literals: the plan keeps a different number of rows either way
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(slot, new IntegerLiteral(1)),
                new EqualTo(slot, new IntegerLiteral(2))), t1));
    }

    private static GroupStructInfo filterUdfIsActive() {
        return filterUdf("is_active", 1, "db1");
    }

    private static GroupStructInfo filterUdfIsDeleted() {
        return filterUdf("is_deleted", 1, "db1");
    }

    private static GroupStructInfo filterUdfIsActiveInAnotherDatabase() {
        return filterUdf("is_active", 1, "db2");
    }

    private static GroupStructInfo filterGlobalUdfIsActive() {
        return filterUdf("is_active", 3, null);
    }

    private static GroupStructInfo filterUdfIsActiveOtherOverload() {
        return filterUdf("is_active", 2, "db1");
    }

    /**
     * A filter whose only predicate is a java udf call. Every java udf renders as the same wrapper
     * class, while the function it calls, the database which holds it and the catalog id of that
     * overload live in fields of the wrapper: the canonical form has to carry all three, otherwise the
     * four cases below share a key although a predicate above them keeps a different number of rows.
     */
    private static GroupStructInfo filterUdf(String name, long functionId, String dbName) {
        LogicalOlapScan t1 = scan(0, "t1");
        JavaUdf udf = new JavaUdf(name, functionId, dbName, org.apache.doris.catalog.Function.BinaryType.JAVA_UDF,
                FunctionSignature.ret(BooleanType.INSTANCE).args(IntegerType.INSTANCE),
                NullableMode.ALWAYS_NULLABLE, FunctionVolatility.IMMUTABLE,
                Udf.createVolatileIdentity(FunctionVolatility.IMMUTABLE),
                null, "evaluate", null, null, "", false, 360, t1.getOutput().get(0));
        return describe(new LogicalFilter<>(ImmutableSet.of(udf), t1));
    }

    private static GroupStructInfo topnWithOneInTheOrderingKey() {
        return topnOrderedByFactor(1);
    }

    private static GroupStructInfo topnWithMinusOneInTheOrderingKey() {
        return topnOrderedByFactor(-1);
    }

    private static GroupStructInfo topnOrderedByFactor(int factor) {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference slot = (SlotReference) t1.getOutput().get(0);
        return describe(new LogicalTopN<>(ImmutableList.of(
                new OrderKey(new Multiply(slot, new IntegerLiteral(factor)), true, true)), 3, 0, t1));
    }

    private static GroupStructInfo twoWindowOutputsXIsRowNumber() {
        return twoWindowOutputs(true);
    }

    private static GroupStructInfo twoWindowOutputsXIsNtile() {
        return twoWindowOutputs(false);
    }

    /**
     * Two window definitions over one table whose aliases are swapped between the two variants: `x` is
     * the row_number in one plan and the ntile in the other, so a predicate on x keeps a different number
     * of rows although both plans carry the same two definitions. The second definition also carries a
     * literal, which the fold has to protect even though it is not the first expression of the header.
     */
    private static GroupStructInfo twoWindowOutputs(boolean xIsRowNumber) {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference key = (SlotReference) t1.getOutput().get(0);
        WindowFrame frame = new WindowFrame(WindowFrame.FrameUnitsType.ROWS,
                WindowFrame.FrameBoundary.newPrecedingBoundary(),
                WindowFrame.FrameBoundary.newCurrentRowBoundary());
        ImmutableList<OrderExpression> orderKeys = ImmutableList.of(
                new OrderExpression(new OrderKey(key, true, true)));
        WindowExpression rowNumber = new WindowExpression(new RowNumber(), ImmutableList.of(), orderKeys, frame);
        WindowExpression ntile = new WindowExpression(new Ntile(new IntegerLiteral(4)),
                ImmutableList.of(), orderKeys, frame);
        // the definitions stay in the same order; only the names of their outputs are swapped, which is
        // exactly the pair the name prefix exists for (without it both render the same header)
        Alias x = new Alias(xIsRowNumber ? rowNumber : ntile, "x");
        Alias y = new Alias(xIsRowNumber ? ntile : rowNumber, "y");
        Slot outputX = xIsRowNumber ? x.toSlot() : y.toSlot();
        LogicalWindow<LogicalOlapScan> window = new LogicalWindow<>(
                ImmutableList.of(new Alias(rowNumber, xIsRowNumber ? "x" : "y"),
                        new Alias(ntile, xIsRowNumber ? "y" : "x")), t1);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(outputX, new IntegerLiteral(1))), window));
    }

    private static GroupStructInfo windowFunctionWithFourBuckets() {
        return windowWithAFunctionLiteral(4);
    }

    private static GroupStructInfo windowFunctionWithTenBuckets() {
        return windowWithAFunctionLiteral(10);
    }

    /**
     * A window whose <em>function</em> carries a literal: {@code ntile(4)} and {@code ntile(10)} keep a
     * different number of rows under a predicate above the window, and the constant of the function is
     * deliberately not normalized away by the analyzer, so this is the cell the identity rule exists for.
     */
    private static GroupStructInfo windowWithAFunctionLiteral(int buckets) {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference key = (SlotReference) t1.getOutput().get(0);
        WindowFrame frame = new WindowFrame(WindowFrame.FrameUnitsType.ROWS,
                WindowFrame.FrameBoundary.newPrecedingBoundary(),
                WindowFrame.FrameBoundary.newCurrentRowBoundary());
        WindowExpression window = new WindowExpression(new Ntile(new IntegerLiteral(buckets)),
                ImmutableList.of(key), ImmutableList.of(), frame);
        Alias alias = new Alias(window, "nt");
        LogicalWindow<LogicalOlapScan> logicalWindow = new LogicalWindow<>(ImmutableList.of(alias), t1);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new LessThanEqual(alias.toSlot(), new IntegerLiteral(1))), logicalWindow));
    }

    private static GroupStructInfo windowWithTwoOrderKeysAndABoundedFrame() {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference key = (SlotReference) t1.getOutput().get(0);
        WindowFrame frame = new WindowFrame(WindowFrame.FrameUnitsType.ROWS,
                WindowFrame.FrameBoundary.newPrecedingBoundary(new IntegerLiteral(2)),
                WindowFrame.FrameBoundary.newFollowingBoundary(new IntegerLiteral(1)));
        WindowExpression window = new WindowExpression(new RowNumber(), ImmutableList.of(),
                ImmutableList.of(
                        new OrderExpression(new OrderKey(key, true, true)),
                        new OrderExpression(new OrderKey(t1.getOutput().get(1), false, false))), frame);
        Alias alias = new Alias(window, "rn");
        LogicalWindow<LogicalOlapScan> logicalWindow = new LogicalWindow<>(ImmutableList.of(alias), t1);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(alias.toSlot(), new IntegerLiteral(1))), logicalWindow));
    }

    private static GroupStructInfo windowWithOneInTheOrderingKey() {
        return windowOrderedByFactor(1);
    }

    private static GroupStructInfo windowWithMinusOneInTheOrderingKey() {
        return windowOrderedByFactor(-1);
    }

    private static GroupStructInfo windowOrderedByFactor(int factor) {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference key = (SlotReference) t1.getOutput().get(0);
        WindowFrame frame = new WindowFrame(WindowFrame.FrameUnitsType.ROWS,
                WindowFrame.FrameBoundary.newPrecedingBoundary(),
                WindowFrame.FrameBoundary.newCurrentRowBoundary());
        WindowExpression window = new WindowExpression(new RowNumber(), ImmutableList.of(),
                ImmutableList.of(new OrderExpression(
                        new OrderKey(new Multiply(key, new IntegerLiteral(factor)), true, true))), frame);
        Alias alias = new Alias(window, "rn");
        LogicalWindow<LogicalOlapScan> logicalWindow = new LogicalWindow<>(ImmutableList.of(alias), t1);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(alias.toSlot(), new IntegerLiteral(1))), logicalWindow));
    }

    private static GroupStructInfo filterAboveAFileScan() {
        ExternalTable table = Mockito.mock(ExternalTable.class);
        Mockito.when(table.getName()).thenReturn("t");
        CatalogIf<?> catalog = Mockito.mock(CatalogIf.class);
        Mockito.when(catalog.getName()).thenReturn("hive");
        DatabaseIf<?> database = Mockito.mock(DatabaseIf.class);
        Mockito.when(database.getCatalog()).thenReturn(catalog);
        Mockito.when(database.getFullName()).thenReturn("db");
        Mockito.when(table.getDatabase()).thenReturn(database);
        LogicalFileScan scan = new LogicalFileScan(new RelationId(1), table,
                ImmutableList.of("hive", "db", "t"), ImmutableList.of(), Optional.empty(), Optional.empty(),
                Optional.empty(), Optional.empty());
        return describe(new LogicalFilter<>(ImmutableSet.of(), scan));
    }

    private static GroupStructInfo filterSlotEqualSlot() {
        LogicalOlapScan t1 = scan(0, "t1");
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(t1.getOutput().get(0), t1.getOutput().get(1))), t1));
    }

    private static GroupStructInfo sortAboveAFilter() {
        LogicalOlapScan t1 = scan(0, "t1");
        LogicalFilter<LogicalOlapScan> filter = new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(t1.getOutput().get(0), new IntegerLiteral(1))), t1);
        return describe(new LogicalSort<>(ImmutableList.of(
                new OrderKey(t1.getOutput().get(1), true, true)), filter));
    }

    private static GroupStructInfo projectAboveScan() {
        LogicalOlapScan t1 = scan(0, "t1");
        return describe(new LogicalProject<>(ImmutableList.<NamedExpression>copyOf(t1.getOutput()), t1));
    }

    private static GroupStructInfo joinInnerTwoScans() {
        LogicalOlapScan t1 = scan(0, "t1");
        LogicalOlapScan t2 = scan(1, "t2");
        return describe(new LogicalJoin<>(JoinType.INNER_JOIN, ImmutableList.of(
                new EqualTo(t1.getOutput().get(0), t2.getOutput().get(0))), t1, t2, null));
    }

    private static GroupStructInfo joinInnerChainThreeLeftDeep() {
        LogicalOlapScan t1 = scan(0, "t1");
        LogicalOlapScan t2 = scan(1, "t2");
        LogicalOlapScan t3 = scan(2, "t3");
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> inner = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(t1.getOutput().get(0), t2.getOutput().get(0))), t1, t2, null);
        return describe(new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(t1.getOutput().get(1), t3.getOutput().get(0))), inner, t3, null));
    }

    private static GroupStructInfo joinInnerChainThreeRightDeep() {
        LogicalOlapScan t1 = scan(0, "t1");
        LogicalOlapScan t2 = scan(1, "t2");
        LogicalOlapScan t3 = scan(2, "t3");
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> cross = new LogicalJoin<>(JoinType.CROSS_JOIN, t2, t3, null);
        return describe(new LogicalJoin<>(JoinType.INNER_JOIN, ImmutableList.of(
                new EqualTo(t1.getOutput().get(0), t2.getOutput().get(0)),
                new EqualTo(t1.getOutput().get(1), t3.getOutput().get(0))), t1, cross, null));
    }

    private static GroupStructInfo crossJoinTwoScans() {
        return describe(new LogicalJoin<>(JoinType.CROSS_JOIN, scan(0, "t1"), scan(1, "t2"), null));
    }

    private static GroupStructInfo joinLeftOuter() {
        LogicalOlapScan t1 = scan(0, "t1");
        LogicalOlapScan t2 = scan(1, "t2");
        return describe(new LogicalJoin<>(JoinType.LEFT_OUTER_JOIN, ImmutableList.of(
                new EqualTo(t1.getOutput().get(0), t2.getOutput().get(0))), t1, t2, null));
    }

    private static GroupStructInfo joinOverAComputedProject() {
        return joinOverAComputedProject(2);
    }

    /**
     * A join whose condition reads the alias of a computed project below it. A join chain flattens the
     * projects it passes, so such a project used to be skipped and its definition was lost: the two
     * moduli rendered one {@code col(x)} although the join keeps a different number of rows.
     */
    private static GroupStructInfo joinOverAComputedProject(int modulo) {
        LogicalOlapScan t = scan(0, "t");
        LogicalOlapScan u = scan(1, "u");
        Alias x = new Alias(new Mod(t.getOutput().get(0), new IntegerLiteral(modulo)), "x");
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(x), t);
        return describe(new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(x.toSlot(), u.getOutput().get(0))), project, u, null));
    }

    private static GroupStructInfo joinOverAnotherComputedProject() {
        return joinOverAComputedProject(3);
    }

    private static GroupStructInfo joinWithAliasesXY() {
        return joinWithAliases(false);
    }

    private static GroupStructInfo joinWithAliasesSwapped() {
        return joinWithAliases(true);
    }

    /**
     * {@code Join(x.a = y.a AND x.b < y.b, t AS x, u AS y)} and the same ON clause over
     * {@code u AS x, t AS y}: the predicates name the aliases and the leaves are sorted by their tables,
     * so only the binding of an alias to its relation tells the two plans apart - the ordered comparison
     * keeps one row in the first plan and none in the second.
     */
    private static GroupStructInfo joinWithAliases(boolean swap) {
        LogicalOlapScan t = scan(0, "t");
        LogicalOlapScan u = scan(1, "u");
        LogicalOlapScan xScan = (swap ? u : t).withTableAlias("x");
        LogicalOlapScan yScan = (swap ? t : u).withTableAlias("y");
        List<NamedExpression> xOutput = aliasOutput(xScan, "x");
        List<NamedExpression> yOutput = aliasOutput(yScan, "y");
        return describe(new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(xOutput.get(0), yOutput.get(0)),
                        new LessThan(xOutput.get(1), yOutput.get(1))),
                new LogicalProject<>(xOutput, xScan), new LogicalProject<>(yOutput, yScan), null));
    }

    /** The forwarding projection the alias rule inserts: the scan's slots qualified by the alias. */
    private static List<NamedExpression> aliasOutput(LogicalOlapScan scan, String alias) {
        List<NamedExpression> output = new ArrayList<>();
        for (Slot slot : scan.getOutput()) {
            output.add(new SlotReference(slot.getExprId(), slot.getName(), slot.getDataType(), slot.nullable(),
                    ImmutableList.of("db", alias)));
        }
        return output;
    }

    private static GroupStructInfo limit() {
        return describe(new LogicalLimit<>(5, 0, LimitPhase.ORIGIN, scan(0, "t1")));
    }

    private static GroupStructInfo topn() {
        LogicalOlapScan t1 = scan(0, "t1");
        return describe(new LogicalTopN<>(ImmutableList.of(
                new OrderKey(t1.getOutput().get(0), true, true)), 3, 0, t1));
    }

    private static GroupStructInfo aggregateGroupByKeys() {
        LogicalOlapScan t1 = scan(0, "t1");
        Expression key = t1.getOutput().get(0);
        return describe(new LogicalAggregate<>(ImmutableList.of(key),
                ImmutableList.of((NamedExpression) key), true, Optional.empty(), t1));
    }

    private static GroupStructInfo aggregateGroupingByAComputedProject() {
        return aggregateGroupingByModulo(2);
    }

    private static GroupStructInfo aggregateGroupingByAnotherComputedProject() {
        return aggregateGroupingByModulo(3);
    }

    /**
     * {@code GROUP BY a % n} is normalized into an aggregation which groups by the alias of a project
     * below it, and that project is transparent: only the definition of the alias distinguishes the
     * aggregations, whose group counts differ.
     */
    private static GroupStructInfo aggregateGroupingByModulo(int modulo) {
        LogicalOlapScan t1 = scan(0, "t1");
        Alias x = new Alias(new Mod(t1.getOutput().get(0), new IntegerLiteral(modulo)), "x");
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(x), t1);
        return describe(new LogicalAggregate<>(ImmutableList.of(x.toSlot()),
                ImmutableList.<NamedExpression>of(x.toSlot()), true, Optional.empty(), project));
    }

    /**
     * The same grouping written inline, without the project the normalization inserts: the definition
     * of the alias is resolved to the expression the alias wraps, so both forms share one key.
     */
    private static GroupStructInfo aggregateGroupingByTheSameExpressionDirectly() {
        LogicalOlapScan t1 = scan(0, "t1");
        Expression key = new Mod(t1.getOutput().get(0), new IntegerLiteral(2));
        return describe(new LogicalAggregate<>(ImmutableList.of(key),
                ImmutableList.<NamedExpression>of(new Alias(key, "x")), true, Optional.empty(), t1));
    }

    /**
     * A plain rename is an alias too: the definition of x is the column a, so a filter on x and a
     * filter on a (or on the same name above another column) must not share a key.
     */
    private static GroupStructInfo filterAboveARenamedProjectOutput() {
        LogicalOlapScan t1 = scan(0, "t1");
        Alias x = new Alias(t1.getOutput().get(0), "x");
        LogicalProject<LogicalOlapScan> project = new LogicalProject<>(
                ImmutableList.<NamedExpression>of(x), t1);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(x.toSlot(), new IntegerLiteral(1))), project));
    }

    private static GroupStructInfo filterAboveAnAggregateCount() {
        return filterAboveAnAggregateCount(false);
    }

    /**
     * A predicate which reads a count: {@code COUNT(DISTINCT v)} and {@code COUNT(v)} have the same child
     * while they count different values, so a v of [1, 1] satisfies {@code s = 1} for the second only.
     */
    private static GroupStructInfo filterAboveAnAggregateCount(boolean distinct) {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference key = (SlotReference) t1.getOutput().get(0);
        Expression value = t1.getOutput().get(1);
        Alias s = new Alias(new Count(distinct, value), "s");
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(ImmutableList.of(key),
                ImmutableList.<NamedExpression>of(key, s), true, Optional.empty(), t1);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(s.toSlot(), new IntegerLiteral(1))), aggregate));
    }

    private static GroupStructInfo filterAboveAnAggregateCountDistinct() {
        return filterAboveAnAggregateCount(true);
    }

    private static GroupStructInfo filterAboveAnAggregateMin() {
        return filterAboveAnAggregateFunction(true);
    }

    private static GroupStructInfo filterAboveAnAggregateMax() {
        return filterAboveAnAggregateFunction(false);
    }

    /**
     * A predicate which reads the output of an aggregate function: {@code MIN(v) AS s} and
     * {@code MAX(v) AS s} keep the same alias and the same grouping key, yet a group whose values are
     * 1 and 2 satisfies {@code s = 1} for the minimum and not for the maximum, so the filter keeps a
     * different number of rows.
     */
    private static GroupStructInfo filterAboveAnAggregateFunction(boolean min) {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference key = (SlotReference) t1.getOutput().get(0);
        Expression value = t1.getOutput().get(1);
        Alias s = new Alias(min ? new Min(value) : new Max(value), "s");
        LogicalAggregate<LogicalOlapScan> aggregate = new LogicalAggregate<>(ImmutableList.of(key),
                ImmutableList.<NamedExpression>of(key, s), true, Optional.empty(), t1);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(s.toSlot(), new IntegerLiteral(1))), aggregate));
    }

    private static GroupStructInfo filterMatchUsingAnalyzerA1() {
        return filterMatchUsingAnalyzer("a1");
    }

    private static GroupStructInfo filterMatchUsingAnalyzerA2() {
        return filterMatchUsingAnalyzer("a2");
    }

    /**
     * `MATCH 'foo-bar' USING ANALYZER a1` and the same predicate with a2 have equal children but
     * select a different analyzer index, so the predicate keeps a different number of rows.
     */
    private static GroupStructInfo filterMatchUsingAnalyzer(String analyzer) {
        LogicalOlapScan t1 = scan(0, "t1");
        MatchAll match = new MatchAll(t1.getOutput().get(1), new VarcharLiteral("foo-bar"), analyzer);
        return describe(new LogicalFilter<>(ImmutableSet.of(match), t1));
    }

    private static GroupStructInfo filterSearchExactApple() {
        return filterSearch("name:EXACT(apple)", "apple");
    }

    private static GroupStructInfo filterSearchExactBanana() {
        return filterSearch("name:EXACT(banana)", "banana");
    }

    /**
     * A search predicate keeps the dsl string and the parsed query in fields, and both are sent to
     * execution: its slot children only say which column is searched, so the terms and operators of
     * the dsl are the identity of the predicate.
     */
    private static GroupStructInfo filterSearch(String dsl, String term) {
        LogicalOlapScan t1 = scan(0, "t1");
        SearchDslParser.QsNode root = new SearchDslParser.QsNode(
                SearchDslParser.QsClauseType.TERM, "name", term);
        SearchDslParser.QsPlan qsPlan = new SearchDslParser.QsPlan(root,
                ImmutableList.of(new SearchDslParser.QsFieldBinding("name", 0)));
        SearchExpression search = new SearchExpression(dsl, qsPlan, ImmutableList.of(t1.getOutput().get(1)));
        return describe(new LogicalFilter<>(ImmutableSet.of(search), t1));
    }

    private static GroupStructInfo aggregateNoKeys() {
        LogicalOlapScan t1 = scan(0, "t1");
        return describe(new LogicalAggregate<>(ImmutableList.of(),
                ImmutableList.of((NamedExpression) t1.getOutput().get(0)), true, Optional.empty(), t1));
    }

    /**
     * A self join of one table: the same scan object is both children, so the memo holds one group for
     * it and the join references that group twice. {@code new Memo(null, plan)} refuses a repeated
     * child ("groupExpression already exists in memo"), while {@code copyIn} is exactly the path the
     * optimizer takes: the second occurrence resolves to the group of the first one.
     */
    private static GroupStructInfo selfJoinOneTable() {
        LogicalOlapScan t1 = scan(0, "t1");
        LogicalJoin<LogicalOlapScan, LogicalOlapScan> join = new LogicalJoin<>(JoinType.INNER_JOIN,
                ImmutableList.of(new EqualTo(t1.getOutput().get(0), t1.getOutput().get(1))), t1, t1, null);
        CopyInResult copied = new Memo().copyIn(join, null, false);
        return GroupStructInfo.describeGroup(copied.correspondingExpression.getOwnerGroup());
    }

    private static GroupStructInfo unionConstantBranch() {
        LogicalOlapScan t1 = scan(0, "t1");
        LogicalOlapScan t2 = scan(1, "t2");
        List<List<NamedExpression>> constants = ImmutableList.of(
                ImmutableList.<NamedExpression>of(new Alias(new IntegerLiteral(2), "x")));
        return describe(new LogicalUnion(SetOperation.Qualifier.ALL, constants, ImmutableList.of(t1, t2)));
    }

    private static GroupStructInfo intersectTwoScans() {
        return describe(new LogicalIntersect(SetOperation.Qualifier.ALL,
                ImmutableList.of(scan(0, "t1"), scan(1, "t2"))));
    }

    private static GroupStructInfo exceptTwoScans() {
        return describe(new LogicalExcept(SetOperation.Qualifier.ALL,
                ImmutableList.of(scan(0, "t1"), scan(1, "t2"))));
    }

    private static GroupStructInfo unionTwoScansSelectedA() {
        return unionTwoScansSelecting(false);
    }

    private static GroupStructInfo unionTwoScansSelectedB() {
        return unionTwoScansSelecting(true);
    }

    /**
     * `SELECT a FROM t UNION SELECT a FROM u` and the same query selecting `u.b`: the two branches are
     * the same scans and the projects which select the columns are transparent, so only the mapping of
     * the second branch differs, while the number of distinct rows does.
     */
    private static GroupStructInfo unionTwoScansSelecting(boolean selectB) {
        LogicalOlapScan t = scan(0, "t");
        LogicalOlapScan u = scan(1, "u");
        SlotReference output = new SlotReference("a", IntegerType.INSTANCE);
        SlotReference fromT = (SlotReference) t.getOutput().get(0);
        SlotReference fromU = (SlotReference) u.getOutput().get(selectB ? 1 : 0);
        return describe(new LogicalUnion(SetOperation.Qualifier.ALL,
                ImmutableList.<NamedExpression>of(output),
                ImmutableList.<List<SlotReference>>of(ImmutableList.of(fromT), ImmutableList.of(fromU)),
                ImmutableList.of(), false, ImmutableList.of(t, u)));
    }

    private static GroupStructInfo windowUnderAFilter() {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference partitionKey = (SlotReference) t1.getOutput().get(0);
        WindowFrame frame = new WindowFrame(WindowFrame.FrameUnitsType.ROWS,
                WindowFrame.FrameBoundary.newPrecedingBoundary(),
                WindowFrame.FrameBoundary.newCurrentRowBoundary());
        WindowExpression window = new WindowExpression(new RowNumber(), ImmutableList.of(partitionKey),
                ImmutableList.of(), frame);
        Alias alias = new Alias(window, "rn");
        LogicalWindow<LogicalOlapScan> logicalWindow = new LogicalWindow<>(ImmutableList.of(alias), t1);
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new GreaterThan(alias.toSlot(), new IntegerLiteral(1))), logicalWindow));
    }

    private static GroupStructInfo generateUnnest() {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference generatorOutput = new SlotReference("g", IntegerType.INSTANCE);
        return describe(new LogicalGenerate<>(ImmutableList.of(new Unnest(new IntegerLiteral(1))),
                ImmutableList.of(generatorOutput), t1));
    }

    private static GroupStructInfo generateWithAnOnCondition() {
        return generateWithAnOnBound(0);
    }

    private static GroupStructInfo generateWithAnotherOnCondition() {
        return generateWithAnOnBound(1000);
    }

    /** A generate whose lateral ON condition filters the produced rows. */
    private static GroupStructInfo generateWithAnOnBound(int bound) {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference generatorOutput = new SlotReference("x", VarcharType.SYSTEM_DEFAULT);
        Expression argument = new Cast(t1.getOutput().get(0), VarcharType.SYSTEM_DEFAULT);
        return describe(new LogicalGenerate<>(
                ImmutableList.of(new ExplodeSplit(argument, new VarcharLiteral(","))),
                ImmutableList.of(generatorOutput), ImmutableList.of(),
                ImmutableList.of(new GreaterThan(generatorOutput, new VarcharLiteral(String.valueOf(bound)))),
                t1));
    }

    private static GroupStructInfo generateExplodeSplit() {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference generatorOutput = new SlotReference("x", VarcharType.SYSTEM_DEFAULT);
        Expression argument = new Cast(t1.getOutput().get(0), VarcharType.SYSTEM_DEFAULT);
        return describe(new LogicalGenerate<>(
                ImmutableList.of(new ExplodeSplit(argument, new VarcharLiteral(","))),
                ImmutableList.of(generatorOutput), t1));
    }

    private static GroupStructInfo generateOutputsXY() {
        return generateWithOutputNames(false);
    }

    private static GroupStructInfo generateOutputsSwapped() {
        return generateWithOutputNames(true);
    }

    /**
     * The same two generator functions in the same order, with the names of the two columns they produce
     * swapped: a predicate above the generate reads one of the columns, so the binding of an output name to
     * the function which fills it is part of the identity.
     */
    private static GroupStructInfo generateWithOutputNames(boolean swap) {
        LogicalOlapScan t1 = scan(0, "t1");
        Expression first = new ExplodeSplit(new Cast(t1.getOutput().get(0), VarcharType.SYSTEM_DEFAULT),
                new VarcharLiteral(","));
        Expression second = new ExplodeSplit(new Cast(t1.getOutput().get(1), VarcharType.SYSTEM_DEFAULT),
                new VarcharLiteral(","));
        Slot x = new SlotReference("x", VarcharType.SYSTEM_DEFAULT);
        Slot y = new SlotReference("y", VarcharType.SYSTEM_DEFAULT);
        return describe(new LogicalGenerate<>(
                ImmutableList.of((Function) first, (Function) second),
                ImmutableList.of(swap ? y : x, swap ? x : y), t1));
    }

    private static GroupStructInfo repeatGroupingSets() {
        LogicalOlapScan t1 = scan(0, "t1");
        NamedExpression key = t1.getOutput().get(0);
        List<List<Expression>> groupingSets = ImmutableList.of(
                ImmutableList.<Expression>of(key), ImmutableList.<Expression>of());
        return describe(new LogicalRepeat<>(groupingSets, ImmutableList.of(key),
                RepeatType.GROUPING_SETS, t1));
    }

    private static GroupStructInfo invalidTooManyScans() {
        Plan plan = scan(0, "t1");
        for (int i = 1; i <= GroupStructInfo.DEFAULT_MAX_SCANS_PER_GROUP; i++) {
            plan = new LogicalJoin<>(JoinType.CROSS_JOIN, plan, scan(i, "t" + (i + 1)), null);
        }
        return describe(plan);
    }

    // ------------------------------------------------------------------------------------------
    // the text cases
    // ------------------------------------------------------------------------------------------

    private static List<TextCase> textCases() {
        List<TextCase> cases = new ArrayList<>();
        // toNoLiteral: the fold of the literal carrying canonical form
        // a filter whose two conditions fold to texts which order differently from the literal carrying
        // ones: the fold has to re-sort them, or the pair EXPLAIN prints is rejected in the other mode
        cases.add(new TextCase("toNoLiteral", "a_fold_reorders_the_conditions_of_a_filter",
                "F{EqualTo(concat(lit(%27b%27:VARCHAR(1)),col(t.r)),lit(%27y%27:VARCHAR(1)));"
                        + "EqualTo(concat(lit(%27a%27:VARCHAR(1)),col(t.s)),lit(%27x%27:VARCHAR(1)))}"
                        + "(S{internal.hbo_test.t1,v3,r1010})"));
        // a scan token is copied verbatim: a table whose name contains `lit(` must not be folded into a
        // token which names another table
        cases.add(new TextCase("toNoLiteral", "a_table_named_like_a_literal_is_not_folded",
                "F{IsNull(col(t.a))}(S{internal.db.lit(%31),v2,r1000})"));
        cases.add(new TextCase("toNoLiteral", "a_foldable_literal",
                "F{EqualTo(col(t.b),lit(1:INT))}(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("toNoLiteral", "a_foldable_literal_next_to_a_protected_header",
                "F{EqualTo(col(t.b),lit(1:INT))}(L{5,0}(S{internal.hbo_test.t1,v3,r1010}))"));
        cases.add(new TextCase("toNoLiteral", "a_literal_inside_a_limit_is_already_none",
                "L{5,0}(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("toNoLiteral", "a_protected_generate_header_is_copied_verbatim",
                "F{EqualTo(col(tmp.x),lit(1:INT))}(G{tf:[ExplodeSplit(Cast(col(internal.hbo_test.t1.a)),"
                        + "lit(%2C:VARCHAR(1)))]}(S{internal.hbo_test.t1,v3,r1010}))"));
        cases.add(new TextCase("toNoLiteral", "a_protected_topn_header_is_copied_verbatim",
                "T{3,0;ord:[col(internal.hbo_test.t1.a):asc:nulls_first]}"
                        + "(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("toNoLiteral", "a_protected_window_header_is_copied_verbatim",
                "W{RowNumber;part:[col(internal.hbo_test.t1.a)];ord:[];"
                        + "frame:[WindowFrame(ROWS, UNBOUNDED_PRECEDING, CURRENT_ROW)]}"
                        + "(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("toNoLiteral", "a_protected_union_header_keeps_its_constant",
                "U{all,cols:[col(t.a)],const:[Alias(lit(2:INT))]}(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("toNoLiteral", "a_protected_intersect_header_is_copied_verbatim",
                "I{distinct,cols:[col(t.a)]}(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("toNoLiteral", "a_protected_except_header_is_copied_verbatim",
                "E{distinct,cols:[col(t.a)]}(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("toNoLiteral", "a_type_which_contains_parentheses",
                "F{EqualTo(col(t.s),lit(abc:VARCHAR(10)))}(S{internal.hbo_test.t1,v3,r10})"));
        cases.add(new TextCase("toNoLiteral", "a_value_with_balanced_parentheses",
                "F{EqualTo(col(t.s),lit(IL (north):VARCHAR))}(S{internal.hbo_test.t1,v3,r10})"));
        cases.add(new TextCase("toNoLiteral", "an_unbalanced_parenthesis",
                "F{EqualTo(col(t.a),lit(1:INT}(S{internal.hbo_test.t1,v3,r1010}"));
        cases.add(new TextCase("toNoLiteral", "a_literal_closed_by_a_later_parenthesis",
                "F{EqualTo(col(t.a),lit(1:INT}(S{internal.hbo_test.t1,v3,r1010}))"));
        cases.add(new TextCase("toNoLiteral", "a_literal_which_never_closes", "lit("));
        cases.add(new TextCase("toNoLiteral", "a_protected_header_which_never_closes", "L{5,0"));
        cases.add(new TextCase("toNoLiteral", "an_already_constant_agnostic_literal",
                "F{EqualTo(col(t.b),lit(*))}(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("toNoLiteral", "a_text_without_any_literal",
                "J{inner,c:[]}(S{internal.hbo_test.t1,v3,r1010};S{internal.hbo_test.t2,v2,r1000})"));

        // stripScanBaseline: the fold a pasted struct info is hashed through
        cases.add(new TextCase("stripScanBaseline", "a_token_with_a_data_state",
                "F{EqualTo(col(t.b),lit(1:INT))}(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("stripScanBaseline", "a_token_without_a_data_state",
                "J{inner,c:[]}(S{null.null.t1};S{null.null.t2})"));
        cases.add(new TextCase("stripScanBaseline", "a_token_with_a_partition_identity",
                "F{GreaterThan(col(t.b),lit(1:INT))}(S{internal.hbo_test.t#pt:10001:10002,v2,r5,p1/2})"));
        cases.add(new TextCase("stripScanBaseline", "a_token_with_a_tablet_identity",
                "S{internal.hbo_test.t#tb:10001:10002,v2,r5}"));
        cases.add(new TextCase("stripScanBaseline", "a_token_with_an_index_identity",
                "S{internal.hbo_test.t#ix:10004,v2,r5}"));
        cases.add(new TextCase("stripScanBaseline", "every_token_of_a_chain",
                "J{inner,c:[]}(S{internal.hbo_test.t1,v3,r1010};S{internal.hbo_test.t2,v2,r1000})"));
        cases.add(new TextCase("stripScanBaseline", "a_token_which_follows_a_brace",
                "L{5,0}S{internal.hbo_test.t1,v2,r1}"));
        cases.add(new TextCase("stripScanBaseline", "a_canonical_without_a_scan_token",
                "A{gb:col(t.a)}(F{EqualTo(col(t.b),lit(1:INT))})"));
        cases.add(new TextCase("stripScanBaseline", "an_escaped_identifier_is_not_a_scan_token",
                "F{EqualTo(col(internal.hbo_test.t.a%3BS%7Bx%2Cv2%7D),lit(1:INT))}"
                        + "(S{internal.hbo_test.t,v2,r1})"));
        cases.add(new TextCase("stripScanBaseline", "an_escaped_literal_is_not_a_scan_token",
                "F{EqualTo(col(t.s),lit(%3BS%7Bx%2Cv2%7D:VARCHAR))}(S{internal.hbo_test.t,v2,r1})"));
        cases.add(new TextCase("stripScanBaseline", "a_raw_token_inside_a_literal_is_read_as_one",
                "F{EqualTo(col(t.s),lit(S{a,v2}:VARCHAR))}(S{internal.hbo_test.t,v2,r1})"));

        // the literal value codec: a value can not forge the structure of the canonical form
        cases.add(new TextCase("encodeLiteralValue", "every_reserved_character", "\\%(){}[],;:|"));
        cases.add(new TextCase("encodeLiteralValue", "a_plain_value_is_the_identity", "hbo_test"));
        cases.add(new TextCase("encodeLiteralValue", "a_value_with_spaces", "IL (north)"));
        cases.add(new TextCase("encodeLiteralValue", "a_mixed_value", "a,b;c{d}"));
        cases.add(new TextCase("decodeLiteralValue", "an_encoded_value", "%3BS%7Bx%2Cv2%7D"));
        cases.add(new TextCase("decodeLiteralValue", "lowercase_hex_is_accepted", "%2c%7b"));
        cases.add(new TextCase("decodeLiteralValue", "a_stray_percent_is_kept", "a%zz"));
        cases.add(new TextCase("decodeLiteralValue", "a_trailing_percent_is_kept", "a%2"));

        // the simplified display path
        cases.add(new TextCase("SimpleStructInfo.render", "a_filter_over_a_scan",
                "F{EqualTo(col(internal.hbo_test.t1.b),lit(1:INT))}(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("SimpleStructInfo.render", "an_inner_join_chain",
                "J{inner,c:[EqualTo(col(internal.hbo_test.t2.a),col(internal.hbo_test.t1.a))]}"
                        + "(S{internal.hbo_test.t1,v3,r1010};S{internal.hbo_test.t2,v2,r1000})"));
        cases.add(new TextCase("SimpleStructInfo.render", "an_aggregation_keeps_its_grouping_keys",
                "A{gb:col(internal.hbo_test.t1.a)}(S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("SimpleStructInfo.render", "a_self_join_numbers_the_occurrences",
                "J{inner,c:[]}(S{internal.hbo_test.t1,v3,r1010};S{internal.hbo_test.t1,v3,r1010})"));
        cases.add(new TextCase("SimpleStructInfo.render", "a_partition_selection_is_kept",
                "S{internal.hbo_test.t1,v3,r10,p1/2}"));
        cases.add(new TextCase("SimpleStructInfo.render", "a_limit_is_not_a_canonical_node",
                "L{5,0}(S{internal.hbo_test.t1,v3,r10})"));
        cases.add(new TextCase("SimpleStructInfo.render", "a_text_which_is_no_canonical_form", "hello"));
        cases.add(new TextCase("SimpleStructInfo.render", "an_empty_text", ""));

        // the scan token: parse and render
        cases.add(new TextCase("HboScanDescriptor", "with_a_data_state",
                "internal.hbo_test.t1,v3,r1010,p1/5"));
        cases.add(new TextCase("HboScanDescriptor", "without_a_data_state", "null.null.t1"));
        cases.add(new TextCase("HboScanDescriptor", "with_an_estimated_row_count",
                "internal.hbo_test.t1,e7"));
        cases.add(new TextCase("HboScanDescriptor", "with_every_identity",
                "internal.hbo_test.t1#pt:10001:10002#tb:10003:10004#ix:10005,v3,r9,p1/2"));
        cases.add(new TextCase("HboScanDescriptor", "with_a_complete_partition_selection",
                "internal.hbo_test.t1,v3,r10,p5/5"));
        return cases;
    }

    // ------------------------------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------------------------------

    private static String sha256(String value) {
        return Hashing.sha256().hashString(value, StandardCharsets.UTF_8).toString();
    }

    private static List<String> readResourceLines(String name) throws IOException {
        Path file = RESOURCE_DIR.resolve(name);
        Assertions.assertTrue(Files.exists(file),
                "the golden resource is not at " + file.toAbsolutePath() + " (run the test from fe/fe-core)");
        return Files.readAllLines(file, StandardCharsets.UTF_8);
    }

    /** Print the lines a regeneration moved, so a blessed change is visible in the run which blessed it. */
    private static void reportWhatMoved(List<String> before, List<String> after) {
        int changed = 0;
        int common = Math.min(before.size(), after.size());
        for (int i = 0; i < common; i++) {
            if (!before.get(i).equals(after.get(i))) {
                changed++;
                System.out.println("[hbo-golden] line " + (i + 1) + " moved:\n  old: " + before.get(i)
                        + "\n  new: " + after.get(i));
            }
        }
        if (before.size() != after.size()) {
            changed += Math.abs(before.size() - after.size());
            System.out.println("[hbo-golden] the file " + (after.size() > before.size() ? "grew" : "shrank")
                    + " from " + before.size() + " to " + after.size() + " lines");
        }
        // lines without a positional counterpart (a pure append or truncation) are printed by content, so
        // a blessed change is never reported as "nothing moved" in the run which blessed it
        for (int i = common; i < after.size(); i++) {
            System.out.println("[hbo-golden] line " + (i + 1) + " added: " + after.get(i));
        }
        for (int i = common; i < before.size(); i++) {
            System.out.println("[hbo-golden] line " + (i + 1) + " removed: " + before.get(i));
        }
        System.out.println("[hbo-golden] " + changed + " line(s) differ from the committed corpus");
    }

    /**
     * Compare the committed corpus with the freshly generated one line by line, so that a mismatch
     * names the line which moved instead of dumping the whole file.
     */

    private static void assertSameLines(List<String> committed, String generated) {
        List<String> actual = ImmutableList.copyOf(generated.split("\n", -1));
        int common = Math.min(committed.size(), actual.size());
        for (int i = 0; i < common; i++) {
            if (!committed.get(i).equals(actual.get(i))) {
                Assertions.fail("the generated corpus differs from " + GOLDEN_FILE_NAME + " at line "
                        + (i + 1) + ":\n  committed: " + committed.get(i) + "\n  generated: " + actual.get(i)
                        + "\n\nregenerate it with -D" + DUMP_PROPERTY + "=true and review every changed line:"
                        + " it is the hbo cache key of the affected shapes.");
            }
        }
        if (committed.size() != actual.size()) {
            String extra = committed.size() > actual.size()
                    ? "committed: " + committed.get(common)
                    : "generated: " + actual.get(common);
            Assertions.fail("the generated corpus has " + actual.size() + " lines, " + GOLDEN_FILE_NAME
                    + " has " + committed.size() + "; the first line which is only in one of them is "
                    + extra);
        }
    }
}
