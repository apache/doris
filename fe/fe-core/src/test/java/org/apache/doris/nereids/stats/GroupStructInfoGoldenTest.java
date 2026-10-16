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

import org.apache.doris.nereids.memo.CopyInResult;
import org.apache.doris.nereids.memo.Group;
import org.apache.doris.nereids.memo.Memo;
import org.apache.doris.nereids.properties.OrderKey;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.EqualTo;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.GreaterThan;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.SlotReference;
import org.apache.doris.nereids.trees.expressions.WindowExpression;
import org.apache.doris.nereids.trees.expressions.WindowFrame;
import org.apache.doris.nereids.trees.expressions.functions.generator.ExplodeSplit;
import org.apache.doris.nereids.trees.expressions.functions.generator.Unnest;
import org.apache.doris.nereids.trees.expressions.functions.window.RowNumber;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.JoinType;
import org.apache.doris.nereids.trees.plans.LimitPhase;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.algebra.Repeat.RepeatType;
import org.apache.doris.nereids.trees.plans.algebra.SetOperation;
import org.apache.doris.nereids.trees.plans.logical.LogicalAggregate;
import org.apache.doris.nereids.trees.plans.logical.LogicalExcept;
import org.apache.doris.nereids.trees.plans.logical.LogicalFilter;
import org.apache.doris.nereids.trees.plans.logical.LogicalGenerate;
import org.apache.doris.nereids.trees.plans.logical.LogicalIntersect;
import org.apache.doris.nereids.trees.plans.logical.LogicalJoin;
import org.apache.doris.nereids.trees.plans.logical.LogicalLimit;
import org.apache.doris.nereids.trees.plans.logical.LogicalOlapScan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.trees.plans.logical.LogicalRepeat;
import org.apache.doris.nereids.trees.plans.logical.LogicalTopN;
import org.apache.doris.nereids.trees.plans.logical.LogicalUnion;
import org.apache.doris.nereids.trees.plans.logical.LogicalWindow;
import org.apache.doris.nereids.trees.plans.physical.PhysicalHashAggregate;
import org.apache.doris.nereids.trees.plans.physical.PhysicalStorageLayerAggregate;
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
 * (that suite pins what is stored in the internal table, this one pins every shape the traversal
 * can render).
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
 *   MAVEN_OPTS="-Dmaven.repo.local=$PWD/../.dsh-m2/repository" \
 *     mvn -o -pl fe-core -am test -Dtest='GroupStructInfoGoldenTest' -DfailIfNoTests=false \
 *     -Dskip.doc=true -Dhbo.golden.dump=true
 *   # and then, to prove the corpus asserts clean:
 *   MAVEN_OPTS="-Dmaven.repo.local=$PWD/../.dsh-m2/repository" \
 *     mvn -o -pl fe-core -am test -Dtest='GroupStructInfoGoldenTest' -DfailIfNoTests=false \
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
            Files.write(file, (generated + "\n").getBytes(StandardCharsets.UTF_8));
            System.out.println("[hbo-golden] wrote " + file.toAbsolutePath());
            return;
        }
        assertSameLines(readResourceLines(GOLDEN_FILE_NAME), generated);
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
        Assertions.assertTrue(windowCanonical.contains("W{RowNumber;part:[col("), windowCanonical);
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
        Set<String> knownSkips = ImmutableSet.of(STORAGE_LAYER_SKIP, PARTIAL_STAGE_SKIP, PARTITION_TOPN_SKIP);
        List<String> skips = new ArrayList<>();
        String caseName = "<no case>";
        ClusterNode node = null;
        int cases = 0;
        int recorded = 0;
        for (String line : lines) {
            if (line.startsWith("# node ")) {
                recorded += node == null ? 0 : node.check(caseName, raw);
                node = new ClusterNode(line);
                continue;
            }
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("[CLUSTER ")) {
                recorded += node == null ? 0 : node.check(caseName, raw);
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
                Assertions.assertTrue(raw.contains(skip),
                        caseName + " does not carry its skip reason as-is: " + skip);
                skips.add(skip);
                continue;
            }
            Assertions.assertNotNull(node, caseName + " records a struct before any node header: " + line);
            node.add(line, caseName);
        }
        recorded += node == null ? 0 : node.check(caseName, raw);
        Assertions.assertTrue(cases >= 15, "the cluster corpus holds only " + cases + " cases");
        Assertions.assertTrue(recorded >= 15, "the cluster corpus records only " + recorded + " entries");
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
        private final String header;
        private final Map<String, String> structs = new LinkedHashMap<>();
        private final Map<String, String> fingerprints = new LinkedHashMap<>();

        ClusterNode(String header) {
            this.header = header;
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
                    Assertions.assertEquals(noLiteral, GroupStructInfo.toNoLiteral(withLiteral),
                            caseName + ": toNoLiteral did not fold the literal carrying struct into the"
                                    + " constant agnostic one recorded next to it");
                    Assertions.assertEquals(GroupStructInfo.stripScanBaseline(withLiteral),
                            GroupStructInfo.stripScanBaseline(noLiteral),
                            caseName + ": the identity of the scans depends on the literal mode");
                }
                if (noLiteral != null) {
                    Assertions.assertEquals(noLiteral, GroupStructInfo.toNoLiteral(noLiteral),
                            caseName + ": the constant agnostic struct is not a fixed point of the fold");
                }
            }
            for (String struct : structs.values()) {
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
        sb.append("# HBO struct info golden corpus - PLAN and TEXT sections (GENERATED).\n");
        sb.append("#\n");
        sb.append("# GENERATED FILE - DO NOT EDIT BY HAND. Every line below is produced by\n");
        sb.append("# GroupStructInfoGoldenTest from the current implementation; regenerate it with:\n");
        sb.append("#\n");
        sb.append("#   cd fe\n");
        sb.append("#   MAVEN_OPTS=\"-Dmaven.repo.local=$PWD/../.dsh-m2/repository\" mvn -o -pl fe-core -am test \\\n");
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
                .append(" (both sections are sorted by case name)").append('\n');
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
        cases.add(new PlanCase("filter_slot_equal_slot",
                "filter comparing two slots of one scan (both literal modes coincide)",
                GroupStructInfoGoldenTest::filterSlotEqualSlot));
        cases.add(new PlanCase("project_above_scan", "project over a scan (transparent)",
                GroupStructInfoGoldenTest::projectAboveScan));
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
        cases.add(new PlanCase("join_left_outer",
                "left outer join of two scans (the join type and the memo order are kept)",
                GroupStructInfoGoldenTest::joinLeftOuter));
        cases.add(new PlanCase("limit", "limit 5 offset 0 over a scan (LimitPhase.ORIGIN)",
                GroupStructInfoGoldenTest::limit));
        cases.add(new PlanCase("topn", "top-n of 3 rows ordered by one key, asc nulls first",
                GroupStructInfoGoldenTest::topn));
        cases.add(new PlanCase("aggregate_group_by_keys", "aggregation with one group by key over a scan",
                GroupStructInfoGoldenTest::aggregateGroupByKeys));
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
        cases.add(new PlanCase("window_under_a_filter",
                "filter over a window partitioned by one column (no order by)",
                GroupStructInfoGoldenTest::windowUnderAFilter));
        cases.add(new PlanCase("generate_unnest", "generate of unnest(1) over a scan",
                GroupStructInfoGoldenTest::generateUnnest));
        cases.add(new PlanCase("generate_explode_split",
                "generate of explode_split(cast(a as varchar), ',') over a scan: the delimiter is identity",
                GroupStructInfoGoldenTest::generateExplodeSplit));
        cases.add(new PlanCase("repeat_grouping_sets", "repeat of the grouping sets ((a), ()) over a scan",
                GroupStructInfoGoldenTest::repeatGroupingSets));
        cases.add(new PlanCase("invalid_too_many_scans",
                "cross join chain of 21 scans: more than hbo_max_scans_per_group",
                GroupStructInfoGoldenTest::invalidTooManyScans));
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

    private static GroupStructInfo filterSlotEqualSlot() {
        LogicalOlapScan t1 = scan(0, "t1");
        return describe(new LogicalFilter<>(ImmutableSet.of(
                new EqualTo(t1.getOutput().get(0), t1.getOutput().get(1))), t1));
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

    private static GroupStructInfo generateExplodeSplit() {
        LogicalOlapScan t1 = scan(0, "t1");
        SlotReference generatorOutput = new SlotReference("x", VarcharType.SYSTEM_DEFAULT);
        Expression argument = new Cast(t1.getOutput().get(0), VarcharType.SYSTEM_DEFAULT);
        return describe(new LogicalGenerate<>(
                ImmutableList.of(new ExplodeSplit(argument, new VarcharLiteral(","))),
                ImmutableList.of(generatorOutput), t1));
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
