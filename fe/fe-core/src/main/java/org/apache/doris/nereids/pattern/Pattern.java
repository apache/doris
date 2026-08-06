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

package org.apache.doris.nereids.pattern;

import org.apache.doris.nereids.trees.AbstractTreeNode;
import org.apache.doris.nereids.trees.plans.Plan;
import org.apache.doris.nereids.trees.plans.PlanType;

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

/**
 * Pattern node used in pattern matching.
 */
public class Pattern<TYPE extends Plan>
        extends AbstractTreeNode<Pattern<? extends Plan>> {

    public static final Pattern ANY = new Pattern(PatternType.ANY);
    public static final Pattern MULTI = new Pattern(PatternType.MULTI);
    public static final Pattern GROUP = new Pattern(PatternType.GROUP);
    public static final Pattern MULTI_GROUP = new Pattern(PatternType.MULTI_GROUP);

    protected final List<Predicate<TYPE>> predicates;
    protected final PatternType patternType;
    protected final PlanType planType;

    public Pattern(PlanType planType, Pattern... children) {
        this(PatternType.NORMAL, planType, children);
    }

    public Pattern(PlanType planType, List<Predicate<TYPE>> predicates, Pattern... children) {
        this(PatternType.NORMAL, planType, predicates, children);
    }

    private Pattern(PatternType patternType, Pattern... children) {
        this(patternType, PlanType.UNKNOWN, children);
    }

    /**
     * Constructor for Pattern.
     *
     * @param patternType pattern type to matching
     * @param children sub pattern
     */
    private Pattern(PatternType patternType, PlanType planType, Pattern... children) {
        super(children);
        this.patternType = patternType;
        this.planType = planType;
        this.predicates = ImmutableList.of();
    }

    /**
     * Constructor for Pattern.
     *
     * @param patternType pattern type to matching
     * @param planType plan type to matching
     * @param predicates custom matching predicate
     * @param children sub pattern
     */
    protected Pattern(PatternType patternType, PlanType planType,
                   List<Predicate<TYPE>> predicates, Pattern... children) {
        super(children);
        this.patternType = patternType;
        this.planType = planType;
        this.predicates = ImmutableList.copyOf(predicates);

        for (int i = 0; i + 1 < children.length; ++i) {
            if (children[i].isMulti()) {
                throw new IllegalStateException("Pattern.MULTI must be last child of current pattern");
            } else if (children[i].isMultiGroup()) {
                throw new IllegalStateException("Pattern.MULTI_GROUP must be last child of current pattern");
            }
        }
    }

    /**
     * get current type in Plan.
     *
     * @return plan type in pattern
     */
    public PlanType getPlanType() {
        return planType;
    }

    /**
     * get current type in Pattern.
     *
     * @return pattern type
     */
    public PatternType getPatternType() {
        return patternType;
    }

    /**
     * get all predicates in Pattern.
     *
     * @return all predicates
     */
    public List<Predicate<TYPE>> getPredicates() {
        return predicates;
    }

    public boolean isGroup() {
        return patternType == PatternType.GROUP;
    }

    public boolean isMultiGroup() {
        return patternType == PatternType.MULTI_GROUP;
    }

    public boolean isAny() {
        return patternType == PatternType.ANY;
    }

    public boolean isMulti() {
        return patternType == PatternType.MULTI;
    }

    /** matchPlan */
    public boolean matchPlanTree(Plan plan) {
        if (!matchRoot(plan)) {
            return false;
        }
        int childPatternNum = arity();
        if (childPatternNum != plan.arity() && childPatternNum > 0 && child(childPatternNum - 1) != MULTI) {
            return false;
        }
        switch (patternType) {
            case ANY:
            case MULTI:
                return matchPredicates((TYPE) plan);
            default:
        }
        if (this instanceof SubTreePattern) {
            return matchPredicates((TYPE) plan);
        }
        return matchChildrenAndSelfPredicates(plan, childPatternNum);
    }

    private boolean matchChildrenAndSelfPredicates(Plan plan, int childPatternNum) {
        List<Plan> childrenPlan = plan.children();
        for (int i = 0; i < childrenPlan.size(); i++) {
            Plan child = childrenPlan.get(i);
            Pattern childPattern = child(Math.min(i, childPatternNum - 1));
            if (!childPattern.matchPlanTree(child)) {
                return false;
            }
        }
        return matchPredicates((TYPE) plan);
    }

    /**
     * Return ture if current Pattern match Plan in params.
     *
     * @param plan wait to match
     * @return ture if current Pattern match Plan in params
     */
    public boolean matchRoot(Plan plan) {
        if (plan == null) {
            return false;
        }
        switch (patternType) {
            case ANY:
            case MULTI:
            case GROUP:
            case MULTI_GROUP:
                return true;
            default:
                return planType == plan.getType();
        }
    }

    /**
     * match all predicates.
     * @param root root plan
     * @return true if all predicates matched
     */
    public boolean matchPredicates(TYPE root) {
        // use loop to speed up
        for (Predicate<TYPE> predicate : predicates) {
            if (!predicate.test(root)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Diagnostic version of matchPlanTree. Returns null if match succeeds,
     * or a human-readable failure reason string if match fails.
     *
     * @param plan the plan to match against
     * @param path the current path in the tree (e.g. "root/child[0]/child[1]")
     * @return null if matched, or a diagnostic message describing where and why the match failed
     */
    public String matchPlanTreeDiagnostic(Plan plan, String path) {
        if (!matchRoot(plan)) {
            return path + ": expected node type " + planType + " but got " + plan.getType();
        }
        int childPatternNum = arity();
        if (childPatternNum != plan.arity() && childPatternNum > 0 && child(childPatternNum - 1) != MULTI) {
            return path + ": expected " + childPatternNum + " children but got " + plan.arity()
                    + " on node " + plan.getType();
        }
        switch (patternType) {
            case ANY:
            case MULTI:
                return matchPredicatesDiagnostic((TYPE) plan, path);
            default:
        }
        if (this instanceof SubTreePattern) {
            return matchPredicatesDiagnostic((TYPE) plan, path);
        }
        return matchChildrenAndSelfPredicatesDiagnostic(plan, childPatternNum, path);
    }

    private String matchChildrenAndSelfPredicatesDiagnostic(Plan plan, int childPatternNum, String path) {
        List<Plan> childrenPlan = plan.children();
        for (int i = 0; i < childrenPlan.size(); i++) {
            Plan child = childrenPlan.get(i);
            Pattern childPattern = child(Math.min(i, childPatternNum - 1));
            String childPath = path + "/" + plan.getType() + ".child[" + i + "]";
            String childResult = childPattern.matchPlanTreeDiagnostic(child, childPath);
            if (childResult != null) {
                return childResult;
            }
        }
        return matchPredicatesDiagnostic((TYPE) plan, path);
    }

    /**
     * Diagnostic version of matchPredicates. Returns null if all predicates pass,
     * or a message describing which predicate failed.
     */
    public String matchPredicatesDiagnostic(TYPE root, String path) {
        for (int i = 0; i < predicates.size(); i++) {
            Predicate<TYPE> predicate = predicates.get(i);
            try {
                if (!predicate.test(root)) {
                    String predicateDesc = (predicate instanceof DescribedPredicate)
                            ? ((DescribedPredicate<TYPE>) predicate).getDescription()
                            : "predicate #" + (i + 1);
                    return path + " (" + root.getType() + "): " + predicateDesc
                            + " failed. [predicate " + (i + 1) + "/" + predicates.size() + "]";
                }
            } catch (Throwable t) {
                String predicateDesc = (predicate instanceof DescribedPredicate)
                        ? ((DescribedPredicate<TYPE>) predicate).getDescription()
                        : "predicate #" + (i + 1);
                return path + " (" + root.getType() + "): " + predicateDesc
                        + " threw " + t.getClass().getSimpleName() + ": " + t.getMessage()
                        + " [predicate " + (i + 1) + "/" + predicates.size() + "]";
            }
        }
        return null;
    }

    @Override
    public Pattern<? extends Plan> withChildren(
            List<Pattern<? extends Plan>> children) {
        throw new IllegalStateException("Pattern can not invoke withChildren");
    }

    public Pattern<TYPE> withPredicates(List<Predicate<TYPE>> predicates) {
        return new Pattern(patternType, planType, predicates, children.toArray(new Pattern[0]));
    }

    public boolean hasMultiChild() {
        return !children.isEmpty() && children.get(children.size() - 1).isMulti();
    }

    public boolean hasMultiGroupChild() {
        return !children.isEmpty() && children.get(children.size() - 1).isMultiGroup();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Pattern<?> pattern = (Pattern<?>) o;
        return predicates.equals(pattern.predicates)
                && patternType == pattern.patternType
                && planType == pattern.planType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(predicates, patternType, planType);
    }

    // for TypePattern
    public Class<TYPE> getMatchedType() {
        return null;
    }
}
