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
// This file is copied from
// https://github.com/apache/impala/blob/branch-2.9.0/fe/src/main/java/org/apache/impala/Expr.java
// and modified by Doris

package org.apache.doris.analysis;

import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.TreeNode;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Root of the expr node hierarchy.
 */
public abstract class Expr extends TreeNode<Expr> implements Cloneable {

    public static final String DEFAULT_EXPR_NAME = "expr";

    protected boolean nullable = false;

    @SerializedName("type")
    protected Type type;  // result of analysis

    // The function to call. This can either be a scalar or aggregate function.
    // Set in analyze().
    protected Function fn;

    // Cached value of IsConstant(), set during analyze() and valid if isAnalyzed_ is true.
    private Supplier<Boolean> isConstant = Suppliers.memoize(this::isConstantImpl);

    protected Expr() {
        super();
        type = Type.INVALID;
    }

    protected Expr(Expr other) {
        super();
        type = other.type;
        isConstant = other.isConstant;
        fn = other.fn;
        children = Expr.cloneList(other.children);
        nullable = other.nullable;
    }

    public void checkValueValid() throws AnalysisException {
    }

    public Type getType() {
        return type;
    }

    // add by cmy. for restoring
    public void setType(Type type) {
        this.type = type;
    }

    public Function getFn() {
        return fn;
    }

    /**
     * Collects the returns types of the child nodes in an array.
     */
    protected Type[] collectChildReturnTypes() {
        Type[] childTypes = new Type[children.size()];
        for (int i = 0; i < children.size(); ++i) {
            childTypes[i] = children.get(i).type;
        }
        return childTypes;
    }

    protected Boolean[] collectChildReturnNullables() {
        Boolean[] childNullables = new Boolean[children.size()];
        for (int i = 0; i < children.size(); ++i) {
            childNullables[i] = children.get(i).isNullable();
        }
        return childNullables;
    }

    public Expr getChildWithoutCast(int i) {
        Preconditions.checkArgument(i < children.size(), "child index {0} out of range {1}", i, children.size());
        Expr child = children.get(i);
        return child instanceof CastExpr ? child.children.get(0) : child;
    }

    public static String debugString(List<? extends Expr> exprs) {
        if (exprs == null || exprs.isEmpty()) {
            return "";
        }
        List<String> strings = Lists.newArrayList();
        for (Expr expr : exprs) {
            strings.add(Strings.nullToEmpty(expr.debugString()));
        }
        return "(" + Joiner.on(" ").join(strings) + ")";
    }

    /**
     * Create a deep copy of 'l'. If sMap is non-null, use it to substitute the
     * elements of l.
     */
    public static <C extends Expr> ArrayList<C> cloneList(List<C> l, ExprSubstitutionMap sMap) {
        Preconditions.checkNotNull(l);
        ArrayList<C> result = new ArrayList<C>();
        for (C element : l) {
            result.add((C) element.clone(sMap));
        }
        return result;
    }

    /**
     * Create a deep copy of 'l'. If sMap is non-null, use it to substitute the
     * elements of l.
     */
    public static <C extends Expr> ArrayList<C> cloneList(List<C> l) {
        Preconditions.checkNotNull(l);
        ArrayList<C> result = new ArrayList<C>();
        for (C element : l) {
            result.add((C) element.clone());
        }
        return result;
    }

    /**
     * Collect all unique Expr nodes of type 'cl' present in 'input' and add them to
     * 'output' if they do not exist in 'output'.
     * This can't go into TreeNode<>, because we'd be using the template param
     * NodeType.
     */
    public static <C extends Expr> void collectList(List<? extends Expr> input, Class<C> cl, List<C> output) {
        Preconditions.checkNotNull(input);
        for (Expr e : input) {
            e.collect(cl, output);
        }
    }

    public static void extractSlots(Expr root, Set<SlotId> slotIdSet) {
        if (root instanceof SlotRef) {
            slotIdSet.add(((SlotRef) root).getDesc().getId());
            return;
        }
        for (Expr child : root.getChildren()) {
            extractSlots(child, slotIdSet);
        }
    }

    /**
     * Accept a visitor and dispatch to the appropriate typed {@code visitXxx} method.
     * Each concrete subclass must override this to call the correct visitor method.
     */
    public abstract <R, C> R accept(ExprVisitor<R, C> visitor, C context);

    public String debugString() {
        return debugString(children);
    }

    /**
     * Creates a deep copy of this expr including its analysis state. The method is
     * abstract in this class to force new Exprs to implement it.
     */
    @Override
    public abstract Expr clone();

    public boolean notCheckDescIdEquals(Object obj) {
        return equals(obj);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != this.getClass()) {
            return false;
        }
        // don't compare type, this could be called pre-analysis
        Expr expr = (Expr) obj;
        if (children.size() != expr.children.size()) {
            return false;
        }
        for (int i = 0; i < children.size(); ++i) {
            if (!children.get(i).equals(expr.children.get(i))) {
                return false;
            }
        }
        if (fn == null && expr.fn == null) {
            return true;
        }
        if (fn == null || expr.fn == null) {
            return false;
        }
        // Both fn_'s are not null
        return fn.equals(expr.fn);
    }

    @Override
    public int hashCode() {
        int result = 31 * Objects.hashCode(type) + getClass().hashCode();
        for (Expr child : children) {
            result = 31 * result + Objects.hashCode(child);
        }
        return result;
    }

    /**
     * Gather conjuncts from this expr and return them in a list.
     * A conjunct is an expr that returns a boolean, e.g., Predicates, function calls,
     * SlotRefs, etc. Hence, this method is placed here and not in Predicate.
     */
    public List<Expr> getConjuncts() {
        List<Expr> list = Lists.newArrayList();
        if (this instanceof CompoundPredicate
                && ((CompoundPredicate) this).getOp() == CompoundPredicate.Operator.AND) {
            // TODO: we have to convert CompoundPredicate.AND to two expr trees for
            // conjuncts because NULLs are handled differently for CompoundPredicate.AND
            // and conjunct evaluation.  This is not optimal for jitted exprs because it
            // will result in two functions instead of one. Create a new CompoundPredicate
            // Operator (i.e. CONJUNCT_AND) with the right NULL semantics and use that
            // instead
            list.addAll((getChild(0)).getConjuncts());
            list.addAll((getChild(1)).getConjuncts());
        } else {
            list.add(this);
        }
        return list;
    }

    /**
     * Create a deep copy of 'this'. If sMap is non-null,
     * use it to substitute 'this' or its subnodes.
     * <p/>
     * Expr subclasses that add non-value-type members must override this.
     */
    public Expr clone(ExprSubstitutionMap sMap) {
        if (sMap != null) {
            for (int i = 0; i < sMap.getLhs().size(); ++i) {
                if (this.equals(sMap.getLhs().get(i))) {
                    return sMap.getRhs().get(i).clone(null);
                }
            }
        }
        Expr result = (Expr) this.clone();
        result.children = Lists.newArrayList();
        for (Expr child : children) {
            result.children.add(((Expr) child).clone(sMap));
        }
        return result;
    }

    /**
     * Returns true if expr is fully bound by slotId, otherwise false.
     */
    public boolean isBound(SlotId slotId) {
        for (Expr child : children) {
            if (!child.isBound(slotId)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @return true if this is an instance of LiteralExpr
     */
    public boolean isLiteral() {
        return this instanceof LiteralExpr;
    }

    /**
     * Returns true if this expression should be treated as constant. I.e. if the frontend
     * and backend should assume that two evaluations of the expression within a query will
     * return the same value. Examples of constant expressions include:
     * - Literal values like 1, "foo", or NULL
     * - Deterministic operators applied to constant arguments, e.g. 1 + 2, or
     *   concat("foo", "bar")
     * - Functions that should be always return the same value within a query but may
     *   return different values for different queries. E.g. now(), which we want to
     *   evaluate only once during planning.
     * May incorrectly return true if the expression is not analyzed.
     * TODO: isAnalyzed_ should be a precondition for isConstant(), since it is not always
     * possible to correctly determine const-ness before analysis (e.g. see
     * FunctionCallExpr.isConstant()).
     */
    public final boolean isConstant() {
        return isConstant.get();
    }

    /**
     * Implements isConstant() - computes the value without using 'isConstant_'.
     */
    protected boolean isConstantImpl() {
        for (Expr expr : children) {
            if (!expr.isConstant()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this.getClass()).add("type", type).toString();
    }

    /**
     * If 'this' is a SlotRef or a Cast that wraps a SlotRef, returns that SlotRef.
     * Otherwise returns null.
     */
    public SlotRef unwrapSlotRef() {
        if (this instanceof SlotRef) {
            return (SlotRef) this;
        } else if (this instanceof CastExpr && getChild(0) instanceof SlotRef) {
            return (SlotRef) getChild(0);
        } else {
            return null;
        }
    }

    /**
     * Returns the first child if this Expr is a CastExpr. Otherwise, returns 'this'.
     */
    public Expr unwrapExpr(boolean implicitOnly) {
        if (this instanceof CastExpr
                && (!implicitOnly || ((CastExpr) this).isImplicit())) {
            return children.get(0);
        }
        return this;
    }

    public String getStringValue() {
        return "";
    }

    /**
     * For excute expr the result is nullable
     */
    public boolean isNullable() {
        return nullable;
    }

    public static AggStateType createAggStateType(String name, List<Type> typeList,
            List<Boolean> nullableList, boolean resultNullable) {
        return new AggStateType(name, resultNullable, typeList, nullableList);
    }

    // This is only for transactional insert operation,
    // to check it the given value in insert stmt is LiteralExpr.
    // And if we write "1" to a boolean column, there will be a cast(1 as boolean) expr,
    // which is also accepted.
    public boolean isLiteralOrCastExpr() {
        if (this instanceof CastExpr) {
            return children.get(0) instanceof LiteralExpr;
        } else {
            return this instanceof LiteralExpr;
        }
    }

    public boolean isNullLiteral() {
        return this instanceof NullLiteral;
    }

    public Set<SlotRef> getInputSlotRef() {
        Set<SlotRef> slots = new HashSet<>();
        if (this instanceof SlotRef) {
            slots.add((SlotRef) this);
            return slots;
        } else {
            for (Expr expr : children) {
                slots.addAll(expr.getInputSlotRef());
            }
        }
        return slots;
    }
}
