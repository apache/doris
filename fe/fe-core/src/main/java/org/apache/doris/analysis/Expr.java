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
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.TableIf.TableType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.FormatOptions;
import org.apache.doris.common.TreeNode;
import org.apache.doris.nereids.util.Utils;
import org.apache.doris.planner.normalize.Normalizer;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprOpcode;

import com.google.common.base.Joiner;
import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.Lists;
import com.google.gson.annotations.SerializedName;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Root of the expr node hierarchy.
 */
public abstract class Expr extends TreeNode<Expr> implements Cloneable {

    public static final String DEFAULT_EXPR_NAME = "expr";

    protected boolean disableTableName = false;

    protected boolean nullable = false;

    @SerializedName("type")
    protected Type type;  // result of analysis

    @SerializedName("opcode")
    protected TExprOpcode opcode;  // opcode for this expr

    // The function to call. This can either be a scalar or aggregate function.
    // Set in analyze().
    protected Function fn;

    // Cached value of IsConstant(), set during analyze() and valid if isAnalyzed_ is true.
    private Supplier<Boolean> isConstant = Suppliers.memoize(this::isConstantImpl);

    protected Optional<String> exprName = Optional.empty();

    protected Expr() {
        super();
        type = Type.INVALID;
        opcode = TExprOpcode.INVALID_OPCODE;
    }

    protected Expr(Expr other) {
        super();
        type = other.type;
        opcode = other.opcode;
        isConstant = other.isConstant;
        fn = other.fn;
        children = Expr.cloneList(other.children);
        nullable = other.nullable;
    }

    public void checkValueValid() throws AnalysisException {
    }

    // Name of expr, this is used by generating column name automatically when there is no
    // alias or is not slotRef
    public String getExprName() {
        if (!this.exprName.isPresent()) {
            this.exprName = Optional.of(Utils.normalizeName(this.getClass().getSimpleName(), DEFAULT_EXPR_NAME));
        }
        return this.exprName.get();
    }

    public Type getType() {
        return type;
    }

    // add by cmy. for restoring
    public void setType(Type type) {
        this.type = type;
    }

    public TExprOpcode getOpcode() {
        return opcode;
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

    public static List<TExpr> treesToThrift(List<? extends Expr> exprs) {
        List<TExpr> result = Lists.newArrayList();
        for (Expr expr : exprs) {
            result.add(expr.treeToThrift());
        }
        return result;
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

    public String toSql() {
        if (disableTableName) {
            return toSqlWithoutTbl();
        }
        return toSqlImpl();
    }

    public String toSql(boolean disableTableName, boolean needExternalSql, TableType tableType, TableIf table) {
        return toSqlImpl(disableTableName, needExternalSql, tableType, table);
    }

    public void disableTableName() {
        disableTableName = true;
        for (Expr child : children) {
            child.disableTableName();
        }
    }

    public String toSqlWithoutTbl() {
        return toSql(true, false, null, null);
    }

    /**
     * Returns a SQL string representing this expr. Subclasses should override this method
     * instead of toSql() to ensure that parenthesis are properly added around the toSql().
     */
    protected abstract String toSqlImpl();

    protected abstract String toSqlImpl(boolean disableTableName, boolean needExternalSql, TableType tableType,
            TableIf table);

    public String toExternalSql(TableType tableType, TableIf table) {
        return toSql(false, true, tableType, table);
    }

    /**
     * Return a column label for the expression
     */
    public String toColumnLabel() {
        return toSql();
    }

    // Convert this expr, including all children, to its Thrift representation.
    public TExpr treeToThrift() {
        TExpr result = new TExpr();
        treeToThriftHelper(result);
        return result;
    }

    protected void treeToThriftHelper(TExpr container) {
        treeToThriftHelper(container, ((expr, exprNode) -> expr.toThrift(exprNode)));
    }

    // Append a flattened version of this expr, including all children, to 'container'.
    protected void treeToThriftHelper(TExpr container, ExprVisitor visitor) {
        TExprNode msg = new TExprNode();
        msg.type = type.toThrift();
        msg.num_children = children.size();
        if (fn != null) {
            msg.setFn(fn.toThrift(type, collectChildReturnTypes(), collectChildReturnNullables()));
            if (fn.hasVarArgs()) {
                msg.setVarargStartIdx(fn.getNumArgs() - 1);
            }
        }
        // useless parameter, just give a number
        msg.output_scale = -1;
        msg.setIsNullable(nullable);
        visitor.visit(this, msg);
        container.addToNodes(msg);
        for (Expr child : children) {
            child.treeToThriftHelper(container, visitor);
        }
    }

    public interface ExprVisitor {
        void visit(Expr expr, TExprNode exprNode);
    }

    // Convert this expr into msg (excluding children), which requires setting
    // msg.op as well as the expr-specific field.
    protected abstract void toThrift(TExprNode msg);

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
        int result = 31 * Objects.hashCode(type) + Objects.hashCode(opcode);
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

    public Map<Long, Set<String>> getTableIdToColumnNames() {
        Map<Long, Set<String>> tableIdToColumnNames = new HashMap<Long, Set<String>>();
        getTableIdToColumnNames(tableIdToColumnNames);
        return tableIdToColumnNames;
    }

    public void getTableIdToColumnNames(Map<Long, Set<String>> tableIdToColumnNames) {
        Preconditions.checkState(tableIdToColumnNames != null);
        for (Expr child : children) {
            child.getTableIdToColumnNames(tableIdToColumnNames);
        }
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
     * This method is used for constant fold of query in FE,
     * for different serde dialect(hive, presto, doris).
     */
    public String getStringValueForQuery(FormatOptions options) {
        return getStringValue();
    }

    /**
     * This method is to return the string value of this expr in a complex type for query
     * It is only used for "getStringValueForQuery()"
     * For most of the integer types, it is same as getStringValueForQuery().
     * But for others like StringLiteral and DateLiteral, it should be wrapped with quotations.
     * eg: 1,2,abc,[1,2,3],["abc","def"],{10:20},{"abc":20}
     */
    protected String getStringValueInComplexTypeForQuery(FormatOptions options) {
        return getStringValueForQuery(options);
    }

    /**
     * This method is to return the string value of this expr for stream load.
     * so there is a little different from "getStringValueForQuery()".
     * eg, for NullLiteral, it should be "\N" for stream load, but "null" for FE constant
     * for StructLiteral, the value should not contain sub column's name.
     */
    public String getStringValueForStreamLoad(FormatOptions options) {
        return getStringValueForQuery(options);
    }

    public final TExpr normalize(Normalizer normalizer) {
        TExpr result = new TExpr();
        treeToThriftHelper(result, (expr, texprNode) -> expr.normalize(texprNode, normalizer));
        return result;
    }

    protected void normalize(TExprNode msg, Normalizer normalizer) {
        this.toThrift(msg);
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

