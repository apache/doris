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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.FunctionSet;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Reference;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;
import org.apache.doris.thrift.TInPredicate;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Class representing a [NOT] IN predicate. It determines if a specified value
 * (first child) matches any value in a subquery (second child) or a list
 * of values (remaining children).
 */
public class InPredicate extends Predicate {
    private static final Logger LOG = LogManager.getLogger(InPredicate.class);

    private static final String IN_SET_LOOKUP = "in_set_lookup";
    private static final String NOT_IN_SET_LOOKUP = "not_in_set_lookup";
    private static final String IN_ITERATE= "in_iterate";
    private static final String NOT_IN_ITERATE = "not_in_iterate";
    private final boolean isNotIn;
    private static final String IN = "in";
    private static final String NOT_IN = "not_in";

    private static final NullLiteral NULL_LITERAL = new NullLiteral();

    public static void initBuiltins(FunctionSet functionSet) {
        for (Type t: Type.getSupportedTypes()) {
            if (t.isNull()) continue;
            // TODO we do not support codegen for CHAR and the In predicate must be codegened
            // because it has variable number of arguments. This will force CHARs to be
            // cast up to strings; meaning that "in" comparisons will not have CHAR comparison
            // semantics.
            if (t.getPrimitiveType() == PrimitiveType.CHAR) continue;

            String typeString = Function.getUdfTypeName(t.getPrimitiveType());

            functionSet.addBuiltin(ScalarFunction.createBuiltin(IN_ITERATE,
                    Lists.newArrayList(t, t), true, Type.BOOLEAN,
                    "doris::InPredicate::in_iterate", null, null, false));
            functionSet.addBuiltin(ScalarFunction.createBuiltin(NOT_IN_ITERATE,
                    Lists.newArrayList(t, t), true, Type.BOOLEAN,
                    "doris::InPredicate::not_in_iterate", null, null, false));

            String prepareFn = "doris::InPredicate::set_lookup_prepare_" + typeString;
            String closeFn = "doris::InPredicate::set_lookup_close_" + typeString;
            functionSet.addBuiltin(ScalarFunction.createBuiltin(IN_SET_LOOKUP,
                    Lists.newArrayList(t, t), true, Type.BOOLEAN,
                    "doris::InPredicate::in_set_lookup", prepareFn, closeFn, false));
            functionSet.addBuiltin(ScalarFunction.createBuiltin(NOT_IN_SET_LOOKUP,
                    Lists.newArrayList(t, t), true, Type.BOOLEAN,
                    "doris::InPredicate::not_in_set_lookup", prepareFn, closeFn, false));

        }
    }

    // First child is the comparison expr for which we
    // should check membership in the inList (the remaining children).
    public InPredicate(Expr compareExpr, List<Expr> inList, boolean isNotIn) {
        children.add(compareExpr);
        children.addAll(inList);
        this.isNotIn = isNotIn;
    }

    protected InPredicate(InPredicate other) {
        super(other);
        isNotIn = other.isNotIn();
    }

    public int getInElementNum() {
        // the first child is compare expr
        return getChildren().size() - 1;
    }

    @Override
    public InPredicate clone() {
        return new InPredicate(this);
    }

    // C'tor for initializing an [NOT] IN predicate with a subquery child.
    public InPredicate(Expr compareExpr, Expr subquery, boolean isNotIn) {
        Preconditions.checkNotNull(compareExpr);
        Preconditions.checkNotNull(subquery);
        children.add(compareExpr);
        children.add(subquery);
        this.isNotIn = isNotIn;
    }

    /**
     * Negates an InPredicate.
     */
    @Override
    public Expr negate() {
      return new InPredicate(getChild(0), children.subList(1, children.size()),
          !isNotIn);
    }

    public List<Expr> getListChildren() {
        return  children.subList(1, children.size());
    }

    public boolean isNotIn() {
        return isNotIn;
    }

    public boolean isLiteralChildren() {
        for (int i = 1; i < children.size(); ++i) {
            if (!(children.get(i) instanceof LiteralExpr)) {
                return false;
            }
        }
        return true;
    }

   @Override
   public void vectorizedAnalyze(Analyzer analyzer) {
        super.vectorizedAnalyze(analyzer);

       PrimitiveType type = getChild(0).getType().getPrimitiveType();

//       OpcodeRegistry.BuiltinFunction match = OpcodeRegistry.instance().getFunctionInfo(
//               FunctionOperator.FILTER_IN, true, true, type);
//       Preconditions.checkState(match != null);
//       Preconditions.checkState(match.getReturnType().equals(Type.BOOLEAN));
//       this.vectorOpcode = match.opcode;
//       LOG.info(debugString() + " opcode: " + vectorOpcode);
   }

    @Override
    public void analyzeImpl(Analyzer analyzer) throws AnalysisException {
        super.analyzeImpl(analyzer);
        
        if (contains(Subquery.class)) {
            // An [NOT] IN predicate with a subquery must contain two children, the second of
            // which is a Subquery.
            if (children.size() != 2 || !(getChild(1) instanceof Subquery)) {
                throw new AnalysisException("Unsupported IN predicate with a subquery: " +
                    toSql());
            } 
            Subquery subquery = (Subquery)getChild(1);
            if (!subquery.returnsScalarColumn()) {
                throw new AnalysisException("Subquery must return a single column: " +
                subquery.toSql());
            }

            // Ensure that the column in the lhs of the IN predicate and the result of
            // the subquery are type compatible. No need to perform any
            // casting at this point. Any casting needed will be performed when the
            // subquery is unnested.
            ArrayList<Expr> subqueryExprs = subquery.getStatement().getResultExprs();
            Expr compareExpr = children.get(0);
            Expr subqueryExpr = subqueryExprs.get(0);
            analyzer.getCompatibleType(compareExpr.getType(), compareExpr, subqueryExpr);
        } else {
            analyzer.castAllToCompatibleType(children);
            vectorizedAnalyze(analyzer);
        }

        boolean allConstant = true;
        for (int i = 1; i < children.size(); ++i) {
            if (!children.get(i).isConstant()) {
                allConstant = false;
                break;
            }
        }
        boolean useSetLookup = allConstant;
        // Only lookup fn_ if all subqueries have been rewritten. If the second child is a
        // subquery, it will have type ArrayType, which cannot be resolved to a builtin
        // function and will fail analysis.
        Type[] argTypes = {getChild(0).type, getChild(1).type};
        if (useSetLookup) {
            // fn = getBuiltinFunction(analyzer, isNotIn ? NOT_IN_SET_LOOKUP : IN_SET_LOOKUP,
            // argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            opcode = isNotIn ? TExprOpcode.FILTER_NOT_IN : TExprOpcode.FILTER_IN;
        } else {
            fn = getBuiltinFunction(analyzer, isNotIn ? NOT_IN_ITERATE : IN_ITERATE,
                    argTypes, Function.CompareMode.IS_NONSTRICT_SUPERTYPE_OF);
            opcode = isNotIn ? TExprOpcode.FILTER_NEW_NOT_IN : TExprOpcode.FILTER_NEW_IN;
        }

        Reference<SlotRef> slotRefRef = new Reference<SlotRef>();
        Reference<Integer> idxRef = new Reference<Integer>();
        if (isSingleColumnPredicate(slotRefRef, idxRef)
                && idxRef.getRef() == 0 && slotRefRef.getRef().getNumDistinctValues() > 0) {
            selectivity = (double) (getChildren().size() - 1) / (double) slotRefRef.getRef()
                    .getNumDistinctValues();
            selectivity = Math.max(0.0, Math.min(1.0, selectivity));
        } else {
            selectivity = Expr.DEFAULT_SELECTIVITY;
        }
    }

    public InPredicate union(InPredicate inPredicate) {
        Preconditions.checkState(inPredicate.isLiteralChildren());
        Preconditions.checkState(this.isLiteralChildren());
        Preconditions.checkState(getChild(0).equals(inPredicate.getChild(0)));
        List<Expr> unionChildren = new ArrayList<>(getListChildren());
        unionChildren.removeAll(inPredicate.getListChildren());
        unionChildren.addAll(inPredicate.getListChildren());
        InPredicate union = new InPredicate(getChild(0), unionChildren, isNotIn);
        return union;
    }

    public InPredicate intersection(InPredicate inPredicate) {
        Preconditions.checkState(inPredicate.isLiteralChildren());
        Preconditions.checkState(this.isLiteralChildren());
        Preconditions.checkState(getChild(0).equals(inPredicate.getChild(0)));
        List<Expr> intersectChildren = new ArrayList<>(getListChildren());
        intersectChildren.retainAll(inPredicate.getListChildren());
        InPredicate intersection = new InPredicate(getChild(0), intersectChildren, isNotIn);
        return intersection;
    }

    @Override
    protected void toThrift(TExprNode msg) {
        // Can't serialize a predicate with a subquery
        Preconditions.checkState(!contains(Subquery.class));
        msg.in_predicate = new TInPredicate(isNotIn);
        msg.node_type = TExprNodeType.IN_PRED;
        msg.setOpcode(opcode);
        msg.setVectorOpcode(vectorOpcode);
    }

    @Override
    public String toSqlImpl() {
        StringBuilder strBuilder = new StringBuilder();
        String notStr = (isNotIn) ? "NOT " : "";
        strBuilder.append(getChild(0).toSql() + " " + notStr + "IN (");
        for (int i = 1; i < children.size(); ++i) {
            strBuilder.append(getChild(i).toSql());
            strBuilder.append((i + 1 != children.size()) ? ", " : "");
        }
        strBuilder.append(")");
        return strBuilder.toString();
    }

    @Override
    public String toString() {
        return toSql();
    }

    @Override
    public Expr getResultValue() throws AnalysisException {
        recursiveResetChildrenResult();
        final Expr leftChildValue = getChild(0);
        if (!(leftChildValue instanceof LiteralExpr) || !isLiteralChildren()) {
            return this;
        }

        if (leftChildValue instanceof NullLiteral) {
            return leftChildValue;
        }

        List<Expr> inListChildren = children.subList(1, children.size());
        boolean containsLeftChild = inListChildren.contains(leftChildValue);

        // See QueryPlanTest.java testConstantInPredicate() for examples.
        // This logic should be same as logic in in_predicate.cpp: get_boolean_val()
        if (containsLeftChild) {
            return new BoolLiteral(!isNotIn);
        }
        if (inListChildren.contains(NULL_LITERAL)) {
            return new NullLiteral();
        }
        return new BoolLiteral(isNotIn);
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            InPredicate expr = (InPredicate) obj;
            if (isNotIn == expr.isNotIn) {
                return true;
            }
        }
        return false;
    }
}
