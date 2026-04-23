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

import org.apache.doris.analysis.ArithmeticExpr.Operator;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.FunctionToThriftConverter;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.thrift.TAggregateExpr;
import org.apache.doris.thrift.TBoolLiteral;
import org.apache.doris.thrift.TCaseExpr;
import org.apache.doris.thrift.TColumnRef;
import org.apache.doris.thrift.TDateLiteral;
import org.apache.doris.thrift.TDecimalLiteral;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TExprNode;
import org.apache.doris.thrift.TExprNodeType;
import org.apache.doris.thrift.TExprOpcode;
import org.apache.doris.thrift.TFloatLiteral;
import org.apache.doris.thrift.TIPv4Literal;
import org.apache.doris.thrift.TIPv6Literal;
import org.apache.doris.thrift.TInPredicate;
import org.apache.doris.thrift.TInfoFunc;
import org.apache.doris.thrift.TIntLiteral;
import org.apache.doris.thrift.TJsonLiteral;
import org.apache.doris.thrift.TLargeIntLiteral;
import org.apache.doris.thrift.TMatchPredicate;
import org.apache.doris.thrift.TSearchClause;
import org.apache.doris.thrift.TSearchFieldBinding;
import org.apache.doris.thrift.TSearchOccur;
import org.apache.doris.thrift.TSearchParam;
import org.apache.doris.thrift.TSlotRef;
import org.apache.doris.thrift.TStringLiteral;
import org.apache.doris.thrift.TTimeV2Literal;
import org.apache.doris.thrift.TTypeDesc;
import org.apache.doris.thrift.TTypeNode;
import org.apache.doris.thrift.TVarBinaryLiteral;

import com.google.common.collect.Lists;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Visitor that converts any {@link Expr} node into its Thrift {@link TExprNode}
 * representation. Replaces the per-subclass {@code toThrift(TExprNode)} methods.
 *
 * <p>Usage: {@code ExprToThriftVisitor.treeToThrift(expr)} or
 * {@code expr.accept(ExprToThriftVisitor.INSTANCE, texprNode)}.
 */
public class ExprToThriftVisitor extends ExprVisitor<Void, TExprNode> {
    private static final Logger LOG = LogManager.getLogger(ExprToThriftVisitor.class);

    public static final ExprToThriftVisitor INSTANCE = new ExprToThriftVisitor();

    protected ExprToThriftVisitor() {
    }

    // -----------------------------------------------------------------------
    // Static utility methods (moved from Expr.java)
    // -----------------------------------------------------------------------

    public static TExpr treeToThrift(Expr expr) {
        TExpr result = new TExpr();
        treeToThriftHelper(expr, result, INSTANCE);
        return result;
    }

    public static List<TExpr> treesToThrift(List<? extends Expr> exprs) {
        List<TExpr> result = Lists.newArrayList();
        for (Expr expr : exprs) {
            result.add(treeToThrift(expr));
        }
        return result;
    }

    /**
     * Append a flattened version of {@code expr} (including all children)
     * to {@code container}, using the given visitor for per-node conversion.
     */
    public static void treeToThriftHelper(Expr expr, TExpr container,
            ExprVisitor<Void, TExprNode> visitor) {
        // CastExpr no-op: skip the cast and serialize the child directly
        if (expr instanceof CastExpr && ((CastExpr) expr).isNoOp()) {
            treeToThriftHelper(expr.getChild(0), container, visitor);
            return;
        }

        TExprNode msg = new TExprNode();
        msg.type = expr.getType().toThrift();
        msg.num_children = expr.getChildren().size();
        if (expr.getFn() != null) {
            msg.setFn(FunctionToThriftConverter.toThrift(expr.getFn(),
                    expr.getType(), expr.collectChildReturnTypes(), expr.collectChildReturnNullables()));
            if (expr.getFn().hasVarArgs()) {
                msg.setVarargStartIdx(expr.getFn().getNumArgs() - 1);
            }
        }
        msg.output_scale = -1;
        msg.setIsNullable(expr.isNullable());

        expr.accept(visitor, msg);
        container.addToNodes(msg);

        for (Expr child : expr.getChildren()) {
            treeToThriftHelper(child, container, visitor);
        }
    }

    @Override
    public Void visit(Expr expr, TExprNode msg) {
        throw new UnsupportedOperationException(
                "ExprToThriftVisitor does not support Expr type: " + expr.getClass().getSimpleName());
    }

    // -----------------------------------------------------------------------
    // Literals
    // -----------------------------------------------------------------------

    @Override
    public Void visitBoolLiteral(BoolLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.BOOL_LITERAL;
        msg.bool_literal = new TBoolLiteral(expr.getValue());
        return null;
    }

    @Override
    public Void visitStringLiteral(StringLiteral expr, TExprNode msg) {
        if (expr.getValue() == null) {
            msg.node_type = TExprNodeType.NULL_LITERAL;
        } else {
            msg.string_literal = new TStringLiteral(expr.getValue());
            msg.node_type = TExprNodeType.STRING_LITERAL;
        }
        return null;
    }

    @Override
    public Void visitIntLiteral(IntLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.INT_LITERAL;
        msg.int_literal = new TIntLiteral(expr.getValue());
        return null;
    }

    @Override
    public Void visitLargeIntLiteral(LargeIntLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.LARGE_INT_LITERAL;
        msg.large_int_literal = new TLargeIntLiteral(expr.getStringValue());
        return null;
    }

    @Override
    public Void visitFloatLiteral(FloatLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.FLOAT_LITERAL;
        msg.float_literal = new TFloatLiteral(expr.getValue());
        return null;
    }

    @Override
    public Void visitDecimalLiteral(DecimalLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.DECIMAL_LITERAL;
        msg.decimal_literal = new TDecimalLiteral(expr.getStringValue());
        return null;
    }

    @Override
    public Void visitDateLiteral(DateLiteral expr, TExprNode msg) {
        if (expr.getType().isDatetimeV2() || expr.getType().isTimeStampTz()) {
            expr.roundFloor(((ScalarType) expr.getType()).getScalarScale());
        }
        msg.node_type = TExprNodeType.DATE_LITERAL;
        msg.date_literal = new TDateLiteral(expr.getStringValue());
        try {
            expr.checkValueValid();
        } catch (AnalysisException e) {
            LOG.warn("meet invalid value when plan to translate " + expr.toString() + " to thrift node");
        }
        return null;
    }

    @Override
    public Void visitTimeV2Literal(TimeV2Literal expr, TExprNode msg) {
        msg.node_type = TExprNodeType.TIMEV2_LITERAL;
        msg.timev2_literal = new TTimeV2Literal(expr.getValue());
        return null;
    }

    @Override
    public Void visitNullLiteral(NullLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.NULL_LITERAL;
        return null;
    }

    @Override
    public Void visitMaxLiteral(MaxLiteral expr, TExprNode msg) {
        // TODO: complete this type
        return null;
    }

    @Override
    public Void visitJsonLiteral(JsonLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.JSON_LITERAL;
        msg.json_literal = new TJsonLiteral(expr.getUnescapedValue());
        return null;
    }

    @Override
    public Void visitIPv4Literal(IPv4Literal expr, TExprNode msg) {
        msg.node_type = TExprNodeType.IPV4_LITERAL;
        msg.ipv4_literal = new TIPv4Literal(expr.getValue());
        return null;
    }

    @Override
    public Void visitIPv6Literal(IPv6Literal expr, TExprNode msg) {
        msg.node_type = TExprNodeType.IPV6_LITERAL;
        msg.ipv6_literal = new TIPv6Literal(expr.getValue());
        return null;
    }

    @Override
    public Void visitVarBinaryLiteral(VarBinaryLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.VARBINARY_LITERAL;
        msg.varbinary_literal = new TVarBinaryLiteral(ByteBuffer.wrap(expr.getValue()));
        return null;
    }

    @Override
    public Void visitArrayLiteral(ArrayLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.ARRAY_LITERAL;
        msg.setChildType(((ArrayType) expr.getType()).getItemType().getPrimitiveType().toThrift());
        return null;
    }

    @Override
    public Void visitMapLiteral(MapLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.MAP_LITERAL;
        TTypeDesc container = new TTypeDesc();
        container.setTypes(new ArrayList<TTypeNode>());
        expr.getType().toThrift(container);
        msg.setType(container);
        return null;
    }

    @Override
    public Void visitStructLiteral(StructLiteral expr, TExprNode msg) {
        msg.node_type = TExprNodeType.STRUCT_LITERAL;
        ((StructType) expr.getType()).getFields()
                .forEach(v -> msg.setChildType(v.getType().getPrimitiveType().toThrift()));
        TTypeDesc container = new TTypeDesc();
        container.setTypes(new ArrayList<TTypeNode>());
        expr.getType().toThrift(container);
        msg.setType(container);
        return null;
    }

    @Override
    public Void visitPlaceHolderExpr(PlaceHolderExpr expr, TExprNode msg) {
        expr.getLiteral().accept(this, msg);
        return null;
    }

    // -----------------------------------------------------------------------
    // Reference / slot expressions
    // -----------------------------------------------------------------------

    @Override
    public Void visitSlotRef(SlotRef expr, TExprNode msg) {
        msg.node_type = TExprNodeType.SLOT_REF;
        msg.slot_ref = new TSlotRef(expr.getDesc().getId().asInt(), expr.getDesc().getParentId().asInt());
        msg.slot_ref.setColUniqueId(expr.getDesc().getUniqueId());
        msg.slot_ref.setIsVirtualSlot(expr.getDesc().getVirtualColumn() != null);
        msg.setLabel(expr.getLabel());
        return null;
    }

    @Override
    public Void visitColumnRefExpr(ColumnRefExpr expr, TExprNode msg) {
        msg.node_type = TExprNodeType.COLUMN_REF;
        TColumnRef columnRef = new TColumnRef();
        columnRef.setColumnId(expr.getColumnId());
        columnRef.setColumnName(expr.getName());
        msg.column_ref = columnRef;
        return null;
    }

    @Override
    public Void visitInformationFunction(InformationFunction expr, TExprNode msg) {
        msg.node_type = TExprNodeType.INFO_FUNC;
        msg.info_func = new TInfoFunc(Long.parseLong(expr.getIntValue()), expr.getStrValue());
        return null;
    }

    @Override
    public Void visitEncryptKeyRef(EncryptKeyRef expr, TExprNode msg) {
        // no operation
        return null;
    }

    @Override
    public Void visitVariableExpr(VariableExpr expr, TExprNode msg) {
        switch (expr.getType().getPrimitiveType()) {
            case BOOLEAN:
                msg.node_type = TExprNodeType.BOOL_LITERAL;
                msg.bool_literal = new TBoolLiteral(expr.getBoolValue());
                break;
            case TINYINT:
            case SMALLINT:
            case INT:
            case BIGINT:
                msg.node_type = TExprNodeType.INT_LITERAL;
                msg.int_literal = new TIntLiteral(expr.getIntValue());
                break;
            case FLOAT:
            case DOUBLE:
                msg.node_type = TExprNodeType.FLOAT_LITERAL;
                msg.float_literal = new TFloatLiteral(expr.getFloatValue());
                break;
            default:
                if (expr.getStrValue() == null) {
                    msg.node_type = TExprNodeType.NULL_LITERAL;
                } else {
                    msg.node_type = TExprNodeType.STRING_LITERAL;
                    msg.string_literal = new TStringLiteral(expr.getStrValue());
                }
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Predicates
    // -----------------------------------------------------------------------

    @Override
    public Void visitBinaryPredicate(BinaryPredicate expr, TExprNode msg) {
        msg.node_type = TExprNodeType.BINARY_PRED;
        msg.setOpcode(toThriftOpcode(expr.getOp()));
        msg.setChildType(expr.getChild(0).getType().getPrimitiveType().toThrift());
        return null;
    }

    @Override
    public Void visitIsNullPredicate(IsNullPredicate expr, TExprNode msg) {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
        return null;
    }

    @Override
    public Void visitCompoundPredicate(CompoundPredicate expr, TExprNode msg) {
        msg.node_type = TExprNodeType.COMPOUND_PRED;
        msg.setOpcode(toThriftOpcode(expr.getOp()));
        return null;
    }

    @Override
    public Void visitInPredicate(InPredicate expr, TExprNode msg) {
        msg.in_predicate = new TInPredicate(expr.isNotIn());
        msg.node_type = TExprNodeType.IN_PRED;
        msg.setOpcode(toThriftOpcode(expr));
        return null;
    }

    @Override
    public Void visitLikePredicate(LikePredicate expr, TExprNode msg) {
        msg.node_type = TExprNodeType.FUNCTION_CALL;
        return null;
    }

    @Override
    public Void visitMatchPredicate(MatchPredicate expr, TExprNode msg) {
        msg.node_type = TExprNodeType.MATCH_PRED;
        msg.setOpcode(toThriftOpcode(expr.getOp()));
        msg.match_predicate = new TMatchPredicate(
                expr.getInvertedIndexParser(), expr.getInvertedIndexParserMode());
        msg.match_predicate.setCharFilterMap(expr.getInvertedIndexCharFilter());
        msg.match_predicate.setParserLowercase(expr.getInvertedIndexParserLowercase());
        msg.match_predicate.setParserStopwords(expr.getInvertedIndexParserStopwords());
        msg.match_predicate.setAnalyzerName(expr.getInvertedIndexAnalyzerName());
        return null;
    }

    @Override
    public Void visitBetweenPredicate(BetweenPredicate expr, TExprNode msg) {
        throw new IllegalStateException(
                "BetweenPredicate needs to be rewritten into a CompoundPredicate.");
    }

    @Override
    public Void visitSearchPredicate(SearchPredicate expr, TExprNode msg) {
        msg.node_type = TExprNodeType.SEARCH_EXPR;
        msg.setSearchParam(buildSearchThriftParam(expr));

        LOG.info("SearchPredicate.toThrift: dsl='{}', num_children_in_base={}, children_size={}",
                expr.getDslString(), msg.num_children, expr.getChildren().size());

        if (expr.getQsPlan() != null) {
            LOG.info("SearchPredicate.toThrift: QsPlan fieldBindings.size={}",
                    expr.getQsPlan().getFieldBindings() != null
                            ? expr.getQsPlan().getFieldBindings().size() : 0);
            if (expr.getQsPlan().getFieldBindings() != null) {
                for (int i = 0; i < expr.getQsPlan().getFieldBindings().size(); i++) {
                    SearchDslParser.QsFieldBinding binding = expr.getQsPlan().getFieldBindings().get(i);
                    LOG.info("SearchPredicate.toThrift: binding[{}] fieldName='{}', slotIndex={}",
                            i, binding.getFieldName(), binding.getSlotIndex());
                }
            }
        }

        for (int i = 0; i < expr.getChildren().size(); i++) {
            Expr child = expr.getChildren().get(i);
            LOG.info("SearchPredicate.toThrift: child[{}] = {} (type={})",
                    i, child.getClass().getSimpleName(), child.getType());
            if (child instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) child;
                LOG.info("SearchPredicate.toThrift: SlotRef details - column={}",
                        slotRef.getColumnName());
                if (slotRef.getDesc() != null) {
                    LOG.info("SearchPredicate.toThrift: SlotRef analyzed - slotId={}",
                            slotRef.getSlotId());
                }
            }
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // Arithmetic / cast
    // -----------------------------------------------------------------------

    @Override
    public Void visitArithmeticExpr(ArithmeticExpr expr, TExprNode msg) {
        msg.node_type = TExprNodeType.ARITHMETIC_EXPR;
        if (!(expr.getType().isDecimalV2() || expr.getType().isDecimalV3())) {
            msg.setOpcode(toThriftOpcode(expr.getOp()));
        }
        return null;
    }

    @Override
    public Void visitCastExpr(CastExpr expr, TExprNode msg) {
        msg.node_type = TExprNodeType.CAST_EXPR;
        msg.setOpcode(TExprOpcode.CAST);
        return null;
    }

    @Override
    public Void visitTryCastExpr(TryCastExpr expr, TExprNode msg) {
        msg.node_type = TExprNodeType.TRY_CAST_EXPR;
        msg.setIsCastNullable(expr.isOriginCastNullable());
        msg.setOpcode(TExprOpcode.TRY_CAST);
        return null;
    }

    @Override
    public Void visitTimestampArithmeticExpr(TimestampArithmeticExpr expr, TExprNode msg) {
        msg.node_type = TExprNodeType.COMPUTE_FUNCTION_CALL;
        msg.setOpcode(toThriftOpcode(expr));
        return null;
    }

    // -----------------------------------------------------------------------
    // Functions / lambda / case
    // -----------------------------------------------------------------------

    @Override
    public Void visitFunctionCallExpr(FunctionCallExpr expr, TExprNode msg) {
        if (expr.isAggregateFunction() || expr.isAnalyticFnCall()) {
            msg.node_type = TExprNodeType.AGG_EXPR;
            FunctionParams aggParams = expr.getAggFnParams();
            if (aggParams == null) {
                aggParams = expr.getFnParams();
            }
            msg.setAggExpr(createTAggregateExprFromFunctionParams(aggParams, expr.isMergeAggFn()));
        } else {
            msg.node_type = TExprNodeType.FUNCTION_CALL;
        }

        if (ConnectContext.get() != null) {
            msg.setShortCircuitEvaluation(ConnectContext.get().getSessionVariable().isShortCircuitEvaluation());
        }
        return null;
    }

    public TAggregateExpr createTAggregateExprFromFunctionParams(FunctionParams functionParams, boolean isMergeAggFn) {
        List<TTypeDesc> paramTypes = new ArrayList<>();
        if (functionParams.exprs() != null) {
            for (Expr expr : functionParams.exprs()) {
                TTypeDesc desc = expr.getType().toThrift();
                desc.setIsNullable(expr.isNullable());
                paramTypes.add(desc);
            }
        }
        TAggregateExpr aggExpr = new TAggregateExpr(isMergeAggFn);
        aggExpr.setParamTypes(paramTypes);
        return aggExpr;
    }

    @Override
    public Void visitLambdaFunctionCallExpr(LambdaFunctionCallExpr expr, TExprNode msg) {
        msg.node_type = TExprNodeType.LAMBDA_FUNCTION_CALL_EXPR;
        return null;
    }

    @Override
    public Void visitLambdaFunctionExpr(LambdaFunctionExpr expr, TExprNode msg) {
        msg.setNodeType(TExprNodeType.LAMBDA_FUNCTION_EXPR);
        return null;
    }

    @Override
    public Void visitCaseExpr(CaseExpr expr, TExprNode msg) {
        msg.node_type = TExprNodeType.CASE_EXPR;
        msg.case_expr = new TCaseExpr(expr.isHasCaseExpr(), expr.isHasElseExpr());
        if (ConnectContext.get() != null) {
            msg.setShortCircuitEvaluation(ConnectContext.get().getSessionVariable().isShortCircuitEvaluation());
        }
        return null;
    }

    // -----------------------------------------------------------------------
    // TExprOpcode conversion helpers
    // -----------------------------------------------------------------------

    public static TExprOpcode toThriftOpcode(ArithmeticExpr.Operator op) {
        switch (op) {
            case MULTIPLY: return TExprOpcode.MULTIPLY;
            case DIVIDE: return TExprOpcode.DIVIDE;
            case MOD: return TExprOpcode.MOD;
            case INT_DIVIDE: return TExprOpcode.INT_DIVIDE;
            case ADD: return TExprOpcode.ADD;
            case SUBTRACT: return TExprOpcode.SUBTRACT;
            case BITAND: return TExprOpcode.BITAND;
            case BITOR: return TExprOpcode.BITOR;
            case BITXOR: return TExprOpcode.BITXOR;
            case BITNOT: return TExprOpcode.BITNOT;
            default:
                throw new IllegalStateException("Unknown ArithmeticExpr.Operator: " + op);
        }
    }

    public static TExprOpcode toThriftOpcode(BinaryPredicate.Operator op) {
        switch (op) {
            case EQ: return TExprOpcode.EQ;
            case NE: return TExprOpcode.NE;
            case LE: return TExprOpcode.LE;
            case GE: return TExprOpcode.GE;
            case LT: return TExprOpcode.LT;
            case GT: return TExprOpcode.GT;
            case EQ_FOR_NULL: return TExprOpcode.EQ_FOR_NULL;
            default:
                throw new IllegalStateException("Unknown BinaryPredicate.Operator: " + op);
        }
    }

    public static TExprOpcode toThriftOpcode(CompoundPredicate.Operator op) {
        switch (op) {
            case AND: return TExprOpcode.COMPOUND_AND;
            case OR: return TExprOpcode.COMPOUND_OR;
            case NOT: return TExprOpcode.COMPOUND_NOT;
            default:
                throw new IllegalStateException("Unknown CompoundPredicate.Operator: " + op);
        }
    }

    public static TExprOpcode toThriftOpcode(MatchPredicate.Operator op) {
        switch (op) {
            case MATCH_ANY: return TExprOpcode.MATCH_ANY;
            case MATCH_ALL: return TExprOpcode.MATCH_ALL;
            case MATCH_PHRASE: return TExprOpcode.MATCH_PHRASE;
            case MATCH_PHRASE_PREFIX: return TExprOpcode.MATCH_PHRASE_PREFIX;
            case MATCH_REGEXP: return TExprOpcode.MATCH_REGEXP;
            case MATCH_PHRASE_EDGE: return TExprOpcode.MATCH_PHRASE_EDGE;
            default:
                throw new IllegalStateException("Unknown MatchPredicate.Operator: " + op);
        }
    }

    public static TExprOpcode toThriftOpcode(InPredicate expr) {
        boolean allConstant = expr.getAllConstant();
        if (allConstant) {
            return expr.isNotIn() ? TExprOpcode.FILTER_NOT_IN : TExprOpcode.FILTER_IN;
        } else {
            return expr.isNotIn() ? TExprOpcode.FILTER_NEW_NOT_IN : TExprOpcode.FILTER_NEW_IN;
        }
    }

    public static TExprOpcode toThriftOpcode(TimestampArithmeticExpr expr) {
        ArithmeticExpr.Operator op = expr.getOp();
        TimestampArithmeticExpr.TimeUnit timeUnit = expr.getTimeUnit();
        switch (timeUnit) {
            case YEAR:
                return op == Operator.ADD ? TExprOpcode.TIMESTAMP_YEARS_ADD : TExprOpcode.TIMESTAMP_YEARS_SUB;
            case MONTH:
                return op == Operator.ADD ? TExprOpcode.TIMESTAMP_MONTHS_ADD : TExprOpcode.TIMESTAMP_MONTHS_SUB;
            case WEEK:
                return op == Operator.ADD ? TExprOpcode.TIMESTAMP_WEEKS_ADD : TExprOpcode.TIMESTAMP_WEEKS_SUB;
            case DAY:
                return op == Operator.ADD ? TExprOpcode.TIMESTAMP_DAYS_ADD : TExprOpcode.TIMESTAMP_DAYS_SUB;
            case HOUR:
                return op == Operator.ADD ? TExprOpcode.TIMESTAMP_HOURS_ADD : TExprOpcode.TIMESTAMP_HOURS_SUB;
            case MINUTE:
                return op == Operator.ADD ? TExprOpcode.TIMESTAMP_MINUTES_ADD : TExprOpcode.TIMESTAMP_MINUTES_SUB;
            case SECOND:
                return op == Operator.ADD ? TExprOpcode.TIMESTAMP_SECONDS_ADD : TExprOpcode.TIMESTAMP_SECONDS_SUB;
            default:
                try {
                    ErrorReport.reportAnalysisException(ErrorCode.ERR_BAD_TIMEUNIT, timeUnit);
                } catch (AnalysisException e) {
                    throw new IllegalStateException(e);
                }
                return TExprOpcode.INVALID_OPCODE;
        }
    }

    // -----------------------------------------------------------------------
    // SearchPredicate helpers (moved from SearchPredicate.java)
    // -----------------------------------------------------------------------

    static TSearchParam buildSearchThriftParam(SearchPredicate expr) {
        TSearchParam param = new TSearchParam();
        param.setOriginalDsl(expr.getDslString());
        param.setRoot(convertQsNodeToThrift(expr.getQsPlan().getRoot()));

        List<TSearchFieldBinding> bindings = new ArrayList<>();
        SearchDslParser.QsPlan qsPlan = expr.getQsPlan();
        for (int i = 0; i < qsPlan.getFieldBindings().size(); i++) {
            SearchDslParser.QsFieldBinding binding = qsPlan.getFieldBindings().get(i);
            TSearchFieldBinding thriftBinding = new TSearchFieldBinding();

            String fieldPath = binding.getFieldName();
            thriftBinding.setFieldName(fieldPath);

            if (fieldPath.contains(".")) {
                int firstDotPos = fieldPath.indexOf('.');
                String parentField = fieldPath.substring(0, firstDotPos);
                String subcolumnPath = fieldPath.substring(firstDotPos + 1);

                thriftBinding.setIsVariantSubcolumn(true);
                thriftBinding.setParentFieldName(parentField);
                thriftBinding.setSubcolumnPath(subcolumnPath);

                LOG.info("buildThriftParam: variant subcolumn field='{}', parent='{}', subcolumn='{}'",
                        fieldPath, parentField, subcolumnPath);
            } else {
                thriftBinding.setIsVariantSubcolumn(false);
            }

            thriftBinding.setSlotIndex(i);

            if (i < expr.getChildren().size() && expr.getChildren().get(i) instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) expr.getChildren().get(i);
                int actualSlotId = slotRef.getSlotId().asInt();
                thriftBinding.setSlotIndex(actualSlotId);
                LOG.info("buildThriftParam: binding field='{}', actual slotId={}",
                        binding.getFieldName(), actualSlotId);
            } else {
                LOG.warn("buildThriftParam: No corresponding SlotRef for field '{}'", binding.getFieldName());
                thriftBinding.setSlotIndex(i);
            }

            List<org.apache.doris.catalog.Index> fieldIndexes = expr.getFieldIndexes();
            if (i < fieldIndexes.size() && fieldIndexes.get(i) != null) {
                Map<String, String> properties = fieldIndexes.get(i).getProperties();
                if (properties != null && !properties.isEmpty()) {
                    thriftBinding.setIndexProperties(properties);
                    LOG.debug("buildThriftParam: field='{}' index_properties={}",
                            fieldPath, properties);
                }
            }

            bindings.add(thriftBinding);
        }
        param.setFieldBindings(bindings);

        if (qsPlan.getDefaultOperator() != null) {
            param.setDefaultOperator(qsPlan.getDefaultOperator());
        }

        if (qsPlan.getMinimumShouldMatch() != null) {
            param.setMinimumShouldMatch(qsPlan.getMinimumShouldMatch());
        }

        return param;
    }

    static TSearchClause convertQsNodeToThrift(SearchDslParser.QsNode node) {
        TSearchClause clause = new TSearchClause();
        clause.setClauseType(node.getType().name());

        if (node.getField() != null) {
            clause.setFieldName(node.getField());
        }

        if (node.getType() == SearchDslParser.QsClauseType.NESTED && node.getNestedPath() != null) {
            clause.setNestedPath(node.getNestedPath());
        }

        if (node.getValue() != null) {
            clause.setValue(node.getValue());
        }

        if (node.getOccur() != null) {
            clause.setOccur(convertQsOccurToThrift(node.getOccur()));
        }

        if (node.getMinimumShouldMatch() != null) {
            clause.setMinimumShouldMatch(node.getMinimumShouldMatch());
        }

        if (node.getChildren() != null && !node.getChildren().isEmpty()) {
            List<TSearchClause> childClauses = new ArrayList<>();
            for (SearchDslParser.QsNode child : node.getChildren()) {
                childClauses.add(convertQsNodeToThrift(child));
            }
            clause.setChildren(childClauses);
        }

        return clause;
    }

    static TSearchOccur convertQsOccurToThrift(SearchDslParser.QsOccur occur) {
        switch (occur) {
            case MUST:
                return TSearchOccur.MUST;
            case SHOULD:
                return TSearchOccur.SHOULD;
            case MUST_NOT:
                return TSearchOccur.MUST_NOT;
            default:
                return TSearchOccur.MUST;
        }
    }

    /**
     * Build DSL AST explain lines for a SearchPredicate.
     * Moved from SearchPredicate to remove thrift dependency from Expr subclass.
     */
    public static List<String> buildDslAstExplainLines(SearchPredicate expr) {
        List<String> lines = new ArrayList<>();
        if (expr.getQsPlan() == null || expr.getQsPlan().getRoot() == null) {
            return lines;
        }
        TSearchClause rootClause = convertQsNodeToThrift(expr.getQsPlan().getRoot());
        appendClauseExplain(rootClause, lines, 0);
        return lines;
    }

    private static void appendClauseExplain(TSearchClause clause, List<String> lines, int depth) {
        StringBuilder line = new StringBuilder();
        line.append(indent(depth)).append("- clause_type=").append(clause.getClauseType());
        if (clause.isSetNestedPath()) {
            line.append(", nested_path=").append('"').append(escapeText(clause.getNestedPath())).append('"');
        }
        if (clause.isSetFieldName()) {
            line.append(", field=").append('"').append(escapeText(clause.getFieldName())).append('"');
        }
        if (clause.isSetValue()) {
            line.append(", value=").append('"').append(escapeText(clause.getValue())).append('"');
        }
        lines.add(line.toString());

        if (clause.isSetChildren() && clause.getChildren() != null && !clause.getChildren().isEmpty()) {
            for (TSearchClause child : clause.getChildren()) {
                appendClauseExplain(child, lines, depth + 1);
            }
        }
    }

    private static String indent(int level) {
        if (level <= 0) {
            return "";
        }
        StringBuilder sb = new StringBuilder(level * 2);
        for (int i = 0; i < level; i++) {
            sb.append("  ");
        }
        return sb.toString();
    }

    private static String escapeText(String value) {
        if (value == null) {
            return "";
        }
        return value
                .replace("\\", "\\\\")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r");
    }
}
