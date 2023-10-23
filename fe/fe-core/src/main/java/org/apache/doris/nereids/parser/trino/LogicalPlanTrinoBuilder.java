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

package org.apache.doris.nereids.parser.trino;

import org.apache.doris.nereids.analyzer.UnboundAlias;
import org.apache.doris.nereids.analyzer.UnboundFunction;
import org.apache.doris.nereids.analyzer.UnboundOneRowRelation;
import org.apache.doris.nereids.analyzer.UnboundRelation;
import org.apache.doris.nereids.analyzer.UnboundResultSink;
import org.apache.doris.nereids.analyzer.UnboundSlot;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.exceptions.DialectTransformException;
import org.apache.doris.nereids.parser.LogicalPlanBuilderAssistant;
import org.apache.doris.nereids.parser.ParserContext;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.NamedExpression;
import org.apache.doris.nereids.trees.expressions.StatementScopeIdGenerator;
import org.apache.doris.nereids.trees.expressions.functions.Function;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.VarcharLiteral;
import org.apache.doris.nereids.trees.plans.logical.LogicalPlan;
import org.apache.doris.nereids.trees.plans.logical.LogicalProject;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.coercion.CharacterType;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * The actually planBuilder for Trino SQL to Doris logical plan.
 * It depends on {@link io.trino.sql.tree.AstVisitor}
 */
public class LogicalPlanTrinoBuilder extends io.trino.sql.tree.AstVisitor<Object, ParserContext> {

    public Object visit(io.trino.sql.tree.Node node, ParserContext context) {
        return this.process(node, context);
    }

    public <T> T visit(io.trino.sql.tree.Node node, ParserContext context, Class<T> clazz) {
        return clazz.cast(this.process(node, context));
    }

    public <T> List<T> visit(List<? extends io.trino.sql.tree.Node> nodes, ParserContext context, Class<T> clazz) {
        return nodes.stream()
                .map(node -> clazz.cast(this.process(node, context)))
                .collect(Collectors.toList());
    }

    public Object processOptional(Optional<? extends io.trino.sql.tree.Node> node, ParserContext context) {
        return node.map(value -> this.process(value, context)).orElse(null);
    }

    public <T> T processOptional(Optional<? extends io.trino.sql.tree.Node> node,
            ParserContext context, Class<T> clazz) {
        return node.map(value -> clazz.cast(this.process(value, context))).orElse(null);
    }

    @Override
    protected LogicalPlan visitQuery(io.trino.sql.tree.Query node, ParserContext context) {
        io.trino.sql.tree.QueryBody queryBody = node.getQueryBody();
        LogicalPlan logicalPlan = (LogicalPlan) visit(queryBody, context);
        if (!(queryBody instanceof io.trino.sql.tree.QuerySpecification)) {
            // TODO: need to handle orderBy and limit
            throw new DialectTransformException("transform querySpecification");
        }
        return logicalPlan;
    }

    @Override
    protected LogicalPlan visitQuerySpecification(io.trino.sql.tree.QuerySpecification node,
            ParserContext context) {
        // from -> where -> group by -> having -> select
        Optional<io.trino.sql.tree.Relation> from = node.getFrom();
        LogicalPlan fromPlan = processOptional(from, context, LogicalPlan.class);
        List<io.trino.sql.tree.SelectItem> selectItems = node.getSelect().getSelectItems();
        if (from == null || !from.isPresent()) {
            // TODO: support query values
            List<NamedExpression> expressions = selectItems.stream()
                    .map(item -> visit(item, context, NamedExpression.class))
                    .collect(ImmutableList.toImmutableList());
            return new UnboundOneRowRelation(StatementScopeIdGenerator.newRelationId(), expressions);
        }
        // TODO: support predicate, aggregate, having, order by, limit
        // TODO: support distinct
        boolean isDistinct = node.getSelect().isDistinct();
        return new UnboundResultSink<>(withProjection(selectItems, fromPlan, isDistinct, context));
    }

    private LogicalProject withProjection(List<io.trino.sql.tree.SelectItem> selectItems,
            LogicalPlan input,
            boolean isDistinct,
            ParserContext context) {
        List<NamedExpression> expressions = selectItems.stream()
                .map(item -> visit(item, context, NamedExpression.class))
                .collect(Collectors.toList());
        return new LogicalProject(expressions, ImmutableList.of(), isDistinct, input);
    }

    @Override
    protected Expression visitSingleColumn(io.trino.sql.tree.SingleColumn node, ParserContext context) {
        String alias = node.getAlias().map(io.trino.sql.tree.Identifier::getValue).orElse(null);
        Expression expr = visit(node.getExpression(), context, Expression.class);
        if (expr instanceof NamedExpression) {
            return (NamedExpression) expr;
        } else {
            return alias == null ? new UnboundAlias(expr) : new UnboundAlias(expr, alias);
        }
    }

    @Override
    protected Object visitIdentifier(io.trino.sql.tree.Identifier node, ParserContext context) {
        return new UnboundSlot(ImmutableList.of(node.getValue()));
    }

    /* ********************************************************************************************
     * visitFunction
     * ******************************************************************************************** */

    @Override
    protected Function visitFunctionCall(io.trino.sql.tree.FunctionCall node, ParserContext context) {
        List<Expression> exprs = visit(node.getArguments(), context, Expression.class);
        Function transformedFn =
                TrinoFnCallTransformers.transform(node.getName().toString(), exprs, context);
        if (transformedFn == null) {
            transformedFn = new UnboundFunction(node.getName().toString(), exprs);

        }
        return transformedFn;
    }

    /* ********************************************************************************************
     * visitTable
     * ******************************************************************************************** */

    @Override
    protected LogicalPlan visitTable(io.trino.sql.tree.Table node, ParserContext context) {
        io.trino.sql.tree.QualifiedName name = node.getName();
        List<String> tableId = name.getParts();
        // build table
        return LogicalPlanBuilderAssistant.withCheckPolicy(
                new UnboundRelation(StatementScopeIdGenerator.newRelationId(), tableId,
                        ImmutableList.of(), false));
    }

    /* ********************************************************************************************
     * visit buildIn function
     * ******************************************************************************************** */

    @Override
    protected Expression visitCast(io.trino.sql.tree.Cast node, ParserContext context) {
        Expression expr = visit(node.getExpression(), context, Expression.class);
        DataType dataType = mappingType(node.getType());
        Expression cast = new Cast(expr, dataType);
        if (dataType.isStringLikeType() && ((CharacterType) dataType).getLen() >= 0) {
            List<Expression> args = ImmutableList.of(
                    cast,
                    new TinyIntLiteral((byte) 1),
                    Literal.of(((CharacterType) dataType).getLen())
            );
            return new UnboundFunction("substr", args);
        } else {
            return cast;
        }
    }

    /* ********************************************************************************************
     * visitLiteral
     * ******************************************************************************************** */

    @Override
    protected Object visitLiteral(io.trino.sql.tree.Literal node, ParserContext context) {
        // TODO: support literal transform
        throw new DialectTransformException("transform literal");
    }

    @Override
    protected Literal visitLongLiteral(io.trino.sql.tree.LongLiteral node, ParserContext context) {
        return LogicalPlanBuilderAssistant.handleIntegerLiteral(String.valueOf(node.getValue()));
    }

    @Override
    protected Object visitDoubleLiteral(io.trino.sql.tree.DoubleLiteral node, ParserContext context) {
        // TODO: support double literal transform
        throw new DialectTransformException("transform double literal");
    }

    @Override
    protected Object visitDecimalLiteral(io.trino.sql.tree.DecimalLiteral node, ParserContext context) {
        // TODO: support decimal literal transform
        throw new DialectTransformException("transform decimal literal");
    }

    @Override
    protected Object visitTimestampLiteral(io.trino.sql.tree.TimestampLiteral node, ParserContext context) {
        try {
            String value = node.getValue();
            if (value.length() <= 10) {
                value += " 00:00:00";
            }
            return new DateTimeLiteral(value);
        } catch (AnalysisException e) {
            throw new DialectTransformException("transform timestamp literal");
        }
    }

    @Override
    protected Object visitGenericLiteral(io.trino.sql.tree.GenericLiteral node, ParserContext context) {
        // TODO: support generic literal transform
        throw new DialectTransformException("transform generic literal");
    }

    @Override
    protected Object visitTimeLiteral(io.trino.sql.tree.TimeLiteral node, ParserContext context) {
        // TODO: support time literal transform
        throw new DialectTransformException("transform time literal");
    }

    @Override
    protected Object visitCharLiteral(io.trino.sql.tree.CharLiteral node, ParserContext context) {
        // TODO: support char literal transform
        throw new DialectTransformException("transform char literal");
    }

    @Override
    protected Expression visitStringLiteral(io.trino.sql.tree.StringLiteral node, ParserContext context) {
        // TODO: add unescapeSQLString.
        String txt = node.getValue();
        if (txt.length() <= 1) {
            return new VarcharLiteral(txt);
        }
        return new VarcharLiteral(LogicalPlanBuilderAssistant.escapeBackSlash(txt.substring(0, txt.length())));
    }

    @Override
    protected Object visitIntervalLiteral(io.trino.sql.tree.IntervalLiteral node, ParserContext context) {
        // TODO: support interval literal transform
        throw new DialectTransformException("transform char literal");
    }

    @Override
    protected Object visitBinaryLiteral(io.trino.sql.tree.BinaryLiteral node, ParserContext context) {
        // TODO: support binary literal transform
        throw new DialectTransformException("transform binary literal");
    }

    @Override
    protected Object visitNullLiteral(io.trino.sql.tree.NullLiteral node, ParserContext context) {
        return NullLiteral.INSTANCE;
    }

    @Override
    protected Object visitBooleanLiteral(io.trino.sql.tree.BooleanLiteral node, ParserContext context) {
        return BooleanLiteral.of(node.getValue());
    }

    private DataType mappingType(io.trino.sql.tree.DataType dataType) {

        if (dataType instanceof io.trino.sql.tree.GenericDataType) {
            io.trino.sql.tree.GenericDataType genericDataType = (io.trino.sql.tree.GenericDataType) dataType;
            String typeName = genericDataType.getName().getValue().toLowerCase();
            List<String> types = Lists.newArrayList(typeName);

            String length = null;
            String precision = null;
            String scale = null;
            List<io.trino.sql.tree.DataTypeParameter> arguments = genericDataType.getArguments();
            if (!arguments.isEmpty()) {
                if (arguments.get(0) instanceof io.trino.sql.tree.NumericParameter) {
                    precision = length = ((io.trino.sql.tree.NumericParameter) arguments.get(0)).getValue();
                }
                if (arguments.size() > 1 && arguments.get(1) instanceof io.trino.sql.tree.NumericParameter) {
                    scale = ((io.trino.sql.tree.NumericParameter) arguments.get(1)).getValue();
                }
            }
            if ("decimal".equals(typeName)) {
                if (precision != null) {
                    types.add(precision);
                }
                if (scale != null) {
                    types.add(scale);
                }
            }
            if ("varchar".equals(typeName) || "char".equals(typeName)) {
                if (length != null) {
                    types.add(length);
                }
            }

            // unsigned decimal in Trino is longDecimal, not handle now, support it later
            if (!"decimal".equals(typeName) && typeName.contains("decimal")) {
                throw new DialectTransformException("transform not standard decimal data type ");
            }
            // Trino only support signed, safe unsigned is false here
            return DataType.convertPrimitiveFromStrings(types, false);
        } else if (dataType instanceof io.trino.sql.tree.DateTimeDataType) {
            // TODO: support date data type mapping
            throw new DialectTransformException("transform date data type");
        }
        throw new AnalysisException("Nereids do not support type: " + dataType);
    }
}
