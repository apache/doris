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

import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.base.Preconditions;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Visitor that produces the standard SQL string for any {@link Expr}.
 * Replaces the 0-arg {@code toSqlImpl()} body in every subclass.
 *
 * <p>Usage: {@code expr.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITH_TABLE)}
 */
public class ExprToSqlVisitor extends ExprVisitor<String, ToSqlParams> {

    public static final ExprToSqlVisitor INSTANCE = new ExprToSqlVisitor();

    protected ExprToSqlVisitor() {
        // singleton
    }

    @Override
    public String visit(Expr expr, ToSqlParams context) {
        throw new UnsupportedOperationException("ExprToSqlVisitor does not support Expr type: "
                + expr.getClass().getSimpleName());
    }

    // -----------------------------------------------------------------------
    // Literals
    // -----------------------------------------------------------------------

    @Override
    public String visitBoolLiteral(BoolLiteral expr, ToSqlParams context) {
        return expr.getValue() ? "TRUE" : "FALSE";
    }

    @Override
    public String visitStringLiteral(StringLiteral expr, ToSqlParams context) {
        return "'" + expr.getValue().replaceAll("'", "''") + "'";
    }

    @Override
    public String visitIntLiteral(IntLiteral expr, ToSqlParams context) {
        return expr.getStringValue();
    }

    @Override
    public String visitLargeIntLiteral(LargeIntLiteral expr, ToSqlParams context) {
        return expr.getStringValue();
    }

    @Override
    public String visitFloatLiteral(FloatLiteral expr, ToSqlParams context) {
        return expr.getStringValue();
    }

    @Override
    public String visitDecimalLiteral(DecimalLiteral expr, ToSqlParams context) {
        return expr.getStringValue();
    }

    @Override
    public String visitDateLiteral(DateLiteral expr, ToSqlParams context) {
        return "'" + expr.getStringValue() + "'";
    }

    @Override
    public String visitTimeV2Literal(TimeV2Literal expr, ToSqlParams context) {
        return "\"" + expr.getStringValue() + "\"";
    }

    @Override
    public String visitNullLiteral(NullLiteral expr, ToSqlParams context) {
        return expr.getStringValue();
    }

    @Override
    public String visitMaxLiteral(MaxLiteral expr, ToSqlParams context) {
        return "MAXVALUE";
    }

    @Override
    public String visitJsonLiteral(JsonLiteral expr, ToSqlParams context) {
        return "'" + expr.getValue().replaceAll("'", "''") + "'";
    }

    @Override
    public String visitIPv4Literal(IPv4Literal expr, ToSqlParams context) {
        return "\"" + expr.getStringValue() + "\"";
    }

    @Override
    public String visitIPv6Literal(IPv6Literal expr, ToSqlParams context) {
        return "\"" + expr.getStringValue() + "\"";
    }

    @Override
    public String visitVarBinaryLiteral(VarBinaryLiteral expr, ToSqlParams context) {
        return expr.toHexLiteral();
    }

    @Override
    public String visitArrayLiteral(ArrayLiteral expr, ToSqlParams context) {
        List<String> list = new ArrayList<>(expr.getChildren().size());
        expr.getChildren().forEach(v -> list.add(v.accept(this, context)));
        return "[" + StringUtils.join(list, ", ") + "]";
    }

    @Override
    public String visitMapLiteral(MapLiteral expr, ToSqlParams context) {
        List<String> list = new ArrayList<>(expr.getChildren().size());
        for (int i = 0; i < expr.getChildren().size() && i + 1 < expr.getChildren().size(); i += 2) {
            list.add(expr.getChild(i).accept(this, context) + ":" + expr.getChild(i + 1).accept(this, context));
        }
        return "MAP{" + StringUtils.join(list, ", ") + "}";
    }

    @Override
    public String visitStructLiteral(StructLiteral expr, ToSqlParams context) {
        List<String> list = new ArrayList<>(expr.getChildren().size());
        expr.getChildren().forEach(v -> list.add(v.accept(this, context)));
        return "STRUCT(" + StringUtils.join(list, ", ") + ")";
    }

    @Override
    public String visitPlaceHolderExpr(PlaceHolderExpr expr, ToSqlParams context) {
        if (expr.getLiteral() == null) {
            return "?";
        }
        return "_placeholder_(" + expr.getLiteral().accept(this, context) + ")";
    }

    // -----------------------------------------------------------------------
    // Reference / slot expressions
    // -----------------------------------------------------------------------

    @Override
    public String visitSlotRef(SlotRef expr, ToSqlParams context) {
        if (context.disableTableName && expr.getLabel() != null) {
            return expr.getLabel();
        }

        StringBuilder sb = new StringBuilder();
        String subColumnPaths = "";
        if (expr.getSubColPath() != null && !expr.getSubColPath().isEmpty()) {
            subColumnPaths = "." + String.join(".", expr.getSubColPath());
        }
        if (expr.getTableNameInfo() != null) {
            return expr.getTableNameInfo().toSql() + "." + expr.getLabel() + subColumnPaths;
        } else if (expr.getLabel() != null) {
            if (ConnectContext.get() != null
                    && ConnectContext.get().getState().isNereids()
                    && !ConnectContext.get().getState().isQuery()
                    && ConnectContext.get().getSessionVariable() != null
                    && expr.desc != null) {
                return expr.getLabel() + "[#" + expr.desc.getId().asInt() + "]";
            } else {
                return expr.getLabel();
            }
        } else if (expr.desc == null) {
            return "`" + expr.getCol() + "`";
        } else if (expr.desc.getSourceExprs() != null) {
            if (!context.disableTableName) {
                if (expr.desc.getId().asInt() != 1) {
                    sb.append("<slot ").append(expr.desc.getId().asInt()).append(">");
                }
            }
            for (Expr srcExpr : expr.desc.getSourceExprs()) {
                if (!context.disableTableName) {
                    sb.append(" ");
                }
                sb.append(context.disableTableName
                        ? srcExpr.accept(INSTANCE, ToSqlParams.WITHOUT_TABLE)
                        : srcExpr.accept(this, context));
            }
            return sb.toString();
        } else {
            return "<slot " + expr.desc.getId().asInt() + ">" + sb;
        }
    }

    @Override
    public String visitColumnRefExpr(ColumnRefExpr expr, ToSqlParams context) {
        return expr.getName();
    }

    @Override
    public String visitInformationFunction(InformationFunction expr, ToSqlParams context) {
        return expr.getFuncType() + "()";
    }

    @Override
    public String visitEncryptKeyRef(EncryptKeyRef expr, ToSqlParams context) {
        return expr.getEncryptKeyName().toSql();
    }

    @Override
    public String visitVariableExpr(VariableExpr expr, ToSqlParams context) {
        StringBuilder sb = new StringBuilder();
        if (expr.getSetType() == SetType.USER) {
            sb.append("@");
        } else {
            sb.append("@@");
            if (expr.getSetType() == SetType.GLOBAL) {
                sb.append("GLOBAL.");
            }
        }
        sb.append(expr.getName());
        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // Predicates
    // -----------------------------------------------------------------------

    @Override
    public String visitBinaryPredicate(BinaryPredicate expr, ToSqlParams context) {
        return "(" + expr.getChild(0).accept(this, context)
                + " " + expr.getOp().toString()
                + " " + expr.getChild(1).accept(this, context) + ")";
    }

    @Override
    public String visitIsNullPredicate(IsNullPredicate expr, ToSqlParams context) {
        return expr.getChild(0).accept(this, context) + (expr.isNotNull() ? " IS NOT NULL" : " IS NULL");
    }

    @Override
    public String visitCompoundPredicate(CompoundPredicate expr, ToSqlParams context) {
        if (expr.getChildren().size() == 1) {
            Preconditions.checkState(expr.getOp() == CompoundPredicate.Operator.NOT);
            return "(NOT " + expr.getChild(0).accept(this, context) + ")";
        } else {
            return "(" + expr.getChild(0).accept(this, context)
                    + " " + expr.getOp().toString()
                    + " " + expr.getChild(1).accept(this, context) + ")";
        }
    }

    @Override
    public String visitInPredicate(InPredicate expr, ToSqlParams context) {
        StringBuilder strBuilder = new StringBuilder();
        String notStr = expr.isNotIn() ? "NOT " : "";
        strBuilder.append(expr.getChild(0).accept(this, context) + " " + notStr + "IN (");
        for (int i = 1; i < expr.getChildren().size(); ++i) {
            strBuilder.append(expr.getChild(i).accept(this, context));
            strBuilder.append((i + 1 != expr.getChildren().size()) ? ", " : "");
        }
        strBuilder.append(")");
        return strBuilder.toString();
    }

    @Override
    public String visitLikePredicate(LikePredicate expr, ToSqlParams context) {
        return expr.getChild(0).accept(this, context)
                + " " + expr.op.toString()
                + " " + expr.getChild(1).accept(this, context);
    }

    @Override
    public String visitMatchPredicate(MatchPredicate expr, ToSqlParams context) {
        return expr.getChild(0).accept(this, context)
                + " " + expr.getOp().toString()
                + " " + expr.getChild(1).accept(this, context)
                + expr.analyzerSqlFragment();
    }

    @Override
    public String visitBetweenPredicate(BetweenPredicate expr, ToSqlParams context) {
        String notStr = expr.isNotBetween() ? "NOT " : "";
        return expr.getChild(0).accept(this, context) + " " + notStr + "BETWEEN "
                + expr.getChild(1).accept(this, context) + " AND " + expr.getChild(2).accept(this, context);
    }

    @Override
    public String visitSearchPredicate(SearchPredicate expr, ToSqlParams context) {
        if (!isExplainVerboseContext()) {
            return "search('" + expr.getDslString() + "')";
        }

        StringBuilder sb = new StringBuilder("search('" + expr.getDslString() + "')");

        List<String> astLines = ExprToThriftVisitor.buildDslAstExplainLines(expr);
        if (!astLines.isEmpty()) {
            sb.append("\n|      dsl_ast:");
            for (String line : astLines) {
                sb.append("\n|        ").append(line);
            }
        }

        List<String> bindings = buildFieldBindingExplainLines(expr);
        if (!bindings.isEmpty()) {
            sb.append("\n|      field_bindings:");
            for (String binding : bindings) {
                sb.append("\n|        ").append(binding);
            }
        }

        return sb.toString();
    }

    // -----------------------------------------------------------------------
    // Arithmetic / cast
    // -----------------------------------------------------------------------

    @Override
    public String visitArithmeticExpr(ArithmeticExpr expr, ToSqlParams context) {
        if (expr.getChildren().size() == 1) {
            return expr.op.toString() + " " + expr.getChild(0).accept(this, context);
        } else {
            return "(" + expr.getChild(0).accept(this, context)
                    + " " + expr.op.toString()
                    + " " + expr.getChild(1).accept(this, context) + ")";
        }
    }

    @Override
    public String visitCastExpr(CastExpr expr, ToSqlParams context) {
        return "CAST(" + expr.getChild(0).accept(this, context) + " AS " + expr.getType().toSql() + ")";
    }

    @Override
    public String visitTryCastExpr(TryCastExpr expr, ToSqlParams context) {
        return "TRY_CAST(" + expr.getChild(0).accept(this, context) + " AS " + expr.getType().toSql() + ")";
    }

    @Override
    public String visitTimestampArithmeticExpr(TimestampArithmeticExpr expr, ToSqlParams context) {
        StringBuilder strBuilder = new StringBuilder();
        if (expr.getFuncName() != null) {
            if (expr.getFuncName().equalsIgnoreCase("TIMESTAMPDIFF")
                    || expr.getFuncName().equalsIgnoreCase("TIMESTAMPADD")) {
                strBuilder.append(expr.getFuncName()).append("(");
                strBuilder.append(expr.getTimeUnitIdent()).append(", ");
                strBuilder.append(expr.getChild(1).accept(this, context)).append(", ");
                strBuilder.append(expr.getChild(0).accept(this, context)).append(")");
                return strBuilder.toString();
            }
            strBuilder.append(expr.getFuncName()).append("(");
            strBuilder.append(expr.getChild(0).accept(this, context)).append(", ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(expr.getChild(1).accept(this, context));
            strBuilder.append(" ").append(expr.getTimeUnitIdent());
            strBuilder.append(")");
            return strBuilder.toString();
        }
        if (expr.isIntervalFirst()) {
            strBuilder.append("INTERVAL ");
            strBuilder.append(expr.getChild(1).accept(this, context) + " ");
            strBuilder.append(expr.getTimeUnitIdent());
            strBuilder.append(" ").append(expr.getOp().toString()).append(" ");
            strBuilder.append(expr.getChild(0).accept(this, context));
        } else {
            strBuilder.append(expr.getChild(0).accept(this, context));
            strBuilder.append(" " + expr.getOp().toString() + " ");
            strBuilder.append("INTERVAL ");
            strBuilder.append(expr.getChild(1).accept(this, context) + " ");
            strBuilder.append(expr.getTimeUnitIdent());
        }
        return strBuilder.toString();
    }

    // -----------------------------------------------------------------------
    // Functions / lambda / case
    // -----------------------------------------------------------------------

    @Override
    public String visitFunctionCallExpr(FunctionCallExpr expr, ToSqlParams context) {
        StringBuilder sb = new StringBuilder();

        if (expr.getFnName().getFunction().equalsIgnoreCase("like")
                || expr.getFnName().getFunction().equalsIgnoreCase("regexp")) {
            sb.append(expr.getChild(0).accept(this, context));
            sb.append(" ");
            sb.append(expr.getFnName());
            sb.append(" ");
            sb.append(expr.getChild(1).accept(this, context));
        } else if (expr.getFnName().getFunction().equalsIgnoreCase("encryptkeyref")) {
            sb.append("key ");
            for (int i = 0; i < expr.getChildren().size(); i++) {
                String str = ((StringLiteral) expr.getChild(i)).getValue();
                if (str.isEmpty()) {
                    continue;
                }
                sb.append(str);
                sb.append(".");
            }
            sb.deleteCharAt(sb.length() - 1);
        } else {
            sb.append(expr.getFnName());
            // inline params
            StringBuilder params = new StringBuilder();
            params.append("(");
            FunctionParams fnParams = expr.getFnParams();
            if (fnParams.isStar()) {
                params.append("*");
            }
            if (fnParams.isDistinct()) {
                params.append("DISTINCT ");
            }
            int len = expr.getChildren().size();
            List<OrderByElement> orderByElements = expr.getOrderByElements();
            if (expr.getFnName().getFunction().equalsIgnoreCase("char")) {
                for (int i = 1; i < len; ++i) {
                    params.append(expr.getChild(i).accept(this, context));
                    if (i < len - 1) {
                        params.append(", ");
                    }
                }
                params.append(" using ");
                String encodeType = expr.getChild(0).accept(this, context);
                if (encodeType.charAt(0) == '\'') {
                    encodeType = encodeType.substring(1, encodeType.length());
                }
                if (encodeType.charAt(encodeType.length() - 1) == '\'') {
                    encodeType = encodeType.substring(0, encodeType.length() - 1);
                }
                params.append(encodeType).append(")");
            } else if (expr.getFnName().getFunction().equalsIgnoreCase("years_diff")
                    || expr.getFnName().getFunction().equalsIgnoreCase("months_diff")
                    || expr.getFnName().getFunction().equalsIgnoreCase("days_diff")
                    || expr.getFnName().getFunction().equalsIgnoreCase("hours_diff")
                    || expr.getFnName().getFunction().equalsIgnoreCase("minutes_diff")
                    || expr.getFnName().getFunction().equalsIgnoreCase("seconds_diff")
                    || expr.getFnName().getFunction().equalsIgnoreCase("milliseconds_diff")
                    || expr.getFnName().getFunction().equalsIgnoreCase("microseconds_diff")) {
                // XXX_diff are used by nereids only
                params.append(expr.getChild(0).accept(this, context)).append(", ");
                params.append(expr.getChild(1).accept(this, context)).append(")");
            } else {
                for (int i = 0; i < len; ++i) {
                    if (i != 0) {
                        if (expr.getFnName().getFunction().equalsIgnoreCase("group_concat")
                                && orderByElements.size() > 0 && i == len - orderByElements.size()) {
                            params.append(" ");
                        } else {
                            params.append(", ");
                        }
                    }
                    if (ConnectContext.get() != null && ConnectContext.get().getState().isQuery() && i == 1
                            && (expr.getFnName().getFunction().equalsIgnoreCase("aes_decrypt")
                                    || expr.getFnName().getFunction().equalsIgnoreCase("aes_encrypt")
                                    || expr.getFnName().getFunction().equalsIgnoreCase("sm4_decrypt")
                                    || expr.getFnName().getFunction().equalsIgnoreCase("sm4_encrypt"))) {
                        params.append("\'***\'");
                        continue;
                    } else if (orderByElements.size() > 0 && i == len - orderByElements.size()) {
                        params.append("ORDER BY ");
                    }
                    params.append(expr.getChild(i).accept(this, context));
                    if (orderByElements.size() > 0 && i >= len - orderByElements.size()) {
                        if (orderByElements.get(i - len + orderByElements.size()).getIsAsc()) {
                            params.append(" ASC");
                        } else {
                            params.append(" DESC");
                        }
                    }
                }
                params.append(")");
            }
            sb.append(params);
            if (expr.getFnName().getFunction().equalsIgnoreCase("json_quote")
                    || expr.getFnName().getFunction().equalsIgnoreCase("json_array")
                    || expr.getFnName().getFunction().equalsIgnoreCase("json_object")
                    || expr.getFnName().getFunction().equalsIgnoreCase("json_insert")
                    || expr.getFnName().getFunction().equalsIgnoreCase("json_replace")
                    || expr.getFnName().getFunction().equalsIgnoreCase("json_set")) {
                return expr.forJSON(sb.toString());
            }
        }
        return sb.toString();
    }

    @Override
    public String visitLambdaFunctionCallExpr(LambdaFunctionCallExpr expr, ToSqlParams context) {
        StringBuilder sb = new StringBuilder();
        String fnName = expr.getFnName().getFunction();
        if (expr.fn != null) {
            fnName = expr.fn.getFunctionName().getFunction();
        }
        sb.append(fnName);
        sb.append("(");
        int childSize = expr.getChildren().size();
        Expr lastExpr = expr.getChild(childSize - 1);
        boolean lastIsLambdaExpr = (lastExpr instanceof LambdaFunctionExpr);
        if (lastIsLambdaExpr) {
            sb.append(lastExpr.accept(this, context));
            sb.append(", ");
        }
        for (int i = 0; i < childSize - 1; ++i) {
            sb.append(expr.getChild(i).accept(this, context));
            if (i != childSize - 2) {
                sb.append(", ");
            }
        }
        if (!lastIsLambdaExpr) {
            if (childSize > 1) {
                sb.append(", ");
            }
            sb.append(lastExpr.accept(this, context));
        }
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String visitLambdaFunctionExpr(LambdaFunctionExpr expr, ToSqlParams context) {
        String nameStr = "";
        Expr lambdaExpr = expr.getSlotExprs().get(0);
        int exprSize = expr.getNames().size();
        for (int i = 0; i < exprSize; ++i) {
            nameStr = nameStr + expr.getNames().get(i);
            if (i != exprSize - 1) {
                nameStr = nameStr + ",";
            }
        }
        if (exprSize > 1) {
            nameStr = "(" + nameStr + ")";
        }
        return String.format("%s -> %s", nameStr, lambdaExpr.accept(this, context));
    }

    @Override
    public String visitCaseExpr(CaseExpr expr, ToSqlParams context) {
        StringBuilder output = new StringBuilder("CASE");
        int childIdx = 0;
        if (expr.isHasCaseExpr()) {
            output.append(' ').append(expr.getChild(childIdx++).accept(this, context));
        }
        while (childIdx + 2 <= expr.getChildren().size()) {
            output.append(" WHEN " + expr.getChild(childIdx++).accept(this, context));
            output.append(" THEN " + expr.getChild(childIdx++).accept(this, context));
        }
        if (expr.isHasElseExpr()) {
            output.append(" ELSE " + expr.getChild(expr.getChildren().size() - 1).accept(this, context));
        }
        output.append(" END");
        return output.toString();
    }

    private static boolean isExplainVerboseContext() {
        ConnectContext ctx = ConnectContext.get();
        if (ctx == null) {
            return false;
        }
        StmtExecutor executor = ctx.getExecutor();
        if (executor == null || executor.getParsedStmt() == null
                || executor.getParsedStmt().getExplainOptions() == null) {
            return false;
        }
        return executor.getParsedStmt().getExplainOptions().isVerbose();
    }

    private static List<String> buildFieldBindingExplainLines(SearchPredicate expr) {
        List<String> lines = new ArrayList<>();
        if (expr.getQsPlan() == null || expr.getQsPlan().getFieldBindings() == null
                || expr.getQsPlan().getFieldBindings().isEmpty()) {
            return lines;
        }
        IntStream.range(0, expr.getQsPlan().getFieldBindings().size()).forEach(index -> {
            SearchDslParser.QsFieldBinding binding = expr.getQsPlan().getFieldBindings().get(index);
            String slotDesc = "<unbound>";
            if (index < expr.getChildren().size() && expr.getChildren().get(index) instanceof SlotRef) {
                SlotRef slotRef = (SlotRef) expr.getChildren().get(index);
                slotDesc = slotRef.getSlotId() != null
                        ? "slot=" + slotRef.getSlotId().asInt()
                        : slotRef.accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
            } else if (index < expr.getChildren().size()) {
                slotDesc = expr.getChildren().get(index).accept(ExprToSqlVisitor.INSTANCE, ToSqlParams.WITHOUT_TABLE);
            }
            lines.add(binding.getFieldName() + " -> " + slotDesc);
        });
        return lines;
    }
}
