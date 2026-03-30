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

import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.FractionalFormat;
import org.apache.doris.foundation.format.FormatOptions;
import org.apache.doris.nereids.util.DateUtils;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * Visitor that generates string values for Expr instances, handling different
 * output modes (query, stream load) and complex type nesting.
 *
 * <p>This visitor extracts the logic previously in {@code getStringValueForQuery},
 * {@code getStringValueInComplexTypeForQuery}, and {@code getStringValueForStreamLoad}
 * from Expr subclasses, following the same visitor pattern as {@link ExprToSqlVisitor}.
 */
public class ExprToStringValueVisitor extends ExprVisitor<String, StringValueContext> {
    private static final Logger LOG = LogManager.getLogger(ExprToStringValueVisitor.class);

    public static final ExprToStringValueVisitor INSTANCE = new ExprToStringValueVisitor();

    @Override
    public String visit(Expr expr, StringValueContext ctx) {
        return expr.getStringValue();
    }

    @Override
    public String visitDateLiteral(DateLiteral expr, StringValueContext ctx) {
        String value;
        if (expr.getType().isTimeStampTz()) {
            try {
                ZoneId dorisZone = DateUtils.getTimeZone();
                String offset = dorisZone.getRules().getOffset(java.time.Instant.now()).toString();
                DateLiteral dateLiteral = new DateLiteral(expr.getStringValue(),
                        ScalarType.createDatetimeV2Type(((ScalarType) expr.getType()).getScalarScale()));
                value = dateLiteral.getStringValue() + offset;
            } catch (Exception e) {
                LOG.warn("generate timestamptz({})'s string value for query failed. ",
                        expr.getStringValue(), e);
                value = expr.getStringValue();
            }
        } else {
            value = expr.getStringValue();
        }
        if (ctx.isInComplexType()) {
            return wrapWithQuotes(value, ctx);
        }
        return value;
    }

    @Override
    public String visitFloatLiteral(FloatLiteral expr, StringValueContext ctx) {
        String value;
        if (expr.getType() == Type.TIMEV2) {
            String timeStr = expr.getStringValue();
            value = timeStr.substring(1, timeStr.length() - 1);
        } else {
            double dValue = expr.getValue();
            if (expr.getType() == Type.FLOAT) {
                Float fValue = (float) dValue;
                if (fValue.equals(Float.POSITIVE_INFINITY)) {
                    dValue = Double.POSITIVE_INFINITY;
                }
                if (fValue.equals(Float.NEGATIVE_INFINITY)) {
                    dValue = Double.NEGATIVE_INFINITY;
                }
            }
            value = FractionalFormat.getFormatStringValue(dValue,
                    expr.getType() == Type.DOUBLE ? 16 : 7,
                    expr.getType() == Type.DOUBLE ? "%.15E" : "%.6E");
        }
        if (ctx.isInComplexType() && expr.getType() == Type.TIMEV2) {
            return wrapWithQuotes(value, ctx);
        }
        return value;
    }

    @Override
    public String visitBoolLiteral(BoolLiteral expr, StringValueContext ctx) {
        FormatOptions options = ctx.getFormatOptions();
        if (options.level > 0) {
            return options.isBoolValueNum() ? expr.getStringValue() : (expr.getValue() ? "true" : "false");
        }
        return expr.getStringValue();
    }

    @Override
    public String visitNullLiteral(NullLiteral expr, StringValueContext ctx) {
        if (ctx.isInComplexType()) {
            return ctx.getFormatOptions().getNullFormat();
        }
        if (ctx.isForStreamLoad()) {
            return FeConstants.null_string;
        }
        return null;
    }

    @Override
    public String visitDecimalLiteral(DecimalLiteral expr, StringValueContext ctx) {
        return expr.getValue().toPlainString();
    }

    @Override
    public String visitArrayLiteral(ArrayLiteral expr, StringValueContext ctx) {
        FormatOptions options = ctx.getFormatOptions();
        List<String> list = new ArrayList<>(expr.getChildren().size());
        ++options.level;
        for (Expr child : expr.getChildren()) {
            list.add(child.accept(this, ctx.asComplexType()));
        }
        --options.level;
        return "[" + StringUtils.join(list, options.getCollectionDelim()) + "]";
    }

    @Override
    public String visitMapLiteral(MapLiteral expr, StringValueContext ctx) {
        FormatOptions options = ctx.getFormatOptions();
        List<Expr> children = expr.getChildren();
        List<String> list = new ArrayList<>(children.size());
        ++options.level;
        StringValueContext childCtx = ctx.asComplexType();
        for (int i = 0; i < children.size() && i + 1 < children.size(); i += 2) {
            if (children.get(i).getType().isComplexType()) {
                throw new UnsupportedOperationException(
                        "Unsupported key type for MAP: " + children.get(i).getType());
            }
            list.add(children.get(i).accept(this, childCtx)
                    + options.getMapKeyDelim()
                    + children.get(i + 1).accept(this, childCtx));
        }
        --options.level;
        return "{" + StringUtils.join(list, options.getCollectionDelim()) + "}";
    }

    @Override
    public String visitStructLiteral(StructLiteral expr, StringValueContext ctx) {
        FormatOptions options = ctx.getFormatOptions();
        List<Expr> children = expr.getChildren();
        List<String> list = new ArrayList<>(children.size());
        if (ctx.isForStreamLoad()) {
            for (int i = 0; i < children.size(); i++) {
                list.add(children.get(i).accept(this, ctx.asQueryComplexType()));
            }
        } else {
            ++options.level;
            StringValueContext childCtx = ctx.asComplexType();
            for (int i = 0; i < children.size(); i++) {
                list.add(options.getNestedStringWrapper()
                        + ((StructType) expr.getType()).getFields().get(i).getName()
                        + options.getNestedStringWrapper()
                        + options.getMapKeyDelim()
                        + children.get(i).accept(this, childCtx));
            }
            --options.level;
        }
        return "{" + StringUtils.join(list, options.getCollectionDelim()) + "}";
    }

    @Override
    public String visitCastExpr(CastExpr expr, StringValueContext ctx) {
        if (ctx.isInComplexType() || ctx.isForStreamLoad()) {
            return expr.getChildren().get(0).accept(this, ctx);
        }
        return expr.getStringValue();
    }

    @Override
    public String visitStringLiteral(StringLiteral expr, StringValueContext ctx) {
        if (ctx.isInComplexType()) {
            return wrapWithQuotes(expr.getStringValue(), ctx);
        }
        return expr.getStringValue();
    }

    @Override
    public String visitIPv4Literal(IPv4Literal expr, StringValueContext ctx) {
        if (ctx.isInComplexType()) {
            return wrapWithQuotes(expr.getStringValue(), ctx);
        }
        return expr.getStringValue();
    }

    @Override
    public String visitIPv6Literal(IPv6Literal expr, StringValueContext ctx) {
        if (ctx.isInComplexType()) {
            return wrapWithQuotes(expr.getStringValue(), ctx);
        }
        return expr.getStringValue();
    }

    @Override
    public String visitVarBinaryLiteral(VarBinaryLiteral expr, StringValueContext ctx) {
        if (ctx.isInComplexType()) {
            return wrapWithQuotes(expr.getStringValue(), ctx);
        }
        return expr.getStringValue();
    }

    @Override
    public String visitPlaceHolderExpr(PlaceHolderExpr expr, StringValueContext ctx) {
        if (ctx.isInComplexType()) {
            return wrapWithQuotes(expr.getStringValue(), ctx);
        }
        return expr.getStringValue();
    }

    @Override
    public String visitJsonLiteral(JsonLiteral expr, StringValueContext ctx) {
        if (ctx.isInComplexType()) {
            return null;
        }
        return expr.getStringValue();
    }

    @Override
    public String visitMaxLiteral(MaxLiteral expr, StringValueContext ctx) {
        if (ctx.isInComplexType()) {
            return null;
        }
        return expr.getStringValue();
    }

    private String wrapWithQuotes(String value, StringValueContext ctx) {
        String wrapper = ctx.getFormatOptions().getNestedStringWrapper();
        return wrapper + value + wrapper;
    }
}
