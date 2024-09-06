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

package org.apache.doris.nereids.rules.expression.rules;

import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.ExprId;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.Config;
import org.apache.doris.common.IdGenerator;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.common.util.TimeUtils;
import org.apache.doris.nereids.glue.translator.ExpressionTranslator;
import org.apache.doris.nereids.rules.expression.ExpressionMatchingContext;
import org.apache.doris.nereids.rules.expression.ExpressionPatternMatcher;
import org.apache.doris.nereids.rules.expression.ExpressionPatternRuleFactory;
import org.apache.doris.nereids.trees.expressions.Alias;
import org.apache.doris.nereids.trees.expressions.ArrayItemReference;
import org.apache.doris.nereids.trees.expressions.Cast;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.Match;
import org.apache.doris.nereids.trees.expressions.functions.BoundFunction;
import org.apache.doris.nereids.trees.expressions.functions.generator.TableGeneratingFunction;
import org.apache.doris.nereids.trees.expressions.functions.scalar.NonNullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Nullable;
import org.apache.doris.nereids.trees.expressions.functions.scalar.Sleep;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BigIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.BooleanLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DateV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.DecimalLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DecimalV3Literal;
import org.apache.doris.nereids.trees.expressions.literal.DoubleLiteral;
import org.apache.doris.nereids.trees.expressions.literal.FloatLiteral;
import org.apache.doris.nereids.trees.expressions.literal.IPv4Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.JsonLiteral;
import org.apache.doris.nereids.trees.expressions.literal.LargeIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.literal.MapLiteral;
import org.apache.doris.nereids.trees.expressions.literal.NullLiteral;
import org.apache.doris.nereids.trees.expressions.literal.SmallIntLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StringLiteral;
import org.apache.doris.nereids.trees.expressions.literal.StructLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TinyIntLiteral;
import org.apache.doris.nereids.types.ArrayType;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.DateTimeV2Type;
import org.apache.doris.nereids.types.DecimalV3Type;
import org.apache.doris.nereids.types.MapType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.proto.InternalService;
import org.apache.doris.proto.InternalService.PConstantExprResult;
import org.apache.doris.proto.Types.PScalarType;
import org.apache.doris.proto.Types.PStructField;
import org.apache.doris.proto.Types.PTypeDesc;
import org.apache.doris.proto.Types.PTypeNode;
import org.apache.doris.proto.Types.PValues;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.rpc.BackendServiceProxy;
import org.apache.doris.system.Backend;
import org.apache.doris.thrift.TExpr;
import org.apache.doris.thrift.TFoldConstantParams;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPrimitiveType;
import org.apache.doris.thrift.TQueryGlobals;
import org.apache.doris.thrift.TQueryOptions;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.net.InetAddresses;
import com.google.protobuf.ByteString;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.Inet4Address;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Constant evaluation of an expression.
 */
public class FoldConstantRuleOnBE implements ExpressionPatternRuleFactory {

    public static final FoldConstantRuleOnBE INSTANCE = new FoldConstantRuleOnBE();
    private static final Logger LOG = LogManager.getLogger(FoldConstantRuleOnBE.class);

    @Override
    public List<ExpressionPatternMatcher<? extends Expression>> buildRules() {
        return ImmutableList.of(
                root(Expression.class)
                        .whenCtx(ctx -> !ctx.cascadesContext.getConnectContext().getSessionVariable()
                                .isDebugSkipFoldConstant())
                        .whenCtx(FoldConstantRuleOnBE::isEnableFoldByBe)
                        .thenApply(FoldConstantRuleOnBE::foldByBE)
        );
    }

    public static boolean isEnableFoldByBe(ExpressionMatchingContext<Expression> ctx) {
        return ctx.cascadesContext != null
                && ctx.cascadesContext.getConnectContext() != null
                && ctx.cascadesContext.getConnectContext().getSessionVariable().isEnableFoldConstantByBe();
    }

    /** foldByBE */
    public static Expression foldByBE(ExpressionMatchingContext<Expression> context) {
        IdGenerator<ExprId> idGenerator = ExprId.createGenerator();

        Expression root = context.expr;
        Map<String, Expression> constMap = Maps.newHashMap();
        Map<String, TExpr> staleConstTExprMap = Maps.newHashMap();
        Expression rootWithoutAlias = root;
        if (root instanceof Alias) {
            rootWithoutAlias = ((Alias) root).child();
        }

        collectConst(rootWithoutAlias, constMap, staleConstTExprMap, idGenerator);
        if (constMap.isEmpty()) {
            return root;
        }
        Map<String, Map<String, TExpr>> paramMap = new HashMap<>();
        paramMap.put("0", staleConstTExprMap);
        Map<String, Expression> resultMap = evalOnBE(paramMap, constMap, context.cascadesContext.getConnectContext());
        if (!resultMap.isEmpty()) {
            return replace(root, constMap, resultMap);
        }
        return root;
    }

    private static Expression replace(
            Expression root, Map<String, Expression> constMap, Map<String, Expression> resultMap) {
        for (Entry<String, Expression> entry : constMap.entrySet()) {
            if (entry.getValue().equals(root)) {
                if (resultMap.containsKey(entry.getKey())) {
                    return resultMap.get(entry.getKey());
                } else {
                    return root;
                }
            }
        }
        List<Expression> newChildren = new ArrayList<>();
        boolean hasNewChildren = false;
        for (Expression child : root.children()) {
            Expression newChild = replace(child, constMap, resultMap);
            if (newChild != child) {
                hasNewChildren = true;
            }
            newChildren.add(newChild);
        }
        return hasNewChildren ? root.withChildren(newChildren) : root;
    }

    private static void collectConst(Expression expr, Map<String, Expression> constMap,
            Map<String, TExpr> tExprMap, IdGenerator<ExprId> idGenerator) {
        if (expr.isConstant() && !shouldSkipFold(expr)) {
            String id = idGenerator.getNextId().toString();
            constMap.put(id, expr);
            Expr staleExpr;
            try {
                staleExpr = ExpressionTranslator.translate(expr, null);
            } catch (Exception e) {
                LOG.warn("expression {} translate to legacy expr failed. ", expr, e);
                return;
            }
            if (staleExpr == null) {
                // just return, it's a fail-safe
                LOG.warn("expression {} translate to legacy expr failed. ", expr);
                return;
            }
            tExprMap.put(id, staleExpr.treeToThrift());
        } else {
            for (int i = 0; i < expr.children().size(); i++) {
                final Expression child = expr.children().get(i);
                collectConst(child, constMap, tExprMap, idGenerator);
            }
        }
    }

    // Some expressions should not do constant folding
    private static boolean shouldSkipFold(Expression expr) {
        // Skip literal expr
        if (expr.isLiteral()) {
            return true;
        }

        // Frontend can not represent those types
        if (expr.getDataType().isAggStateType() || expr.getDataType().isObjectType()
                || expr.getDataType().isVariantType() || expr.getDataType().isTimeLikeType()
                || expr.getDataType().isIPv6Type()) {
            return true;
        }

        // Frontend can not represent geo types
        if (expr instanceof BoundFunction && ((BoundFunction) expr).getName().toLowerCase().startsWith("st_")) {
            return true;
        }

        // TableGeneratingFunction need pass PlanTranslatorContext value
        if (expr instanceof TableGeneratingFunction) {
            return true;
        }

        // ArrayItemReference translate can't findColumnRef
        if (expr instanceof ArrayItemReference) {
            return true;
        }

        // Match need give more info rather then as left child a NULL, in
        // match_phrase_prefix/MATCH_PHRASE/MATCH_PHRASE/MATCH_ANY
        if (expr instanceof Match) {
            return true;
        }

        // sleep will cause rpc timeout
        if (expr instanceof Sleep) {
            return true;
        }

        // Do not constant fold cast(null as dataType) because we cannot preserve the
        // cast-to-types and that can lead to query failures, e.g., CTAS
        if (expr instanceof Cast && ((Cast) expr).child().isNullLiteral()) {
            return true;
        }

        // This kind of function is often used to change the attributes of columns.
        // Folding will make it impossible to construct columns such as nullable(1).
        if (expr instanceof Nullable || expr instanceof NonNullable) {
            return true;
        }

        return false;
    }

    private static Map<String, Expression> evalOnBE(Map<String, Map<String, TExpr>> paramMap,
            Map<String, Expression> constMap, ConnectContext context) {

        Map<String, Expression> resultMap = new HashMap<>();
        try {
            List<Long> backendIds = Env.getCurrentSystemInfo().getAllBackendIds(true);
            if (backendIds.isEmpty()) {
                throw new UserException("No alive backends");
            }
            Collections.shuffle(backendIds);
            Backend be = Env.getCurrentSystemInfo().getBackend(backendIds.get(0));
            TNetworkAddress brpcAddress = new TNetworkAddress(be.getHost(), be.getBrpcPort());

            TQueryGlobals queryGlobals = new TQueryGlobals();
            queryGlobals.setNowString(TimeUtils.getDatetimeFormatWithTimeZone().format(LocalDateTime.now()));
            queryGlobals.setTimestampMs(System.currentTimeMillis());
            queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            if (context.getSessionVariable().getTimeZone().equals("CST")) {
                queryGlobals.setTimeZone(TimeUtils.DEFAULT_TIME_ZONE);
            } else {
                queryGlobals.setTimeZone(context.getSessionVariable().getTimeZone());
            }

            TQueryOptions tQueryOptions = new TQueryOptions();
            tQueryOptions.setBeExecVersion(Config.be_exec_version);

            TFoldConstantParams tParams = new TFoldConstantParams(paramMap, queryGlobals);
            tParams.setVecExec(true);
            tParams.setQueryOptions(tQueryOptions);
            tParams.setQueryId(context.queryId());
            tParams.setIsNereids(true);

            Future<PConstantExprResult> future = BackendServiceProxy.getInstance().foldConstantExpr(brpcAddress,
                    tParams);
            PConstantExprResult result = future.get(5, TimeUnit.SECONDS);

            if (result.getStatus().getStatusCode() == 0) {
                for (Entry<String, InternalService.PExprResultMap> e : result.getExprResultMapMap().entrySet()) {
                    for (Entry<String, InternalService.PExprResult> e1 : e.getValue().getMapMap().entrySet()) {
                        Expression ret;
                        if (e1.getValue().getSuccess()) {
                            PTypeDesc pTypeDesc = e1.getValue().getTypeDesc();
                            DataType type = convertToNereidsType(pTypeDesc.getTypesList(), 0).key();
                            PValues resultContent = e1.getValue().getResultContent();
                            List<Literal> resultExpression = getResultExpression(type, resultContent);
                            if (resultExpression.isEmpty()) {
                                ret = constMap.get(e1.getKey());
                                LOG.warn("Be constant folding convert {} to {} failed query_id: {}", e1.getKey(), ret,
                                        DebugUtil.printId(context.queryId()));
                            } else {
                                ret = resultExpression.get(0);
                            }
                        } else {
                            ret = constMap.get(e1.getKey());
                        }
                        if (LOG.isDebugEnabled()) {
                            LOG.debug("Be constant folding convert {} to {}", e1.getKey(), ret);
                        }
                        resultMap.put(e1.getKey(), ret);
                    }
                }

            } else {
                LOG.warn("query {} failed to get const expr value from be: {}",
                        DebugUtil.printId(context.queryId()), result.getStatus().getErrorMsgsList());
            }
        } catch (Exception e) {
            LOG.warn("query {} failed to get const expr value from be: {}",
                    DebugUtil.printId(context.queryId()), e.getMessage());
        }
        return resultMap;
    }

    /**
     * convert PValues which from BE to Expression of nereids
     */
    public static List<Literal> getResultExpression(DataType type, PValues resultContent) {
        List<Literal> res = new ArrayList<>();
        if (type.isNullType()) {
            int num = resultContent.getNullMapCount();
            for (int i = 0; i < num; ++i) {
                res.add(new NullLiteral(type));
            }
        } else if (type.isBooleanType()) {
            int num = resultContent.getUint32ValueCount();
            for (int i = 0; i < num; ++i) {
                Literal literal = BooleanLiteral.of(resultContent.getUint32Value(i) != 0);
                res.add(literal);
            }
        } else if (type.isTinyIntType()) {
            int num = resultContent.getInt32ValueCount();
            for (int i = 0; i < num; ++i) {
                Literal literal = new TinyIntLiteral((byte) resultContent.getInt32Value(i));
                res.add(literal);
            }
        } else if (type.isSmallIntType()) {
            int num = resultContent.getInt32ValueCount();
            for (int i = 0; i < num; ++i) {
                Literal literal = new SmallIntLiteral((short) resultContent.getInt32Value(i));
                res.add(literal);
            }
        } else if (type.isIntegerType()) {
            int num = resultContent.getInt32ValueCount();
            for (int i = 0; i < num; ++i) {
                Literal literal = new IntegerLiteral(resultContent.getInt32Value(i));
                res.add(literal);
            }
        } else if (type.isBigIntType()) {
            int num = resultContent.getInt64ValueCount();
            for (int i = 0; i < num; ++i) {
                Literal literal = new BigIntLiteral(resultContent.getInt64Value(i));
                res.add(literal);
            }
        } else if (type.isLargeIntType()) {
            int num = resultContent.getBytesValueCount();
            for (int i = 0; i < num; ++i) {
                ByteString bytesValue = resultContent.getBytesValue(i);
                byte[] bytes = convertByteOrder(bytesValue.toByteArray());
                BigInteger convertedBigInteger = new BigInteger(bytes);
                Literal literal = new LargeIntLiteral(convertedBigInteger);
                res.add(literal);
            }
        } else if (type.isFloatType()) {
            int num = resultContent.getFloatValueCount();
            for (int i = 0; i < num; ++i) {
                float value = resultContent.getFloatValue(i);
                Literal literal = null;
                if (Float.isNaN(value)) {
                    literal = new NullLiteral(type);
                } else {
                    literal = new FloatLiteral(value);
                }
                res.add(literal);
            }
        } else if (type.isDoubleType()) {
            int num = resultContent.getDoubleValueCount();
            for (int i = 0; i < num; ++i) {
                double value = resultContent.getDoubleValue(i);
                Literal literal = null;
                if (Double.isNaN(value)) {
                    literal = new NullLiteral(type);
                } else {
                    literal = new DoubleLiteral(value);
                }
                res.add(literal);
            }
        } else if (type.isDecimalV2Type()) {
            int num = resultContent.getBytesValueCount();
            for (int i = 0; i < num; ++i) {
                ByteString bytesValue = resultContent.getBytesValue(i);
                byte[] bytes = convertByteOrder(bytesValue.toByteArray());
                BigInteger value = new BigInteger(bytes);
                BigDecimal bigDecimal = new BigDecimal(value, 9); // decimalv2 scale always 9
                Literal literal = new DecimalLiteral(bigDecimal);
                res.add(literal);
            }
        } else if (type.isDecimalV3Type()) {
            int num = resultContent.getBytesValueCount();
            DecimalV3Type decimalV3Type = (DecimalV3Type) type;
            for (int i = 0; i < num; ++i) {
                ByteString bytesValue = resultContent.getBytesValue(i);
                byte[] bytes = convertByteOrder(bytesValue.toByteArray());
                BigInteger value = new BigInteger(bytes);
                BigDecimal bigDecimal = new BigDecimal(value, decimalV3Type.getScale());
                Literal literal = new DecimalV3Literal(decimalV3Type, bigDecimal);
                res.add(literal);
            }
        } else if (type.isDateTimeV2Type()) {
            int num = resultContent.getUint64ValueCount();
            for (int i = 0; i < num; ++i) {
                long uint64Value = resultContent.getUint64Value(i);
                LocalDateTime dateTimeV2 = convertToJavaDateTimeV2(uint64Value);
                if (dateTimeV2 == null && resultContent.hasHasNull()) {
                    res.add(new NullLiteral(type));
                } else {
                    Literal literal = new DateTimeV2Literal((DateTimeV2Type) type, dateTimeV2.getYear(),
                            dateTimeV2.getMonthValue(), dateTimeV2.getDayOfMonth(), dateTimeV2.getHour(),
                            dateTimeV2.getMinute(), dateTimeV2.getSecond(), dateTimeV2.getNano() / 1000);
                    res.add(literal);
                }
            }
        } else if (type.isDateV2Type()) {
            int num = resultContent.getUint32ValueCount();
            for (int i = 0; i < num; ++i) {
                int uint32Value = resultContent.getUint32Value(i);
                LocalDate localDate = convertToJavaDateV2(uint32Value);
                if (localDate == null && resultContent.hasHasNull()) {
                    res.add(new NullLiteral(type));
                } else {
                    DateV2Literal dateV2Literal = new DateV2Literal(localDate.getYear(), localDate.getMonthValue(),
                            localDate.getDayOfMonth());
                    res.add(dateV2Literal);
                }
            }
        } else if (type.isIPv4Type()) {
            int num = resultContent.getUint32ValueCount();
            for (int i = 0; i < num; ++i) {
                Inet4Address inet4Address = InetAddresses.fromInteger(resultContent.getUint32Value(i));
                IPv4Literal iPv4Literal = new IPv4Literal(inet4Address.getHostAddress());
                res.add(iPv4Literal);
            }
        } else if (type.isJsonType()) {
            int num = resultContent.getStringValueCount();
            for (int i = 0; i < num; ++i) {
                String stringValue = resultContent.getStringValue(i);
                // maybe need handle NULL_IN_CSV_FOR_ORDINARY_TYPE = "\\N";
                if ("\\N".equalsIgnoreCase(stringValue) && resultContent.hasHasNull()) {
                    res.add(new NullLiteral(type));
                } else {
                    res.add(new JsonLiteral(stringValue));
                }
            }
        } else if (type.isStringLikeType()) {
            int num = resultContent.getStringValueCount();
            for (int i = 0; i < num; ++i) {
                Literal literal = new StringLiteral(resultContent.getStringValue(i));
                res.add(literal);
            }
        } else if (type.isArrayType()) {
            ArrayType arrayType = (ArrayType) type;
            int childCount = resultContent.getChildElementCount();
            List<Literal> allLiterals = new ArrayList<>();
            for (int i = 0; i < childCount; ++i) {
                allLiterals.addAll(getResultExpression(arrayType.getItemType(),
                        resultContent.getChildElement(i)));
            }
            int offsetCount = resultContent.getChildOffsetCount();
            if (offsetCount == 1) {
                ArrayLiteral arrayLiteral = new ArrayLiteral(allLiterals, arrayType);
                res.add(arrayLiteral);
            } else {
                for (int i = 0; i < offsetCount; ++i) {
                    List<Literal> childLiteral = new ArrayList<>();
                    int startOffset = (int) ((i == 0) ? 0 : resultContent.getChildOffset(i - 1));
                    int endOffset = (int) resultContent.getChildOffset(i);
                    for (int off = startOffset; off < endOffset; ++off) {
                        childLiteral.add(allLiterals.get(off));
                    }
                    ArrayLiteral arrayLiteral = new ArrayLiteral(childLiteral, arrayType);
                    res.add(arrayLiteral);
                }
            }
        } else if (type.isMapType()) {
            MapType mapType = (MapType) type;
            int childCount = resultContent.getChildElementCount();
            List<Literal> allKeys = new ArrayList<>();
            List<Literal> allValues = new ArrayList<>();
            for (int i = 0; i < childCount; i = i + 2) {
                allKeys.addAll(getResultExpression(mapType.getKeyType(),
                        resultContent.getChildElement(i)));
                allValues.addAll(getResultExpression(mapType.getValueType(),
                        resultContent.getChildElement(i + 1)));
            }
            int offsetCount = resultContent.getChildOffsetCount();
            if (offsetCount == 1) {
                MapLiteral mapLiteral = new MapLiteral(allKeys, allValues, mapType);
                res.add(mapLiteral);
            } else {
                for (int i = 0; i < offsetCount; ++i) {
                    List<Literal> keyLiteral = new ArrayList<>();
                    List<Literal> valueLiteral = new ArrayList<>();
                    int startOffset = (int) ((i == 0) ? 0 : resultContent.getChildOffset(i - 1));
                    int endOffset = (int) resultContent.getChildOffset(i);
                    for (int off = startOffset; off < endOffset; ++off) {
                        keyLiteral.add(allKeys.get(off));
                        valueLiteral.add(allValues.get(off));
                    }
                    MapLiteral mapLiteral = new MapLiteral(keyLiteral, valueLiteral, mapType);
                    res.add(mapLiteral);
                }
            }
        } else if (type.isStructType()) {
            StructType structType = (StructType) type;
            int childCount = resultContent.getChildElementCount();
            List<List<Literal>> allFields = new ArrayList<>();
            for (int i = 0; i < childCount; ++i) {
                allFields.add(getResultExpression(structType.getFields().get(i).getDataType(),
                        resultContent.getChildElement(i)));
            }
            for (int i = 0; i < allFields.get(0).size(); ++i) {
                List<Literal> fields = new ArrayList<>();
                for (int child = 0; child < childCount; ++child) {
                    fields.add(allFields.get(child).get(i));
                }
                StructLiteral structLiteral = new StructLiteral(fields, structType);
                res.add(structLiteral);
            }
        } else {
            LOG.warn("the type: {} is not support, should implement it", type.toString());
        }
        if (resultContent.hasHasNull()) {
            for (int i = 0; i < resultContent.getNullMapCount(); ++i) {
                if (resultContent.getNullMap(i)) {
                    res.set(i, new NullLiteral(type));
                }
            }
        }
        return res;
    }

    private static Pair<DataType, Integer> convertToNereidsType(List<PTypeNode> typeNodes, int start) {
        PScalarType pScalarType = typeNodes.get(start).getScalarType();
        boolean containsNull = typeNodes.get(start).getContainsNull();
        TPrimitiveType tPrimitiveType = TPrimitiveType.findByValue(pScalarType.getType());
        DataType type;
        int parsedNodes;
        if (tPrimitiveType == TPrimitiveType.ARRAY) {
            Pair<DataType, Integer> itemType = convertToNereidsType(typeNodes, start + 1);
            type = ArrayType.of(itemType.key(), containsNull);
            parsedNodes = 1 + itemType.value();
        } else if (tPrimitiveType == TPrimitiveType.MAP) {
            Pair<DataType, Integer> keyType = convertToNereidsType(typeNodes, start + 1);
            Pair<DataType, Integer> valueType = convertToNereidsType(typeNodes, start + 1 + keyType.value());
            type = MapType.of(keyType.key(), valueType.key());
            parsedNodes = 1 + keyType.value() + valueType.value();
        } else if (tPrimitiveType == TPrimitiveType.STRUCT) {
            parsedNodes = 1;
            ArrayList<StructField> fields = new ArrayList<>();
            for (int i = 0; i < typeNodes.get(start).getStructFieldsCount(); ++i) {
                Pair<DataType, Integer> fieldType = convertToNereidsType(typeNodes, start + parsedNodes);
                PStructField structField = typeNodes.get(start).getStructFields(i);
                fields.add(new StructField(structField.getName(), fieldType.key(),
                        structField.getContainsNull(),
                        structField.getComment() == null ? "" : structField.getComment()));
                parsedNodes += fieldType.value();
            }
            type = new StructType(fields);
        } else if (tPrimitiveType == TPrimitiveType.DECIMALV2) {
            type = DataType.fromCatalogType(ScalarType.createDecimalType(PrimitiveType.fromThrift(tPrimitiveType),
                    pScalarType.getPrecision(), pScalarType.getScale()));
            parsedNodes = 1;
        } else {
            type = DataType.fromCatalogType(ScalarType.createType(PrimitiveType.fromThrift(tPrimitiveType),
                    pScalarType.getLen(), pScalarType.getPrecision(), pScalarType.getScale()));
            parsedNodes = 1;
        }
        return Pair.of(type, parsedNodes);
    }

    private static LocalDateTime convertToJavaDateTimeV2(long time) {
        int year = (int) (time >> 46);
        int yearMonth = (int) (time >> 42);
        int yearMonthDay = (int) (time >> 37);

        int month = (yearMonth & 0XF);
        int day = (yearMonthDay & 0X1F);

        int hour = (int) ((time >> 32) & 0X1F);
        int minute = (int) ((time >> 26) & 0X3F);
        int second = (int) ((time >> 20) & 0X3F);
        int microsecond = (int) (time & 0XFFFFF);

        try {
            return LocalDateTime.of(year, month, day, hour, minute, second, microsecond * 1000);
        } catch (DateTimeException e) {
            return null;
        }
    }

    private static LocalDate convertToJavaDateV2(int date) {
        int year = date >> 9;
        int month = (date >> 5) & 0XF;
        int day = date & 0X1F;
        try {
            return LocalDate.of(year, month, day);
        } catch (DateTimeException e) {
            return null;
        }
    }

    /**
     * Change the order of the bytes, Because JVM is Big-Endian , x86 is Little-Endian.
     */
    private static byte[] convertByteOrder(byte[] bytes) {
        int length = bytes.length;
        for (int i = 0; i < length / 2; ++i) {
            byte temp = bytes[i];
            bytes[i] = bytes[length - 1 - i];
            bytes[length - 1 - i] = temp;
        }
        return bytes;
    }
}
