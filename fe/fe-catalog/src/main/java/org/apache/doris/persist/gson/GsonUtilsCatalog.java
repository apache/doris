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

package org.apache.doris.persist.gson;

import org.apache.doris.analysis.ArithmeticExpr;
import org.apache.doris.analysis.ArrayLiteral;
import org.apache.doris.analysis.BetweenPredicate;
import org.apache.doris.analysis.BinaryPredicate;
import org.apache.doris.analysis.BoolLiteral;
import org.apache.doris.analysis.CaseExpr;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.ColumnRefExpr;
import org.apache.doris.analysis.CompoundPredicate;
import org.apache.doris.analysis.DateLiteral;
import org.apache.doris.analysis.DecimalLiteral;
import org.apache.doris.analysis.EncryptKeyRef;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FloatLiteral;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.IPv4Literal;
import org.apache.doris.analysis.IPv6Literal;
import org.apache.doris.analysis.InPredicate;
import org.apache.doris.analysis.InformationFunction;
import org.apache.doris.analysis.IntLiteral;
import org.apache.doris.analysis.IsNullPredicate;
import org.apache.doris.analysis.JsonLiteral;
import org.apache.doris.analysis.LambdaFunctionCallExpr;
import org.apache.doris.analysis.LambdaFunctionExpr;
import org.apache.doris.analysis.LargeIntLiteral;
import org.apache.doris.analysis.LikePredicate;
import org.apache.doris.analysis.LiteralExpr;
import org.apache.doris.analysis.MapLiteral;
import org.apache.doris.analysis.MatchPredicate;
import org.apache.doris.analysis.MaxLiteral;
import org.apache.doris.analysis.NullLiteral;
import org.apache.doris.analysis.NumericLiteralExpr;
import org.apache.doris.analysis.PlaceHolderExpr;
import org.apache.doris.analysis.SearchPredicate;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.StringLiteral;
import org.apache.doris.analysis.StructLiteral;
import org.apache.doris.analysis.TimeV2Literal;
import org.apache.doris.analysis.TimestampArithmeticExpr;
import org.apache.doris.analysis.TryCastExpr;
import org.apache.doris.analysis.VarBinaryLiteral;
import org.apache.doris.analysis.VariableExpr;
import org.apache.doris.analysis.VirtualSlotRef;
import org.apache.doris.catalog.AggStateType;
import org.apache.doris.catalog.AggregateFunction;
import org.apache.doris.catalog.AliasFunction;
import org.apache.doris.catalog.AnyElementType;
import org.apache.doris.catalog.AnyStructType;
import org.apache.doris.catalog.AnyType;
import org.apache.doris.catalog.ArrayType;
import org.apache.doris.catalog.Function;
import org.apache.doris.catalog.MapType;
import org.apache.doris.catalog.ScalarFunction;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.StructType;
import org.apache.doris.catalog.TemplateType;
import org.apache.doris.catalog.VariantType;
import org.apache.doris.persist.gson.GsonUtilsBase.AtomicBooleanAdapter;
import org.apache.doris.persist.gson.GsonUtilsBase.GuavaMultimapAdapter;
import org.apache.doris.persist.gson.GsonUtilsBase.GuavaTableAdapter;
import org.apache.doris.persist.gson.GsonUtilsBase.HiddenAnnotationExclusionStrategy;
import org.apache.doris.persist.gson.GsonUtilsBase.ImmutableListDeserializer;
import org.apache.doris.persist.gson.GsonUtilsBase.ImmutableMapDeserializer;
import org.apache.doris.persist.gson.GsonUtilsBase.PostProcessTypeAdapterFactory;
import org.apache.doris.persist.gson.GsonUtilsBase.PreProcessTypeAdapterFactory;
import org.apache.doris.persist.gson.GsonUtilsBase.SkipClassExclusionStrategy;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Table;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ReflectionAccessFilter;
import com.google.gson.ToNumberPolicy;

import java.util.concurrent.atomic.AtomicBoolean;

public class GsonUtilsCatalog {

    // runtime adapter for class "Type"
    private static final RuntimeTypeAdapterFactory<org.apache.doris.catalog.Type> columnTypeAdapterFactory
            = RuntimeTypeAdapterFactory
            .of(org.apache.doris.catalog.Type.class, "clazz")
            // TODO: register other sub type after Doris support more types.
            .registerSubtype(ScalarType.class, ScalarType.class.getSimpleName())
            .registerSubtype(ArrayType.class, ArrayType.class.getSimpleName())
            .registerSubtype(MapType.class, MapType.class.getSimpleName())
            .registerSubtype(StructType.class, StructType.class.getSimpleName())
            .registerSubtype(AggStateType.class, AggStateType.class.getSimpleName())
            .registerSubtype(AnyElementType.class, AnyElementType.class.getSimpleName())
            .registerSubtype(AnyStructType.class, AnyStructType.class.getSimpleName())
            .registerSubtype(AnyType.class, AnyType.class.getSimpleName())
            .registerSubtype(TemplateType.class, TemplateType.class.getSimpleName())
            .registerSubtype(VariantType.class, VariantType.class.getSimpleName());

    // runtime adapter for class "Expr"
    private static final RuntimeTypeAdapterFactory<org.apache.doris.analysis.Expr> exprAdapterFactory
            = RuntimeTypeAdapterFactory
            .of(Expr.class, "clazz")
            .registerSubtype(ArithmeticExpr.class, ArithmeticExpr.class.getSimpleName())
            .registerSubtype(CaseExpr.class, CaseExpr.class.getSimpleName())
            .registerSubtype(CastExpr.class, CastExpr.class.getSimpleName())
            .registerSubtype(ColumnRefExpr.class, ColumnRefExpr.class.getSimpleName())
            .registerSubtype(TryCastExpr.class, TryCastExpr.class.getSimpleName())
            .registerSubtype(EncryptKeyRef.class, EncryptKeyRef.class.getSimpleName())
            .registerSubtype(FunctionCallExpr.class, FunctionCallExpr.class.getSimpleName())
            .registerSubtype(LambdaFunctionCallExpr.class, LambdaFunctionCallExpr.class.getSimpleName())
            .registerSubtype(InformationFunction.class, InformationFunction.class.getSimpleName())
            .registerSubtype(LambdaFunctionExpr.class, LambdaFunctionExpr.class.getSimpleName())
            .registerSubtype(LiteralExpr.class, LiteralExpr.class.getSimpleName())
            .registerSubtype(ArrayLiteral.class, ArrayLiteral.class.getSimpleName())
            .registerSubtype(BoolLiteral.class, BoolLiteral.class.getSimpleName())
            .registerSubtype(DateLiteral.class, DateLiteral.class.getSimpleName())
            .registerSubtype(IPv4Literal.class, IPv4Literal.class.getSimpleName())
            .registerSubtype(IPv6Literal.class, IPv6Literal.class.getSimpleName())
            .registerSubtype(JsonLiteral.class, JsonLiteral.class.getSimpleName())
            .registerSubtype(MapLiteral.class, MapLiteral.class.getSimpleName())
            .registerSubtype(MaxLiteral.class, MaxLiteral.class.getSimpleName())
            .registerSubtype(NullLiteral.class, NullLiteral.class.getSimpleName())
            .registerSubtype(NumericLiteralExpr.class, NumericLiteralExpr.class.getSimpleName())
            .registerSubtype(DecimalLiteral.class, DecimalLiteral.class.getSimpleName())
            .registerSubtype(FloatLiteral.class, FloatLiteral.class.getSimpleName())
            .registerSubtype(IntLiteral.class, IntLiteral.class.getSimpleName())
            .registerSubtype(LargeIntLiteral.class, LargeIntLiteral.class.getSimpleName())
            .registerSubtype(PlaceHolderExpr.class, PlaceHolderExpr.class.getSimpleName())
            .registerSubtype(StringLiteral.class, StringLiteral.class.getSimpleName())
            .registerSubtype(StructLiteral.class, StructLiteral.class.getSimpleName())
            .registerSubtype(TimeV2Literal.class, TimeV2Literal.class.getSimpleName())
            .registerSubtype(VarBinaryLiteral.class, VarBinaryLiteral.class.getSimpleName())
            .registerSubtype(BetweenPredicate.class, BetweenPredicate.class.getSimpleName())
            .registerSubtype(BinaryPredicate.class, BinaryPredicate.class.getSimpleName())
            .registerSubtype(CompoundPredicate.class, CompoundPredicate.class.getSimpleName())
            .registerSubtype(InPredicate.class, InPredicate.class.getSimpleName())
            .registerSubtype(IsNullPredicate.class, IsNullPredicate.class.getSimpleName())
            .registerSubtype(LikePredicate.class, LikePredicate.class.getSimpleName())
            .registerSubtype(MatchPredicate.class, MatchPredicate.class.getSimpleName())
            .registerSubtype(SearchPredicate.class, SearchPredicate.class.getSimpleName())
            .registerSubtype(SlotRef.class, SlotRef.class.getSimpleName())
            .registerSubtype(VirtualSlotRef.class, VirtualSlotRef.class.getSimpleName())
            .registerSubtype(TimestampArithmeticExpr.class, TimestampArithmeticExpr.class.getSimpleName())
            .registerSubtype(VariableExpr.class, VariableExpr.class.getSimpleName());

    // runtime adapter for class "Function"
    private static final RuntimeTypeAdapterFactory<Function> functionAdapterFactory
            = RuntimeTypeAdapterFactory.of(Function.class, "clazz")
            .registerSubtype(ScalarFunction.class, ScalarFunction.class.getSimpleName())
            .registerSubtype(AggregateFunction.class, AggregateFunction.class.getSimpleName())
            .registerSubtype(AliasFunction.class, AliasFunction.class.getSimpleName());

    private static final GsonBuilder GSON_BUILDER = new GsonBuilder()
            .setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
            .setExclusionStrategies(new SkipClassExclusionStrategy())
            .addSerializationExclusionStrategy(new HiddenAnnotationExclusionStrategy())
            .serializeSpecialFloatingPointValues()
            .enableComplexMapKeySerialization()
            .addReflectionAccessFilter(ReflectionAccessFilter.BLOCK_INACCESSIBLE_JAVA)
            .registerTypeHierarchyAdapter(Table.class, new GuavaTableAdapter<>())
            .registerTypeHierarchyAdapter(Multimap.class, new GuavaMultimapAdapter<>())
            .registerTypeAdapterFactory(new PostProcessTypeAdapterFactory())
            .registerTypeAdapterFactory(new PreProcessTypeAdapterFactory())
            .registerTypeAdapter(ImmutableMap.class, new ImmutableMapDeserializer())
            .registerTypeAdapter(ImmutableList.class, new ImmutableListDeserializer())
            .registerTypeAdapter(AtomicBoolean.class, new AtomicBooleanAdapter())
            .registerTypeAdapterFactory(exprAdapterFactory)
            .registerTypeAdapterFactory(columnTypeAdapterFactory)
            .registerTypeAdapterFactory(functionAdapterFactory);

    // this instance is thread-safe.
    public static final Gson GSON = GSON_BUILDER.create();
}
