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

package org.apache.doris.nereids.trees.expressions.functions.scalar;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Env;
import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.Pair;
import org.apache.doris.dictionary.Dictionary;
import org.apache.doris.dictionary.DictionaryManager;
import org.apache.doris.dictionary.LayoutType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.functions.AlwaysNotNullable;
import org.apache.doris.nereids.trees.expressions.functions.CustomSignature;
import org.apache.doris.nereids.trees.expressions.literal.ArrayLiteral;
import org.apache.doris.nereids.trees.expressions.literal.Literal;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.DataType;
import org.apache.doris.nereids.types.StructField;
import org.apache.doris.nereids.types.StructType;
import org.apache.doris.nereids.util.TypeCoercionUtils;

import com.google.common.base.Preconditions;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * dict_get_many function.
 * {@code STRUCT dict_get_many("<name>", ARRAY<VARCHAR> <value_col_names>, STRUCT <QUERY_VALUE>);}
 */
public class DictGetMany extends ScalarFunction implements CustomSignature, AlwaysNotNullable {
    /**
     * constructor with 3 arguments. (1. dbName.dictName, 2. queryKeyColumnName, 3. queryKeyValue)
     */
    public DictGetMany(Expression arg0, Expression arg1, Expression arg2) {
        super("dict_get_many", arg0, arg1, arg2);
    }

    /** constructor for withChildren and reuse signature */
    private DictGetMany(ScalarFunctionParams functionParams) {
        super(functionParams);
    }

    @Override
    public void checkLegalityBeforeTypeCoercion() {
        if (getArguments().size() != 3) {
            throw new AnalysisException("dict_get() requires 3 arguments");
        }
        if (!getArgument(0).isLiteral() || !getArgument(1).isLiteral()) {
            throw new AnalysisException("dict_get() requires literal arguments for position 0 and 1");
        }
        if (((Literal) getArgument(0)).getStringValue().split("\\.").length != 2) {
            throw new AnalysisException("dict_get() requires dbName.dictName as first argument");
        }
        String[] firstNames = ((Literal) getArgument(0)).getStringValue().split("\\."); // db.dict
        String dbName = firstNames[0];
        String dictName = firstNames[1];
        if (dbName.isEmpty() || dictName.isEmpty()) {
            throw new AnalysisException("dict_get() requires dbName.dictName as first argument");
        }
    }

    @Override
    public void checkLegalityAfterRewrite() {
        checkLegalityBeforeTypeCoercion();
    }

    @Override
    public FunctionSignature customSignature() {
        return customSignatureDict().key();
    }

    /**
     * use for visitDictGetMany to get real signature
     */
    public Pair<FunctionSignature, Dictionary> customSignatureDict() {
        DictionaryManager dicMgr = Env.getCurrentEnv().getDictionaryManager();
        String[] firstNames = ((Literal) getArgument(0)).getStringValue().split("\\."); // db.dict
        String dbName = firstNames[0];
        String dictName = firstNames[1];
        List<Literal> colNames = ((ArrayLiteral) getArgument(1)).getValue();

        Dictionary dictionary;
        try {
            dictionary = dicMgr.getDictionary(dbName, dictName);
            // check is not key column
            for (Literal colName : colNames) {
                if (dictionary.getDicColumns().stream()
                        .anyMatch(col -> col.getName().equals(colName.getStringValue()) && col.isKey())) {
                    throw new AnalysisException("Can't ask for key " + colName.getStringValue() + " by dict_get()");
                }
            }
        } catch (DdlException e) {
            throw new AnalysisException(e.getMessage());
        }

        // return struct is combine of query columns' types.
        List<StructField> returnFields = new ArrayList<>();
        for (Literal colName : colNames) {
            Column col = dictionary.getOriginColumn(colName.getStringValue());
            StructField field = new StructField(col.getName(), DataType.fromCatalogType(col.getType()),
                    true, col.getComment());
            returnFields.add(field);
        }

        // generate target query types(for 3rd argument) one by one for each nested column in the struct
        List<StructField> originQueryFields = ((StructType) getArgumentType(2)).getFields();
        List<StructField> targetQueryFields = new ArrayList<>(); // after add essential castExpr
        List<DataType> targetTypes = dictionary.getKeyColumnTypes(); // query columns should cast to key columns' types

        for (int i = 0; i < originQueryFields.size(); i++) {
            StructField field = originQueryFields.get(i);
            DataType queryType = field.getDataType();
            DataType targetType = targetTypes.get(i);

            if (dictionary.getLayout() == LayoutType.HASH_MAP) {
                Optional<DataType> castType = TypeCoercionUtils.implicitCast(queryType, targetType);
                if (castType.isPresent() && !castType.get().equals(queryType)) {
                    queryType = castType.get();
                }
            } else { // IP_TRIE
                if (!queryType.isIPType()) {
                    // we CAN'T CAST it because we dont know which one of ipv4 or ipv6 is the target.
                    throw new AnalysisException("dict_get_many() only support IP type for IP_TRIE");
                }
            }
            // TypeCoercionUtils could deal inner type individually.
            // so we could add castExpr for part of the nested columns.
            targetQueryFields.add(new StructField(field.getName(), queryType, field.isNullable(), field.getComment()));
        }

        return Pair.of(FunctionSignature.ret(new StructType(returnFields)).args(getArgumentType(0), getArgumentType(1),
                new StructType(targetQueryFields)), dictionary);
    }

    /**
     * withChildren.
     */
    @Override
    public DictGetMany withChildren(List<Expression> children) {
        Preconditions.checkArgument(children.size() == 3);
        return new DictGetMany(getFunctionParams(children));
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitDictGetMany(this, context);
    }
}
