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

package org.apache.doris.nereids.trees.expressions.functions.table;

import org.apache.doris.catalog.FunctionSignature;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.trees.expressions.visitor.ExpressionVisitor;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.tablefunction.PaimonTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * Table-valued function for accessing Paimon metadata tables.
 */
public class PaimonMeta extends TableValuedFunction {

    public PaimonMeta(Properties properties) {
        super("paimon_meta", properties);
    }

    public static PaimonMeta createPaimonMeta(List<String> nameParts, String queryType) {
        Map<String, String> prop = Maps.newHashMap();
        prop.put(PaimonTableValuedFunction.TABLE, Joiner.on(".").join(nameParts));
        prop.put(PaimonTableValuedFunction.QUERY_TYPE, queryType);
        return new PaimonMeta(new Properties(prop));
    }

    @Override
    protected TableValuedFunctionIf toCatalogFunction() {
        try {
            Map<String, String> arguments = getTVFProperties().getMap();
            return PaimonTableValuedFunction.create(arguments);
        } catch (Throwable t) {
            throw new AnalysisException("Can not build PaimonTableValuedFunction by "
                    + this + ": " + t.getMessage(), t);
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPaimonMeta(this, context);
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.of(AnyDataType.INSTANCE_WITHOUT_INDEX, getArgumentsTypes());
    }
}
