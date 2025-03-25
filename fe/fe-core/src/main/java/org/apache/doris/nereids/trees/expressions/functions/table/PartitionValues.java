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
import org.apache.doris.tablefunction.PartitionValuesTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * partition_values
 */
public class PartitionValues extends TableValuedFunction {
    public PartitionValues(Properties properties) {
        super("partition_values", properties);
    }

    /**
     * Create PartitionValues function.
     * @param qualifiedTableName ctl.db.tbl
     * @return PartitionValues function
     */
    public static TableValuedFunction create(List<String> qualifiedTableName) {
        Preconditions.checkArgument(qualifiedTableName != null && qualifiedTableName.size() == 3);
        Map<String, String> parameters = Maps.newHashMap();
        parameters.put(PartitionValuesTableValuedFunction.CATALOG, qualifiedTableName.get(0));
        parameters.put(PartitionValuesTableValuedFunction.DB, qualifiedTableName.get(1));
        parameters.put(PartitionValuesTableValuedFunction.TABLE, qualifiedTableName.get(2));
        return new PartitionValues(new Properties(parameters));
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.of(AnyDataType.INSTANCE_WITHOUT_INDEX, getArgumentsTypes());
    }

    @Override
    protected TableValuedFunctionIf toCatalogFunction() {
        try {
            Map<String, String> arguments = getTVFProperties().getMap();
            return new PartitionValuesTableValuedFunction(arguments);
        } catch (Throwable t) {
            throw new AnalysisException("Can not build PartitionsTableValuedFunction by "
                    + this + ": " + t.getMessage(), t);
        }
    }

    @Override
    public <R, C> R accept(ExpressionVisitor<R, C> visitor, C context) {
        return visitor.visitPartitionValues(this, context);
    }
}
