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
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.nereids.types.coercion.AnyDataType;
import org.apache.doris.tablefunction.ExternalSearchTableValuedFunction;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/**
 * A search table function over a plugin catalog's table ({@code vector_search}, {@code full_text_search}); see
 * {@link ExternalSearchTableValuedFunction}. The analyzer wraps its relation in the TopN that merges the candidates
 * of the scan's ranges and moves the WHERE written on the function below that TopN; the analysis and
 * lazy-materialization rules recognize it by this class.
 */
public abstract class ExternalSearch extends TableValuedFunction {
    protected final Set<String> deferredArguments;

    protected ExternalSearch(String functionName, Properties properties, Set<String> deferredArguments) {
        super(functionName, properties);
        this.deferredArguments = ImmutableSet.copyOf(deferredArguments);
    }

    /**
     * The same search while a statement is prepared, with the arguments its parameters will supply: the connector
     * returns the columns without their values.
     */
    public abstract ExternalSearch withDeferredArguments(Set<String> deferredArguments);

    public final ExternalSearchTableValuedFunction getSearchFunction() {
        return (ExternalSearchTableValuedFunction) getCatalogFunction();
    }

    @Override
    public FunctionSignature customSignature() {
        return FunctionSignature.of(AnyDataType.INSTANCE_WITHOUT_INDEX, getArgumentsTypes());
    }
}
