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

import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.expressions.Properties;
import org.apache.doris.tablefunction.FullTextSearchTableValuedFunction;
import org.apache.doris.tablefunction.TableValuedFunctionIf;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

/** full_text_search: the rows of a plugin catalog's table that best match a text query. */
public class FullTextSearch extends ExternalSearch {
    public FullTextSearch(Properties properties) {
        this(properties, ImmutableSet.of());
    }

    public FullTextSearch(Properties properties, Set<String> deferredArguments) {
        super(FullTextSearchTableValuedFunction.NAME, properties, deferredArguments);
    }

    @Override
    public FullTextSearch withDeferredArguments(Set<String> deferredArguments) {
        return new FullTextSearch(getTVFProperties(), deferredArguments);
    }

    @Override
    protected TableValuedFunctionIf toCatalogFunction() {
        try {
            return new FullTextSearchTableValuedFunction(getTVFProperties().getMap(), deferredArguments);
        } catch (Throwable t) {
            throw new AnalysisException("Can not build full_text_search(): " + t.getMessage(), t);
        }
    }
}
