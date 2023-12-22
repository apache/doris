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

package org.apache.doris.plugin;

import org.apache.doris.analysis.StatementBase;
import org.apache.doris.nereids.parser.Dialect;
import org.apache.doris.qe.SessionVariable;

import com.google.common.collect.ImmutableSet;

import java.util.List;
import javax.annotation.Nullable;

/**
 * Sql dialect adapter interface.
 */
public interface DialectConverterPlugin {

    /**
     * Set dialect set which this plugin can handle
     */
    ImmutableSet<Dialect> acceptDialects();

    /**
     * Override this method if you want to convert sql before parse, returns null as default.
     * */
    default @Nullable String convertSql(String originSql, SessionVariable sessionVariable) {
        return null;
    }

    /**
     * <pre>
     * Parse sql with dialect.
     * Fallback to {@link org.apache.doris.nereids.parser.NereidsParser#parseSQL(String)}
     * if this method returns null, empty list or any exception throws.
     * Use Dialect.getByName(sessionVariable.getSqlDialect()) to extract the dialect parameter.
     * </pre>
     * */
    default @Nullable List<StatementBase> parseSqlWithDialect(String sql, SessionVariable sessionVariable) {
        return null;
    }
}
