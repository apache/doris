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
     * <pre>
     * Override this method if you want to convert sql before parse.
     * Fallback to next dialect plugin if this method returns null, empty string or any exception throws.
     * If all plugins fail to convert (returns null, empty string or any exception throws),
     * Nereids parser will use the original SQL.
     * </pre>
     * */
    default @Nullable String convertSql(String originSql, SessionVariable sessionVariable) {
        return null;
    }

    /**
     * Return SQL only when the conversion succeeded. A stored view must not silently parse
     * foreign SQL using Doris syntax when a converter is unavailable.
     */
    default @Nullable String tryConvertSql(String originSql, SessionVariable sessionVariable) {
        String converted = convertSql(originSql, sessionVariable);
        // Legacy converters may echo their input on failure. They can override this method to
        // distinguish a successful identity conversion from that fallback.
        return originSql.equals(converted) ? null : converted;
    }

    /**
     * <pre>
     * Parse sql with dialect.
     * Fallback to next dialect plugin if this method returns null, empty string or any exception throws.
     * If all plugins fail to parse (returns null, empty string or any exception throws),
     * Nereids parser will fallback to invoke {@link org.apache.doris.nereids.parser.NereidsParser#parseSQL(String)}.
     * Use Dialect.getByName(sessionVariable.getSqlDialect()) to extract the dialect parameter.
     * </pre>
     * */
    @Nullable List<StatementBase> parseSqlWithDialect(String sql, SessionVariable sessionVariable);
}
