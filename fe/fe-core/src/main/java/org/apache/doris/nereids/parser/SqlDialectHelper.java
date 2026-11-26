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

package org.apache.doris.nereids.parser;

import org.apache.doris.catalog.Env;
import org.apache.doris.plugin.DialectConverterPlugin;
import org.apache.doris.plugin.PluginMgr;
import org.apache.doris.qe.SessionVariable;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Helper class for SQL dialect conversion.
 */
public class SqlDialectHelper {
    private static final Logger LOG = LogManager.getLogger(SqlDialectHelper.class);

    /**
     * Convert SQL statement based on current SQL dialect
     *
     * @param originStmt original SQL statement
     * @param sessionVariable session variable containing dialect settings
     * @return converted SQL statement, or original statement if conversion fails
     */
    public static String convertSqlByDialect(String originStmt, SessionVariable sessionVariable) {
        return convertSql(originStmt, sessionVariable, false).orElse(originStmt);
    }

    /** A foreign view may use the result only when a converter successfully handled its dialect. */
    public static Optional<String> tryConvertSqlByDialect(String originStmt, SessionVariable sessionVariable) {
        return convertSql(originStmt, sessionVariable, true);
    }

    private static Optional<String> convertSql(String originStmt, SessionVariable sessionVariable, boolean strict) {
        @Nullable Dialect sqlDialect = Dialect.getByName(sessionVariable.getSqlDialect());
        if (sqlDialect != null && sqlDialect != Dialect.DORIS) {
            PluginMgr pluginMgr = Env.getCurrentEnv().getPluginMgr();
            List<DialectConverterPlugin> plugins = pluginMgr.getActiveDialectPluginList(sqlDialect);
            for (DialectConverterPlugin plugin : plugins) {
                try {
                    String convertedSql = strict ? plugin.tryConvertSql(originStmt, sessionVariable)
                            : plugin.convertSql(originStmt, sessionVariable);
                    if (StringUtils.isNotEmpty(convertedSql)) {
                        return Optional.of(convertedSql);
                    }
                } catch (Throwable throwable) {
                    LOG.warn("Convert sql with dialect {} failed, plugin: {}, sql: {}.",
                            sqlDialect, plugin.getClass().getSimpleName(), originStmt, throwable);
                }
            }
        }
        return Optional.empty();
    }
}
