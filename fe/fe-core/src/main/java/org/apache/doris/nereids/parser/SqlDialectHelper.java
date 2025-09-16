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
        String convertedStmt = originStmt;
        @Nullable Dialect sqlDialect = Dialect.getByName(sessionVariable.getSqlDialect());
        if (sqlDialect != null && sqlDialect != Dialect.DORIS) {
            PluginMgr pluginMgr = Env.getCurrentEnv().getPluginMgr();
            List<DialectConverterPlugin> plugins = pluginMgr.getActiveDialectPluginList(sqlDialect);
            for (DialectConverterPlugin plugin : plugins) {
                try {
                    String convertedSql = plugin.convertSql(originStmt, sessionVariable);
                    if (StringUtils.isNotEmpty(convertedSql)) {
                        convertedStmt = convertedSql;
                        break;
                    }
                } catch (Throwable throwable) {
                    LOG.warn("Convert sql with dialect {} failed, plugin: {}, sql: {}, use origin sql.",
                            sqlDialect, plugin.getClass().getSimpleName(), originStmt, throwable);
                }
            }
        }
        return convertedStmt;
    }
}
