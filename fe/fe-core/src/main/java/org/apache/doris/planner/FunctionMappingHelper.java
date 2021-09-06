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

package org.apache.doris.planner;

import com.google.common.collect.Maps;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Map;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.FunctionCallExpr;
import org.apache.doris.analysis.FunctionName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class FunctionMappingHelper {
    private static final Logger LOG = LogManager.getLogger(FunctionMappingHelper.class);

    public final static Map<String, Map<String, String>> EXTERNAL_DATABASE_FUNCTION_MAPPING = Maps.newTreeMap();
    public final static Map<String, String> MYSQL_MAPPING_FUNCTION = Maps.newTreeMap();
    public final static Map<String, String> ORACLE_MAPPING_FUNCTION = Maps.newTreeMap();
    public final static Map<String, String> SQLSERVER_MAPPING_FUNCTION = Maps.newTreeMap();
    public final static Map<String, String> POSTGRESQL_MAPPING_FUNCTION = Maps.newTreeMap();

    public enum DatabaseType {
        MYSQL,
        ORACLE,
        SQLSERVER,
        POSTGRESQL
    }

    static {
        // Add mapping function.
        addFunctionMapping(DatabaseType.MYSQL,"get_json_string", "json_extract");
    }

    public static void addFunctionMapping(DatabaseType databaseType, String sourceFunction, String targetFunction) {
        switch (databaseType) {
            case MYSQL:
                MYSQL_MAPPING_FUNCTION.put(sourceFunction, targetFunction);
                break;
            case ORACLE:
                ORACLE_MAPPING_FUNCTION.put(sourceFunction, targetFunction);
                break;
            case SQLSERVER:
                SQLSERVER_MAPPING_FUNCTION.put(sourceFunction, targetFunction);
                break;
            case POSTGRESQL:
                POSTGRESQL_MAPPING_FUNCTION.put(sourceFunction, targetFunction);
                break;
            default:
                break;
        }
        EXTERNAL_DATABASE_FUNCTION_MAPPING.put(DatabaseType.MYSQL.name(), MYSQL_MAPPING_FUNCTION);
        EXTERNAL_DATABASE_FUNCTION_MAPPING.put(DatabaseType.ORACLE.name(), ORACLE_MAPPING_FUNCTION);
        EXTERNAL_DATABASE_FUNCTION_MAPPING.put(DatabaseType.SQLSERVER.name(), SQLSERVER_MAPPING_FUNCTION);
        EXTERNAL_DATABASE_FUNCTION_MAPPING.put(DatabaseType.POSTGRESQL.name(), POSTGRESQL_MAPPING_FUNCTION);
    }

    public static void mappingEngineFunction(Expr expr, String engineType) {
        ArrayList<Expr> children = expr.getChildren();
        Map<String, String> functionMapping = FunctionMappingHelper.EXTERNAL_DATABASE_FUNCTION_MAPPING.get(engineType);

        for (Expr child : children) {
            mappingEngineFunction(child, engineType);
        }

        for (String sourceFunctionName:functionMapping.keySet()) {
            if ((expr instanceof FunctionCallExpr) &&
                    ((FunctionCallExpr) expr).getFnName().getFunction().equalsIgnoreCase(sourceFunctionName)) {
                try {
                    Field field = ((FunctionCallExpr) expr).getClass().getDeclaredField("fnName");
                    field.setAccessible(true);
                    field.set(expr, new FunctionName(functionMapping.get(sourceFunctionName)));
                } catch (NoSuchFieldException | IllegalAccessException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
