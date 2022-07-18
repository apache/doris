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

package org.apache.doris.datasource;

import org.apache.doris.analysis.AlterCatalogNameStmt;
import org.apache.doris.analysis.AlterCatalogPropertyStmt;
import org.apache.doris.analysis.CreateCatalogStmt;
import org.apache.doris.analysis.DropCatalogStmt;
import org.apache.doris.analysis.StatementBase;

import java.util.Map;

/**
 * A factory to create catalog instance of log or covert catalog into log.
 */
public class CatalogFactory {

    /**
     * Convert the sql statement into catalog log.
     */
    public static CatalogLog constructorCatalogLog(long catalogId, StatementBase stmt) {
        CatalogLog log = new CatalogLog();
        if (stmt instanceof CreateCatalogStmt) {
            log.setCatalogId(catalogId);
            log.setCatalogName(((CreateCatalogStmt) stmt).getCatalogName());
            log.setProps(((CreateCatalogStmt) stmt).getProperties());
        } else if (stmt instanceof DropCatalogStmt) {
            log.setCatalogId(catalogId);
        } else if (stmt instanceof AlterCatalogPropertyStmt) {
            log.setCatalogId(catalogId);
            log.setNewProps(((AlterCatalogPropertyStmt) stmt).getNewProperties());
        } else if (stmt instanceof AlterCatalogNameStmt) {
            log.setCatalogId(catalogId);
            log.setNewCatalogName(((AlterCatalogNameStmt) stmt).getNewCatalogName());
        } else {
            throw new RuntimeException("Unknown stmt for datasource manager " + stmt.getClass().getSimpleName());
        }
        return log;
    }

    /**
     * create the datasource instance from data source log.
     */
    public static DataSourceIf constructorFromLog(CatalogLog log) {
        return constructorDataSource(log.getCatalogId(), log.getCatalogName(), log.getProps());
    }

    private static DataSourceIf constructorDataSource(long catalogId, String name, Map<String, String> props) {
        String type = props.get("type");
        DataSourceIf dataSource;
        switch (type) {
            case "hms":
                dataSource = new HMSExternalDataSource(catalogId, name, props);
                break;
            case "es":
                dataSource = new EsExternalDataSource(catalogId, name, props);
                break;
            default:
                throw new RuntimeException("Unknown datasource type for " + type);
        }
        return dataSource;
    }
}
