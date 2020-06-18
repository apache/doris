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

package org.apache.doris.optimizer;


import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.Table;
import org.apache.doris.catalog.Type;

import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformance;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;

import java.util.List;

public class CalciteMetaMgr {
    public static SchemaPlus registerRootSchema(String db) {
        SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        Database database = Catalog.getCurrentCatalog().getDb(db);

        for (Table table : database.getTables()) {
            TableImpl tableDef = new TableImpl(table);
            rootSchema.add(table.getName().toUpperCase(), tableDef);
        }
        return rootSchema;
    }

    public static SqlConformance conformance(FrameworkConfig config) {
        final Context context = config.getContext();
        if (context != null) {
            final CalciteConnectionConfig connectionConfig =
                    context.unwrap(CalciteConnectionConfig.class);
            if (connectionConfig != null) {
                return connectionConfig.conformance();
            }
        }
        return SqlConformanceEnum.DEFAULT;
    }

    public static RexBuilder createRexBuilder(RelDataTypeFactory typeFactory) {
        return new RexBuilder(typeFactory);
    }

    public static class ViewExpanderImpl implements RelOptTable.ViewExpander {
        public ViewExpanderImpl() {
        }

        @Override
        public RelRoot expandView(RelDataType rowType, String queryString, List<String> schemaPath,
                                  List<String> viewPath) {
            return null;
        }
    }

    private static final class TableImpl extends AbstractTable {

        final Table table;

        private TableImpl(Table table) {
            this.table = table;
        }

        @Override
        public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(typeFactory);
            for (Column column : this.table.getBaseSchema()) {
                builder.add(column.getName().toUpperCase(), convertType(column.getType(), typeFactory, column.isAllowNull()));
            }
            return builder.build();
        }

        private static RelDataType convertType(
                Type type,
                RelDataTypeFactory typeFactory, boolean nullable
        ) {
            RelDataType relDataType = convertTypeNotNull(type, typeFactory);
            if (nullable) {
                return typeFactory.createTypeWithNullability(relDataType, true);
            } else {
                return relDataType;
            }
        }

        private static RelDataType convertTypeNotNull(
                Type type,
                RelDataTypeFactory typeFactory
        ) {

            switch (type.getPrimitiveType()) {
                case TINYINT:
                    return typeFactory.createSqlType(SqlTypeName.TINYINT);
                case SMALLINT:
                    return typeFactory.createSqlType(SqlTypeName.SMALLINT);
                case INT:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case BIGINT:
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case VARCHAR:
                    return typeFactory.createSqlType(SqlTypeName.CHAR);
                case CHAR:
                    return typeFactory.createSqlType(SqlTypeName.CHAR);
                case DATE:
                    return typeFactory.createSqlType(SqlTypeName.DATE);
                case DATETIME:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                default:
                    return typeFactory.createSqlType(SqlTypeName.ANY);

            }
        }
    }
}

