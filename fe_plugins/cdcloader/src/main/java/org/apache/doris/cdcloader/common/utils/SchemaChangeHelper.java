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

package org.apache.doris.cdcloader.common.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.cdcloader.mysql.constants.LoadConstants;
import org.apache.flink.cdc.common.event.AddColumnEvent;
import org.apache.flink.cdc.common.event.DropColumnEvent;
import org.apache.flink.cdc.common.event.RenameColumnEvent;
import org.apache.flink.cdc.common.event.SchemaChangeEvent;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Column;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SchemaChangeHelper {

    private static final Logger LOG = LoggerFactory.getLogger(SchemaChangeHelper.class);

    public static List<String> parserSchemaChangeEvents(Map<String, String> context, List<SchemaChangeEvent> events) {
        List<String> sqls = new ArrayList<>();
        for(SchemaChangeEvent event : events){
            String sql = parserSchemaChangeEvent(context, event);
            sqls.add(sql);
        }
        return sqls;
    }

    public static String parserSchemaChangeEvent(Map<String, String> context, SchemaChangeEvent event) {
        if (event instanceof AddColumnEvent) {
            //add column
            return parserAddColumnEvent((AddColumnEvent) event);
        } else if (event instanceof DropColumnEvent) {
            //drop column
            return parserDropColumnEvent((DropColumnEvent) event);
        } else if (event instanceof RenameColumnEvent) {
            return parserRenameColumnEvent((RenameColumnEvent) event);
        } else {
            throw new UnsupportedOperationException("Unsupported schema change event, " + event);
        }
    }

    private static String parserAddColumnEvent(AddColumnEvent event) {
        TableId tableId = event.tableId();
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("ALTER TABLE `%s`.`%s` ", tableId.getSchemaName(), tableId.getTableName()));
        List<AddColumnEvent.ColumnWithPosition> addedColumns = event.getAddedColumns();
        for(int index = 0; index < addedColumns.size() ; index++){
            if(index > 0){
                builder.append(",");
            }
            AddColumnEvent.ColumnWithPosition col = addedColumns.get(index);
            Column addColumn = col.getAddColumn();
            builder.append("ADD COLUMN ")
                .append(buildColumnStmt(addColumn, false));
        }
        return builder.toString();
    }

    private static String buildColumnStmt(Column column, boolean isKey) {
        StringBuilder builder = new StringBuilder();
        builder.append("`");
        builder.append(column.getName());
        builder.append("` ");
        builder.append(DataTypeUtils.convertFromDataType(column.getType(), isKey));
        builder.append(" ");
        if (StringUtils.isNotEmpty(column.getComment())) {
            builder.append(String.format(" COMMENT \"%s\"", column.getComment()));
        }
        return builder.toString();
    }

    private static String parserDropColumnEvent(DropColumnEvent event) {
        TableId tableId = event.tableId();
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("ALTER TABLE `%s`.`%s` ", tableId.getSchemaName(), tableId.getTableName()));
        List<String> droppedColumnNames = event.getDroppedColumnNames();
        for(int index = 0; index < droppedColumnNames.size(); index ++ ){
            if(index > 0){
                builder.append(",");
            }
            String column = droppedColumnNames.get(index);
            builder.append("DROP COLUMN ")
                .append("`")
                .append(column)
                .append("`");
        }
        return builder.toString();
    }

    private static String parserRenameColumnEvent(RenameColumnEvent event) {
        LOG.warn("Rename column is not supported, event {}", event);
        throw new UnsupportedOperationException("Rename column is not supported currently");
    }

    private static List<String> identifier(List<String> name) {
        List<String> result = name.stream().map(m -> identifier(m)).collect(Collectors.toList());
        return result;
    }

    private static String identifier(String name) {
        return "`" + name + "`";
    }

    private static String quoteComment(String comment) {
        if (comment == null) {
            return "";
        } else {
            return comment.replaceAll("'", "\\\\'");
        }
    }

    private static String quoteProperties(String name) {
        return "'" + name + "'";
    }

    private static Map<String, String> getTableCreateProperties(Map<String, String> properties) {
        final Map<String, String> tableCreateProps = new HashMap<>();

        for (Map.Entry<String, String> entry : properties.entrySet()) {
            if (entry.getKey().startsWith(LoadConstants.TABLE_PROPS_PREFIX)) {
                String subKey = entry.getKey().substring(LoadConstants.TABLE_PROPS_PREFIX.length());
                tableCreateProps.put(subKey, entry.getValue());
            }
        }
        return tableCreateProps;
    }
}
