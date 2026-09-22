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

package org.apache.doris.datasource.lance.metadata;

import org.apache.doris.catalog.Column;

import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import java.util.ArrayList;
import java.util.List;

/** Shared Arrow-to-Doris column construction for catalog schemas and search TVFs. */
public final class LanceSchemaHelper {
    private LanceSchemaHelper() {
    }

    /** Returns fresh mutable columns; column positions are ordinals, not Lance field IDs. */
    public static List<Column> toDorisColumns(Schema schema) {
        List<Column> columns = new ArrayList<>(schema.getFields().size());
        int position = 0;
        for (Field field : schema.getFields()) {
            columns.add(toDorisColumn(field, position++));
        }
        return columns;
    }

    /** Preserves Arrow nullability and comments while assigning the Doris schema position. */
    public static Column toDorisColumn(Field field, int position) {
        String comment = field.getMetadata() == null ? null : field.getMetadata().get("comment");
        return new Column(field.getName(), LanceTypeConverter.toDorisType(field), false,
                null, field.isNullable(), comment, true, position);
    }

}
