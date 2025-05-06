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

package org.apache.doris.datasource.doris;

import org.apache.doris.catalog.AggregateType;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.util.JsonUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.List;

public class DorisUtil {

    public static List<Column> genColumnsFromDoris(DorisRestClient client, String dbName, String tableName) {
        String columnsJsonStr = client.getJsonColumns(dbName, tableName);

        List<Column> columnList = new ArrayList<>();
        ObjectNode objectNode = JsonUtil.parseObject(columnsJsonStr);
        JsonNode properties = objectNode.path("properties");
        for (JsonNode columnJson : properties) {
            if (columnJson.isObject()) {
                columnList.add(parseColumn((ObjectNode) columnJson));
            }
        }
        return columnList;
    }

    private static Column parseColumn(ObjectNode columnJson) {
        boolean nullable = columnJson.path("nullable").asBoolean(false);
        String name = columnJson.path("name").asText();
        String comment = columnJson.path("comment").asText();
        boolean isKey = columnJson.path("key").asBoolean(false);

        String defaultValue = null;
        JsonNode defaultValueJson = columnJson.get("default_value");
        if (defaultValueJson != null) {
            defaultValue = JsonUtil.convertNodeToString(defaultValueJson);
        }

        String typeName = columnJson.path("type").asText();
        Type type = Type.getTypeFromTypeName(typeName);

        String aggregationTypeName = columnJson.path("aggregation_type").asText();
        AggregateType aggType = AggregateType.getAggTypeFromAggName(aggregationTypeName);

        JsonNode attributesJson = columnJson.get("type_attributes");
        if (attributesJson != null) {
            String scale = attributesJson.path("scale").asText("0");
            String precision = attributesJson.path("precision").asText("0");
            String length = attributesJson.path("length").asText("0");

            type = ScalarType.createType(
                type.getPrimitiveType(),
                Integer.parseInt(length),
                Integer.parseInt(precision),
                Integer.parseInt(scale)
            );
        }

        return new Column(name, type, isKey, aggType, nullable, defaultValue, comment);
    }
}
