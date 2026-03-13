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

package org.apache.doris.catalog.stream;


import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.TableIf;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.DdlException;

import com.google.common.base.Preconditions;

import java.util.List;

public class StreamBuildFactory {
    public static class BuildParams {
        String streamName;
        TableIf baseTable;
    }

    private BuildParams params;

    public StreamBuildFactory() {
        params = new BuildParams();
    }

    public StreamBuildFactory withBaseTable(TableIf baseTable) {
        params.baseTable = baseTable;
        return this;
    }

    public StreamBuildFactory withName(String name) {
        params.streamName = name;
        return this;
    }

    public BaseStream build() throws DdlException {
        Preconditions.checkNotNull(params, "The factory isn't initialized.");
        Preconditions.checkNotNull(params.streamName, "Stream name isn't initialized.");
        Preconditions.checkNotNull(params.baseTable, "Stream base table isn't initialized.");
        List<Column> schema;
        schema = params.baseTable.getBaseSchema();

        // extra columns
        Column changeTypeColumn = new Column(BaseStream.STREAM_CHANGE_TYPE_COL, Type.VARCHAR);
        Column sequenceColumn = new Column(BaseStream.STREAM_SEQ_COL, Type.BIGINT);
        changeTypeColumn.setIsVisible(false);
        sequenceColumn.setIsVisible(false);
        schema.add(changeTypeColumn);
        schema.add(sequenceColumn);
        switch (params.baseTable.getType()) {
            case OLAP:
                return new OlapTableStream(params.streamName, schema, params.baseTable);
            default:
                throw new DdlException("unsupported stream base table type: " + params.baseTable.getType());
        }
    }
}
