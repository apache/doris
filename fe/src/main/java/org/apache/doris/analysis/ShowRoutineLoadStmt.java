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


package org.apache.doris.analysis;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.qe.ShowResultSetMetaData;

/*
  Show routine load progress by routine load name

  syntax:
      SHOW ROUTINE LOAD name
 */
public class ShowRoutineLoadStmt extends ShowStmt {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("id")
                    .add("name")
                    .add("db_id")
                    .add("table_id")
                    .add("partitions")
                    .add("state")
                    .add(CreateRoutineLoadStmt.DESIRED_CONCURRENT_NUMBER_PROPERTY)
                    .add("progress")
                    .build();

    private final String name;

    public ShowRoutineLoadStmt(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    @Override
    public void analyze(Analyzer analyzer) throws AnalysisException, UserException {
        super.analyze(analyzer);
        if (Strings.isNullOrEmpty(name)) {
            throw new AnalysisException("routine load name could not be empty or null");
        }
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }
}
