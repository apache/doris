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

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.OrderByPair;
import org.apache.doris.qe.ShowResultSetMetaData;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.List;

/*
  Show routine load progress by routine load name

  syntax:
      SHOW [ALL] ROUTINE LOAD [FOR JobName] [LIKE pattern]

      without ALL: only show job which is not final
      with ALL: show all of job include history job

      without name: show all of routine load job in database with different name
      with name: show all of job named ${name} in database

      without on db: show all of job in connection db
         if user does not choose db before, return error
      with on db: show all of job in ${db}

      example:
        show routine load named test in database1
        use database1
        SHOW ROUTINE LOAD for test;

        show routine load in database1 include history
        use database1;
        SHOW ALL ROUTINE LOAD;

        show routine load in database1 whose name match pattern "%test%"
        use database1;
        SHOW ROUTINE LOAD LIKE "%test%";

        show routine load in all of database
        please use show proc
 */
public class ShowRoutineLoadStmt extends ShowStmt implements NotFallbackInParser {

    private static final ImmutableList<String> TITLE_NAMES =
            new ImmutableList.Builder<String>()
                    .add("Id")
                    .add("Name")
                    .add("CreateTime")
                    .add("PauseTime")
                    .add("EndTime")
                    .add("DbName")
                    .add("TableName")
                    .add("IsMultiTable")
                    .add("State")
                    .add("DataSourceType")
                    .add("CurrentTaskNum")
                    .add("JobProperties")
                    .add("DataSourceProperties")
                    .add("CustomProperties")
                    .add("Statistic")
                    .add("Progress")
                    .add("Lag")
                    .add("ReasonOfStateChanged")
                    .add("ErrorLogUrls")
                    .add("OtherMsg")
                    .add("User")
                    .add("Comment")
                    .build();

    private final LabelName labelName;
    private String dbFullName; // optional
    private String name; // optional
    private boolean includeHistory = false;
    private String pattern; // optional
    private final LimitElement limitElement;
    private final List<OrderByElement> orderByElements;

    private ArrayList<OrderByPair> orderByPairs;


    public ShowRoutineLoadStmt(LabelName labelName, boolean includeHistory,
            String pattern, List<OrderByElement> orderByElements, LimitElement limitElement) {
        this.labelName = labelName;
        this.includeHistory = includeHistory;
        this.pattern = pattern;
        this.orderByElements = orderByElements;
        this.limitElement = limitElement;
    }

    public String getDbFullName() {
        return dbFullName;
    }

    public String getName() {
        return name;
    }

    public boolean isIncludeHistory() {
        return includeHistory;
    }

    public String getPattern() {
        return pattern;
    }

    public ArrayList<OrderByPair> getOrderByPairs() {
        return this.orderByPairs;
    }

    public long getLimit() {
        if (limitElement != null && limitElement.hasLimit()) {
            return limitElement.getLimit();
        }
        return -1L;
    }

    public long getOffset() {
        if (limitElement != null && limitElement.hasOffset()) {
            return limitElement.getOffset();
        }
        return -1L;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        super.analyze(analyzer);
        checkLabelName(analyzer);

        if (orderByElements != null && !orderByElements.isEmpty()) {
            orderByPairs = new ArrayList<OrderByPair>();
            for (OrderByElement orderByElement : orderByElements) {
                if (!(orderByElement.getExpr() instanceof SlotRef)) {
                    throw new AnalysisException("Should order by column");
                }
                SlotRef slotRef = (SlotRef) orderByElement.getExpr();
                int index = analyzeColumn(slotRef.getColumnName());
                OrderByPair orderByPair = new OrderByPair(index, !orderByElement.getIsAsc());
                orderByPairs.add(orderByPair);
            }
        }
    }

    private void checkLabelName(Analyzer analyzer) throws AnalysisException {
        dbFullName = labelName == null ? null : labelName.getDbName();
        if (Strings.isNullOrEmpty(dbFullName)) {
            dbFullName = analyzer.getContext().getDatabase();
            if (Strings.isNullOrEmpty(dbFullName)) {
                ErrorReport.reportAnalysisException(ErrorCode.ERR_NO_DB_ERROR);
            }
        }
        name = labelName == null ? null : labelName.getLabelName();
    }

    public static List<String> getTitleNames() {
        return TITLE_NAMES;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        ShowResultSetMetaData.Builder builder = ShowResultSetMetaData.builder();

        for (String title : TITLE_NAMES) {
            builder.addColumn(new Column(title, ScalarType.createVarchar(30)));
        }
        return builder.build();
    }

    private int analyzeColumn(String columnName) throws AnalysisException {
        for (String title : TITLE_NAMES) {
            if (title.equalsIgnoreCase(columnName)) {
                return TITLE_NAMES.indexOf(title);
            }
        }

        throw new AnalysisException("Title name[" + columnName + "] does not exist");
    }

    @Override
    public RedirectStatus getRedirectStatus() {
        return RedirectStatus.FORWARD_NO_SYNC;
    }
}
