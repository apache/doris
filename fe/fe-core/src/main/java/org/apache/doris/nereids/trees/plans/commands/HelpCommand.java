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

package org.apache.doris.nereids.trees.plans.commands;

import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.ShowResultSet;
import org.apache.doris.qe.ShowResultSetMetaData;
import org.apache.doris.qe.StmtExecutor;
import org.apache.doris.qe.help.HelpModule;
import org.apache.doris.qe.help.HelpTopic;

import com.google.common.base.Strings;
import com.google.common.collect.Lists;

import java.util.List;

/**
 * Represents the command for HELP.
 */
public class HelpCommand extends ShowCommand {
    private static final List<List<String>> EMPTY_SET = Lists.newArrayList();

    private static final ShowResultSetMetaData TOPIC_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("description", ScalarType.createVarchar(1000)))
                    .addColumn(new Column("example", ScalarType.createVarchar(1000)))
                    .build();
    private static final ShowResultSetMetaData CATEGORY_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("source_category_name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("is_it_category", ScalarType.createVarchar(1)))
                    .build();
    private static final ShowResultSetMetaData KEYWORD_META_DATA =
            ShowResultSetMetaData.builder()
                    .addColumn(new Column("name", ScalarType.createVarchar(64)))
                    .addColumn(new Column("is_it_category", ScalarType.createVarchar(1)))
                    .build();

    private final String mark;

    public HelpCommand(String mark) {
        super(PlanType.HELP_COMMAND);
        this.mark = mark;
    }

    @Override
    public ShowResultSet doRun(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (Strings.isNullOrEmpty(mark)) {
            throw new AnalysisException("Help empty info.");
        }
        HelpModule module = HelpModule.getInstance();
        ShowResultSet resultSet;

        // Get topic
        HelpTopic topic = module.getTopic(mark);
        // Get by Keyword
        if (topic == null) {
            List<String> topics = module.listTopicByKeyword(mark);
            if (topics.size() == 0) {
                // assign to avoid code style problem
                topic = null;
            } else if (topics.size() == 1) {
                topic = module.getTopic(topics.get(0));
            } else {
                // Send topic list and category list
                List<List<String>> rows = Lists.newArrayList();
                for (String str : topics) {
                    rows.add(Lists.newArrayList(str, "N"));
                }
                List<String> categories = module.listCategoryByName(mark);
                for (String str : categories) {
                    rows.add(Lists.newArrayList(str, "Y"));
                }
                return new ShowResultSet(KEYWORD_META_DATA, rows);
            }
        }
        if (topic != null) {
            resultSet = new ShowResultSet(TOPIC_META_DATA, Lists.<List<String>>newArrayList(
                    Lists.newArrayList(topic.getName(), topic.getDescription(), topic.getExample())));
        } else {
            List<String> categories = module.listCategoryByName(mark);
            if (categories.isEmpty()) {
                // If no category match for this name, return
                resultSet = new ShowResultSet(KEYWORD_META_DATA, EMPTY_SET);
            } else if (categories.size() > 1) {
                // Send category list
                resultSet = new ShowResultSet(CATEGORY_META_DATA,
                        Lists.<List<String>>newArrayList(categories));
            } else {
                // Send topic list and sub-category list
                List<List<String>> rows = Lists.newArrayList();
                List<String> topics = module.listTopicByCategory(categories.get(0));
                for (String str : topics) {
                    rows.add(Lists.newArrayList(str, "N"));
                }
                List<String> subCategories = module.listCategoryByCategory(categories.get(0));
                for (String str : subCategories) {
                    rows.add(Lists.newArrayList(str, "Y"));
                }
                resultSet = new ShowResultSet(KEYWORD_META_DATA, rows);
            }
        }
        return resultSet;
    }

    @Override
    public ShowResultSetMetaData getMetaData() {
        return ShowResultSetMetaData.builder().build();
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitHelpCommand(this, context);
    }
}
