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

import org.apache.doris.analysis.StmtType;
import org.apache.doris.dictionary.LayoutType;
import org.apache.doris.nereids.exceptions.AnalysisException;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.commands.info.CreateDictionaryInfo;
import org.apache.doris.nereids.trees.plans.commands.info.DictionaryColumnDefinition;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;

/**
 * Command for creating a dictionary.
 */
public class CreateDictionaryCommand extends Command implements ForwardWithSync {
    private static final Logger LOG = LogManager.getLogger(CreateDictionaryCommand.class);

    private final CreateDictionaryInfo createDictionaryInfo;

    public CreateDictionaryCommand(boolean ifNotExists, String dbName, String dictName, String sourceCtlName,
            String sourceDbName, String sourceTableName, List<DictionaryColumnDefinition> columns,
            Map<String, String> properties, LayoutType layout) {
        super(PlanType.CREATE_DICTIONARY_COMMAND);
        this.createDictionaryInfo = new CreateDictionaryInfo(ifNotExists, dbName, dictName, sourceCtlName, sourceDbName,
                sourceTableName, columns, properties, layout);
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitCreateDictionaryCommand(this, context);
    }

    @Override
    public StmtType stmtType() {
        return StmtType.DDL;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) {
        try {
            // 1. Validate the dictionary info. names and existence.
            createDictionaryInfo.validateAndSet(ctx);

            // 2. Create dictionary and save it in manager. it will schedule data load.
            ctx.getEnv().getDictionaryManager().createDictionary(ctx, createDictionaryInfo);

            LOG.info("Created dictionary {} in {} from {}", createDictionaryInfo.getDictName(),
                    createDictionaryInfo.getDbName(), createDictionaryInfo.getSourceTableName());
        } catch (Exception e) {
            LOG.warn("Failed to create dictionary: {}", e.getMessage());
            throw new AnalysisException("Failed to create dictionary: " + e.getMessage());
        }
    }
}
