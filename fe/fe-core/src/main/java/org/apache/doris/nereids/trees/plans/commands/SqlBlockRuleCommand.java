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

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.SqlBlockUtil;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;

/**
 * Common class for SqlBlockRule Commands.
 */
public abstract class SqlBlockRuleCommand extends Command {
    public static final String SQL_PROPERTY = "sql";

    public static final String SQL_HASH_PROPERTY = "sqlHash";

    public static final String SCANNED_PARTITION_NUM = "partition_num";

    public static final String SCANNED_TABLET_NUM = "tablet_num";

    public static final String SCANNED_CARDINALITY = "cardinality";

    public static final String GLOBAL_PROPERTY = "global";

    public static final String ENABLE_PROPERTY = "enable";

    public static final Long LONG_NOT_SET = SqlBlockUtil.LONG_MINUS_ONE;

    public static final String STRING_NOT_SET = SqlBlockUtil.STRING_DEFAULT;

    private static final ImmutableSet<String> PROPERTIES_SET = new ImmutableSet.Builder<String>().add(SQL_PROPERTY)
                        .add(SQL_HASH_PROPERTY).add(GLOBAL_PROPERTY).add(ENABLE_PROPERTY).add(SCANNED_PARTITION_NUM)
                        .add(SCANNED_TABLET_NUM).add(SCANNED_CARDINALITY).build();

    protected final String ruleName;

    protected String sql;

    protected String sqlHash;

    protected Long partitionNum;

    protected Long tabletNum;

    protected Long cardinality;

    // whether effective global, default is false
    protected Boolean global;

    // whether to use the rule, default is true
    protected Boolean enable;

    protected final Map<String, String> properties;

    /**
    * constructor
    */
    public SqlBlockRuleCommand(String ruleName, Map<String, String> properties, PlanType planType) {
        super(planType);
        this.ruleName = ruleName;
        this.properties = properties;
    }

    private static void checkCommonProperties(Map<String, String> properties) throws UserException {
        if (properties == null || properties.isEmpty()) {
            throw new AnalysisException("Not set properties");
        }
        Optional<String> optional = properties.keySet().stream().filter(entity -> !PROPERTIES_SET.contains(entity))
                .findFirst();
        if (optional.isPresent()) {
            throw new AnalysisException(optional.get() + " is invalid property");
        }
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }
        // check properties
        checkCommonProperties(properties);
        setProperties(properties);
        doRun(ctx, executor);
    }

    public abstract void doRun(ConnectContext ctx, StmtExecutor executor) throws Exception;

    public abstract void setProperties(Map<String, String> properties) throws UserException;
}

