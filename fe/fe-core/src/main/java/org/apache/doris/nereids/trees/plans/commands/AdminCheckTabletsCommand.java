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
import org.apache.doris.common.DdlException;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.nereids.trees.plans.PlanType;
import org.apache.doris.nereids.trees.plans.visitor.PlanVisitor;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.qe.StmtExecutor;

import org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * drop user command
 */
public class AdminCheckTabletsCommand extends Command implements ForwardNoSync {
    private static final Logger LOG = LogManager.getLogger(AdminCheckTabletsCommand.class);
    private final List<Long> tabletIds;
    private final Map<String, String> properties;

    /**
     * Check Type in Properties.
     * At present only CONSISTENCY is supported
     */
    public enum CheckType {
        CONSISTENCY; // check the consistency of replicas of tablet
        public static CheckType getTypeFromString(String str) throws AnalysisException {
            try {
                return CheckType.valueOf(str.toUpperCase());
            } catch (Exception e) {
                throw new AnalysisException("Unknown check type: " + str);
            }
        }
    }

    /**
     * constructor
     */
    // ADMIN CHECK TABLET (id1, id2, ...) PROPERTIES ("type" = "check_consistency");
    public AdminCheckTabletsCommand(List<Long> tabletIds, Map<String, String> properties) {
        super(PlanType.ADMIN_CHECK_TABLETS_COMMAND);
        this.tabletIds = tabletIds;
        this.properties = properties;
    }

    @Override
    public void run(ConnectContext ctx, StmtExecutor executor) throws Exception {
        // check auth
        if (!Env.getCurrentEnv().getAccessManager().checkGlobalPriv(ConnectContext.get(), PrivPredicate.ADMIN)) {
            ErrorReport.reportAnalysisException(ErrorCode.ERR_SPECIFIC_ACCESS_DENIED_ERROR, "ADMIN");
        }

        if (CollectionUtils.isEmpty(tabletIds)) {
            throw new AnalysisException("Tablet id list is empty");
        }

        String typeStr = PropertyAnalyzer.analyzeType(properties);
        if (typeStr == null) {
            throw new AnalysisException("Should specify 'type' property");
        }
        CheckType checkType = CheckType.getTypeFromString(typeStr);

        if (properties != null && !properties.isEmpty()) {
            throw new AnalysisException("Unknown properties: " + properties.keySet());
        }

        if (Objects.requireNonNull(checkType) == CheckType.CONSISTENCY) {
            Env.getCurrentEnv().getConsistencyChecker().addTabletsToCheck(tabletIds);
        }
    }

    @Override
    protected void checkSupportedInCloudMode(ConnectContext ctx) throws DdlException {
        LOG.info("AdminCheckTabletsCommand not supported in cloud mode");
        throw new DdlException("Unsupported operation");
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context) {
        return visitor.visitAdminCheckTabletsCommand(this, context);
    }
}
