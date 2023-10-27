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

package org.apache.doris.nereids.trees.plans.commands.info;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.PropertyAnalyzer;
import org.apache.doris.qe.ConnectContext;

import java.util.Map;
import java.util.Objects;

/**
 * rename
 */
public class AlterMTMVPropertyInfo extends AlterMTMVInfo {
    private final Map<String, String> properties;

    /**
     * constructor for alter MTMV
     */
    public AlterMTMVPropertyInfo(TableNameInfo mvName, Map<String, String> properties) {
        super(mvName);
        this.properties = Objects.requireNonNull(properties, "require properties object");
    }

    public void analyze(ConnectContext ctx) throws AnalysisException {
        super.analyze(ctx);
        analyzeProperties();
    }

    private void analyzeProperties() {
        for (String key : properties.keySet()) {
            if (PropertyAnalyzer.PROPERTIES_GRACE_PERIOD.equals(key)) {
                String gracePeriod = properties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD);
                try {
                    Long.parseLong(gracePeriod);
                } catch (NumberFormatException e) {
                    throw new org.apache.doris.nereids.exceptions.AnalysisException(
                            "valid grace_period: " + properties.get(PropertyAnalyzer.PROPERTIES_GRACE_PERIOD));
                }
            } else {
                throw new org.apache.doris.nereids.exceptions.AnalysisException("illegal key:" + key);
            }
        }

    }

    @Override
    public void run() throws UserException {
        Env.getCurrentEnv().alterMTMVProperty(this);
    }

    public Map<String, String> getProperties() {
        return properties;
    }
}
