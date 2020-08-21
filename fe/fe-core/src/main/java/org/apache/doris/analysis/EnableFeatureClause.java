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

import org.apache.doris.alter.AlterOpType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.UserException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class EnableFeatureClause extends AlterTableClause {
    private static final Logger LOG = LogManager.getLogger(EnableFeatureClause.class);

    public enum Features {
        BATCH_DELETE,
        UNKNOWN
    }

    private String featureName;
    private boolean needSchemaChange;
    private Features feature;

    public EnableFeatureClause(String featureName) {
        super(AlterOpType.ENABLE_FEATURE);
        this.featureName = featureName;
        this.needSchemaChange = false;
    }

    public boolean needSchemaChange() {
        return needSchemaChange;
    }

    public Features getFeature() {
        return feature;
    }

    @Override
    public void analyze(Analyzer analyzer) throws UserException {
        switch (featureName.toUpperCase()) {
            case "BATCH_DELETE":
                this.needSchemaChange = true;
                this.feature = Features.BATCH_DELETE;
                break;
            default:
                throw new AnalysisException("unknown feature name: " + featureName);
        }
    }

    @Override
    public String toSql() {
        StringBuilder sb = new StringBuilder();
        sb.append("ENABLE FEATURE \"").append(featureName).append("\"");
        return sb.toString();
    }
}
