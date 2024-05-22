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

package org.apache.doris.cdcloader.mysql.config;

import org.apache.commons.lang3.StringUtils;
import org.apache.doris.cdcloader.mysql.constants.LoadConstants;
import org.apache.doris.cdcloader.mysql.utils.LoaderUtil;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfig;
import org.apache.flink.cdc.connectors.mysql.source.config.MySqlSourceConfigFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LoaderOptions {

    private final Long jobId;

    private final String frontends;

    private final Map<String, String> cdcConfig;

    public LoaderOptions(Long jobId, String frontends, Map<String, String> cdcConfig) {
        this.jobId = jobId;
        this.frontends = frontends;
        this.cdcConfig = cdcConfig;
    }

    public Long getJobId() {
        return jobId;
    }

    public String getFrontends() {
        return frontends;
    }

    public Map<String, String> getCdcConfig() {
        return cdcConfig;
    }

    public MySqlSourceConfig generateMySqlConfig() {
        Map<String, String> cdcConfig = this.getCdcConfig();
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        configFactory.hostname(cdcConfig.get(LoadConstants.HOSTNAME));
        configFactory.port(Integer.valueOf(cdcConfig.get(LoadConstants.PORT)));
        configFactory.username(cdcConfig.get(LoadConstants.USERNAME));
        configFactory.password(cdcConfig.get(LoadConstants.PASSWORD));
        configFactory.databaseList(cdcConfig.get(LoadConstants.DATABASE_NAME));
        configFactory.tableList(cdcConfig.get(LoadConstants.TABLE_NAME));
        return configFactory.createConfig(0);
    }

    public String getAvailableFrontend(){
        if(StringUtils.isBlank(frontends)){
            return null;
        }
        List<String> frontendLists = Arrays.asList(frontends.split(","));
        Collections.shuffle(frontendLists);
        for(String frontend : frontendLists){
            if(LoaderUtil.tryHttpConnection(frontend)){
                return frontend;
            }
        }
        return null;
    }
}
