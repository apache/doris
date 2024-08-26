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

package org.apache.doris.httpv2.rest;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ConfigException;
import org.apache.doris.common.DdlException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * used to set fe config.
 * eg:
 * fe_host:http_port/api/_set_config?config_key1=config_value1&config_key2=config_value2&...
 */
@RestController
public class SetConfigAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(SetConfigAction.class);

    private static final String PERSIST_PARAM = "persist";
    private static final String RESET_PERSIST = "reset_persist";

    @RequestMapping(path = "/api/_set_config", method = RequestMethod.GET)
    protected Object set_config(HttpServletRequest request, HttpServletResponse response) {
        executeCheckPassword(request, response);
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        boolean needPersist = false;
        boolean resetPersist = true;

        Map<String, String[]> tempConfigs = request.getParameterMap();
        Map<String, String[]> configs = Maps.newHashMap();
        configs.putAll(tempConfigs);
        if (configs.containsKey(PERSIST_PARAM)) {
            String[] val = configs.remove(PERSIST_PARAM);
            if (val.length == 1 && val[0].equals("true")) {
                needPersist = true;
            }
        }
        if (configs.containsKey(RESET_PERSIST)) {
            String[] val = configs.remove(RESET_PERSIST);
            if (val.length == 1 && val[0].equals("false")) {
                resetPersist = false;
            }
        }

        Map<String, String> setConfigs = Maps.newHashMap();
        List<ErrConfig> errConfigs = Lists.newArrayList();

        if (LOG.isDebugEnabled()) {
            LOG.debug("get config from url: {}, need persist: {}", configs, needPersist);
        }

        for (Map.Entry<String, String[]> config : configs.entrySet()) {
            String confKey = config.getKey();
            String[] confValue = config.getValue();
            try {
                if (confValue != null && confValue.length == 1) {
                    try {
                        Env.getCurrentEnv().setMutableConfigwithCallback(confKey, confValue[0]);
                    } catch (ConfigException e) {
                        throw new DdlException(e.getMessage());
                    }
                    setConfigs.put(confKey, confValue[0]);
                } else {
                    throw new DdlException("conf value size != 1");
                }
            } catch (DdlException e) {
                LOG.warn("failed to set config {}:{}", confKey, Arrays.toString(confValue), e);
                errConfigs.add(new ErrConfig(confKey, Arrays.toString(confValue), e.getMessage()));
            }
        }

        String persistMsg = "";
        if (needPersist) {
            try {
                ConfigBase.persistConfig(setConfigs, resetPersist);
                persistMsg = "ok";
            } catch (IOException e) {
                LOG.warn("failed to persist config", e);
                persistMsg = e.getMessage();
            }
        }

        return ResponseEntityBuilder.ok(new SetConfigEntity(setConfigs, errConfigs, persistMsg));
    }

    @Setter
    @AllArgsConstructor
    public static class ErrConfig {
        @SerializedName(value = "config_name")
        @JsonProperty("config_name")
        private String configName;
        @SerializedName(value = "config_value")
        @JsonProperty("config_value")
        private String configValue;
        @SerializedName(value = "err_info")
        @JsonProperty("err_info")
        private String errInfo;

        public String getConfigName() {
            return configName;
        }

        public String getConfigValue() {
            return configValue;
        }

        public String getErrInfo() {
            return errInfo;
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    public static class SetConfigEntity {
        @SerializedName(value = "set")
        @JsonProperty("set")
        Map<String, String> setConfigs;
        @SerializedName(value = "err")
        @JsonProperty("err")
        List<ErrConfig> errConfigs;
        @SerializedName(value = "persist")
        @JsonProperty("persist")
        String persistMsg;
    }
}
