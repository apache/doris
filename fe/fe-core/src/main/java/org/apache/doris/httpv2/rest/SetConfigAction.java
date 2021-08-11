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

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ConfigBase.ConfField;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.clearspring.analytics.util.Lists;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 * used to set fe config
 * eg:
 *  fe_host:http_port/api/_set_config?config_key1=config_value1&config_key2=config_value2&...
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

        LOG.debug("get config from url: {}, need persist: {}", configs, needPersist);

        Field[] fields = ConfigBase.confClass.getFields();
        for (Field f : fields) {
            // ensure that field has "@ConfField" annotation
            ConfField anno = f.getAnnotation(ConfField.class);

            if (anno == null) {
                continue;
            }

            // ensure that field has property string
            String confKey = anno.value().equals("") ? f.getName() : anno.value();
            String[] confVals = configs.get(confKey);
            if (confVals == null) {
                continue;
            }

            if (confVals.length != 1) {
                errConfigs.add(new ErrConfig(confKey, "", "No or multiple configuration values."));
                continue;
            }

            if (!anno.mutable()) {
                errConfigs.add(new ErrConfig(confKey, confVals[0], "Not support dynamic modification."));
                continue;
            }

            if (anno.masterOnly() && !Catalog.getCurrentCatalog().isMaster()) {
                errConfigs.add(new ErrConfig(confKey, confVals[0], "Not support modification on non-master"));
                continue;
            }

            try {
                ConfigBase.setConfigField(f, confVals[0]);
            } catch (IllegalArgumentException e){
                errConfigs.add(new ErrConfig(confKey, confVals[0], "Unsupported configuration value type."));
                continue;
            } catch (Exception e) {
                LOG.warn("failed to set config {}:{}, {}", confKey, confVals[0], e.getMessage());
                errConfigs.add(new ErrConfig(confKey, confVals[0], e.getMessage()));
                continue;
            }

            setConfigs.put(confKey, confVals[0]);
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

        List<String> errConfigNames = errConfigs.stream().map(ErrConfig::getConfigName).collect(Collectors.toList());
        for (String key : configs.keySet()) {
            if (!setConfigs.containsKey(key) && !errConfigNames.contains(key)) {
                String[] confVals = configs.get(key);
                String confVal = confVals.length == 1 ? confVals[0] : "invalid value";
                errConfigs.add(new ErrConfig(key, confVal, "invalid config"));
            }
        }

        return ResponseEntityBuilder.ok(new SetConfigEntity(setConfigs, errConfigs, persistMsg));
    }

    @Setter
    @AllArgsConstructor
    public static class ErrConfig{
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
    public static class SetConfigEntity{
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
