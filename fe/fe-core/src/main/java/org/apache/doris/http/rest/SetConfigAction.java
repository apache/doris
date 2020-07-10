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

package org.apache.doris.http.rest;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.common.ConfigBase;
import org.apache.doris.common.ConfigBase.ConfField;
import org.apache.doris.common.DdlException;
import org.apache.doris.http.entity.HttpStatus;
import org.apache.doris.http.entity.ResponseEntity;
import org.apache.doris.mysql.privilege.PrivPredicate;
import org.apache.doris.qe.ConnectContext;

import com.google.common.collect.Maps;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.lang.reflect.Field;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/*
 * used to set fe config
 * eg:
 *  fe_host:http_port/api/_set_config?config_key1=config_value1&config_key2=config_value2&...
 */
public class SetConfigAction extends RestBaseController {
    private static final Logger LOG = LogManager.getLogger(SetConfigAction.class);

    @RequestMapping(path = "/api/_set_config",method = RequestMethod.GET)
    protected Object set_config(HttpServletRequest request, HttpServletResponse response) throws DdlException {
        executeCheckPassword(request,response);
        ResponseEntity entity = ResponseEntity.status(HttpStatus.OK).build("Success");
        checkGlobalAuth(ConnectContext.get().getCurrentUserIdentity(), PrivPredicate.ADMIN);

        Map<String, String[]> configs = request.getParameterMap();
        Map<String, String> setConfigs = Maps.newHashMap();
        Map<String, String> errConfigs = Maps.newHashMap();

        LOG.debug("get config from url: {}", configs);

        Field[] fields = ConfigBase.confClass.getFields();
        for (Field f : fields) {
            // ensure that field has "@ConfField" annotation
            ConfField anno = f.getAnnotation(ConfField.class);
            if (anno == null || !anno.mutable()) {
                continue;
            }

            if (anno.masterOnly() && !Catalog.getCurrentCatalog().isMaster()) {
                continue;
            }

            // ensure that field has property string
            String confKey = anno.value().equals("") ? f.getName() : anno.value();
            String[] confVals = configs.get(confKey);
            if (confVals == null || confVals.length == 0) {
                continue;
            }

            if (confVals.length > 1) {
                continue;
            }

            try {
                ConfigBase.setConfigField(f,confVals[0] );
            } catch (Exception e) {
                LOG.warn("failed to set config {}:{}", confKey, confVals[0],  e);
                continue;
            }

            setConfigs.put(confKey, confVals[0]);
        }

        for (String key : configs.keySet()) {
            if (!setConfigs.containsKey(key)) {
                errConfigs.put(key, configs.get(key).toString());
            }
        }

        Map<String, Map<String, String>> resultMap = Maps.newHashMap();
        resultMap.put("set", setConfigs);
        resultMap.put("err", errConfigs);

        entity.setData(resultMap);
        return entity;
    }

    public static void print(String msg) {
        System.out.println(System.currentTimeMillis() + " " + msg);
    }
}
