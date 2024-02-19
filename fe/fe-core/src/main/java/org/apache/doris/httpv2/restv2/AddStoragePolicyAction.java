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

package org.apache.doris.httpv2.restv2;

import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.httpv2.rest.RestBaseController;
import org.apache.doris.policy.StoragePolicy;

import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
@RequestMapping("/rest/v2")
public class AddStoragePolicyAction extends RestBaseController {

    private static final Logger LOG = LogManager.getLogger(AddStoragePolicyAction.class);

    @RequestMapping(path = "/api/storage_policy", method = RequestMethod.POST)
    public Object clusterOverview(@RequestBody StoragePolicyVo body,
            HttpServletRequest request, HttpServletResponse response) {
        if (Config.enable_all_http_auth) {
            executeCheckPassword(request, response);
        }

        try {
            if (!Env.getCurrentEnv().isMaster()) {
                return redirectToMasterOrException(request, response);
            }
        } catch (Exception e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }

        StoragePolicy storagePolicy = new StoragePolicy(body.getPolicyId(), body.getPolicyName());
        try {
            storagePolicy.init(body.toPropertiesMap(), true);
            Env.getCurrentEnv().getPolicyMgr().addPolicy(storagePolicy);
            return ResponseEntityBuilder.ok();
        } catch (UserException e) {
            return ResponseEntityBuilder.okWithCommonError(e.getMessage());
        }
    }

    @Getter
    @Setter
    public static class StoragePolicyVo {
        private String policyName;
        private long policyId;
        private String storageResource;
        private long cooldownTimestampMs;
        private long cooldownTtl;

        public Map<String, String> toPropertiesMap() {
            Map<String, String> properties = Maps.newHashMap();
            properties.put("storage_resource", storageResource);
            properties.put("cooldown_datetime", Long.toString(cooldownTimestampMs));
            properties.put("cooldown_ttl", Long.toString(cooldownTtl));
            return properties;
        }
    }
}
