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
package org.apache.doris.manager.agent.command;

import com.alibaba.fastjson.JSON;
import org.apache.doris.manager.agent.exception.AgentException;
import org.apache.doris.manager.common.domain.BeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.CommandRequest;
import org.apache.doris.manager.common.domain.CommandType;
import org.apache.doris.manager.common.domain.FeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.FeStartCommandRequestBody;
import org.apache.logging.log4j.util.Strings;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;

public class CommandFactory {
    public static Command get(CommandRequest commandRequest) {
        String body = null;
        if (Strings.isNotBlank(commandRequest.getBody())) {
            try {
                body = URLDecoder.decode(commandRequest.getBody(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                throw new AgentException(e.getMessage());
            }
        }

        switch (CommandType.valueOf(commandRequest.getCommandType())) {
            case INSTALL_FE:
                FeInstallCommandRequestBody feInstallCommandRequestBody = JSON.parseObject(body, FeInstallCommandRequestBody.class);
                return new FeInstallCommand(feInstallCommandRequestBody);
            case INSTALL_BE:
                BeInstallCommandRequestBody beInstallCommandRequestBody = JSON.parseObject(body, BeInstallCommandRequestBody.class);
                return new BeInstallCommand(beInstallCommandRequestBody);
            case START_FE:
                FeStartCommandRequestBody feStartCommandRequestBody = JSON.parseObject(body, FeStartCommandRequestBody.class);
                return new FeStartCommand(feStartCommandRequestBody);
            case STOP_FE:
                return new FeStopCommand();
            case START_BE:
                return new BeStartCommand();
            case STOP_BE:
                return new BeStopCommand();
            default:
                throw new AgentException("unkown CommandType");
        }
    }
}
