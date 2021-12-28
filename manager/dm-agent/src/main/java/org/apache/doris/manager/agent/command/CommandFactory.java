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
import org.apache.doris.manager.common.domain.BeStartCommandRequestBody;
import org.apache.doris.manager.common.domain.BrokerInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.BrokerStartCommandRequestBody;
import org.apache.doris.manager.common.domain.CommandRequest;
import org.apache.doris.manager.common.domain.CommandType;
import org.apache.doris.manager.common.domain.FeInstallCommandRequestBody;
import org.apache.doris.manager.common.domain.FeStartCommandRequestBody;
import org.apache.doris.manager.common.domain.WriteBeConfCommandRequestBody;
import org.apache.doris.manager.common.domain.WriteBrokerConfCommandRequestBody;
import org.apache.doris.manager.common.domain.WriteFeConfCommandRequestBody;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Base64;
import java.util.Objects;

public class CommandFactory {
    public static Command get(CommandRequest commandRequest) {
        String body = null;
        if (Objects.nonNull(commandRequest.getBody())) {
            try {
                body = URLDecoder.decode(commandRequest.getBody(), "UTF-8");
            } catch (UnsupportedEncodingException e) {
                e.printStackTrace();
                throw new AgentException(e.getMessage());
            }
        }

        switch (CommandType.valueOf(commandRequest.getCommandType())) {
            case INSTALL_FE:
                validBody(body);
                return new FeInstallCommand(JSON.parseObject(body, FeInstallCommandRequestBody.class));
            case INSTALL_BE:
                validBody(body);
                return new BeInstallCommand(JSON.parseObject(body, BeInstallCommandRequestBody.class));
            case INSTALL_BROKER:
                validBody(body);
                return new BrokerInstallCommand(JSON.parseObject(body, BrokerInstallCommandRequestBody.class));
            case START_FE:
                return new FeStartCommand(JSON.parseObject(body, FeStartCommandRequestBody.class));
            case STOP_FE:
                return new FeStopCommand();
            case START_BE:
                return new BeStartCommand(JSON.parseObject(body, BeStartCommandRequestBody.class));
            case STOP_BE:
                return new BeStopCommand();
            case START_BROKER:
                return new BrokerStartCommand(JSON.parseObject(body, BrokerStartCommandRequestBody.class));
            case STOP_BROKER:
                return new BrokerStopCommand();
            case WRITE_FE_CONF:
                validBody(body);
                WriteFeConfCommandRequestBody writeFeConfCommandRequestBody = JSON.parseObject(body, WriteFeConfCommandRequestBody.class);
                Base64.Decoder decoder = Base64.getDecoder();
                try {
                    writeFeConfCommandRequestBody.setContent(new String(decoder.decode(writeFeConfCommandRequestBody.getContent()), "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                return new WriteFeConfCommand(writeFeConfCommandRequestBody);
            case WRITE_BE_CONF:
                validBody(body);
                WriteBeConfCommandRequestBody writeBeConfCommandRequestBody = JSON.parseObject(body, WriteBeConfCommandRequestBody.class);
                Base64.Decoder decoder2 = Base64.getDecoder();
                try {
                    writeBeConfCommandRequestBody.setContent(new String(decoder2.decode(writeBeConfCommandRequestBody.getContent()), "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                return new WriteBeConfCommand(writeBeConfCommandRequestBody);
            case WRITE_BROKER_CONF:
                validBody(body);
                WriteBrokerConfCommandRequestBody writeBrokerConfCommandRequestBody = JSON.parseObject(body, WriteBrokerConfCommandRequestBody.class);
                Base64.Decoder decoder3 = Base64.getDecoder();
                try {
                    writeBrokerConfCommandRequestBody.setContent(new String(decoder3.decode(writeBrokerConfCommandRequestBody.getContent()), "UTF-8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                return new WriteBrokerConfCommand(writeBrokerConfCommandRequestBody);
            default:
                throw new AgentException("unkown CommandType");
        }
    }

    private static void validBody(String body) {
        if (Objects.isNull(body) || body.length() == 0) {
            throw new AgentException("param body can not be empty");
        }
    }
}
