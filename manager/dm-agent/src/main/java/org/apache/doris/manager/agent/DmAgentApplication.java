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

package org.apache.doris.manager.agent;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.StdDateFormat;
import lombok.extern.slf4j.Slf4j;
import org.apache.doris.manager.agent.common.PropertiesUtil;
import org.apache.doris.manager.agent.register.AgentContext;
import org.apache.doris.manager.agent.register.AgentHeartbeat;
import org.apache.doris.manager.agent.register.ApplicationOption;
import org.apache.doris.manager.agent.service.ServiceContext;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.system.ApplicationHome;
import org.springframework.context.annotation.Bean;
import org.springframework.http.MediaType;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@SpringBootApplication
@Slf4j
public class DmAgentApplication {

    public static void main(String[] args) {
        ApplicationOption option = new ApplicationOption();
        CmdLineParser parser = new CmdLineParser(option);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            e.printStackTrace();
            return;
        }

        String agentPort = PropertiesUtil.getPropValue("server.port");
        ApplicationHome applicationHome = new ApplicationHome(DmAgentApplication.class);
        String agentInstallDir = applicationHome.getSource().getParentFile().getParentFile().toString();

        boolean agentRegisterSuccess = AgentContext.register(option.agentIp, Integer.valueOf(agentPort), option.agentServer, agentInstallDir);
        if (!agentRegisterSuccess) {
            log.error("agent register fail");
            return;
        }

        boolean serviceRegisterSuccess = ServiceContext.registerFromServer(option.agentServer, option.agentIp);
        if (!serviceRegisterSuccess) {
            log.error("service register fail");
            return;
        }

        SpringApplication.run(DmAgentApplication.class, args);
        AgentHeartbeat.start();
    }

    @Bean
    public MappingJackson2HttpMessageConverter mappingJackson2HttpMessageConverter() {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss") {
            @Override
            public Date parse(String source) {
                try {
                    return super.parse(source);
                } catch (Exception e) {
                    try {
                        return new StdDateFormat().parse(source);
                    } catch (ParseException e1) {
                        throw new RuntimeException("Date format is illegal:" + e);
                    }
                }
            }
        };

        MappingJackson2HttpMessageConverter jsonConverter = new MappingJackson2HttpMessageConverter();
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.setDateFormat(dateFormat);
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        jsonConverter.setObjectMapper(objectMapper);
        List<MediaType> list = new ArrayList<>();
        list.add(MediaType.APPLICATION_JSON_UTF8);
        jsonConverter.setSupportedMediaTypes(list);
        return jsonConverter;
    }
}
