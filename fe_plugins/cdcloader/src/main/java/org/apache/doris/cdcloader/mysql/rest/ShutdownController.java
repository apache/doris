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

package org.apache.doris.cdcloader.mysql.rest;

import org.apache.doris.cdcloader.common.rest.ResponseEntityBuilder;
import org.apache.doris.cdcloader.mysql.loader.LoadContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class ShutdownController extends BaseController implements ApplicationContextAware {
    private static final Logger LOG = LoggerFactory.getLogger(ShutdownController.class);

    private ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.context = applicationContext;
    }

    @PostMapping("/api/shutdown")
    public Object shutdownContext(@RequestParam("jobId") long jobId) {
        if(!checkJobId(jobId)){
            return ResponseEntityBuilder.jobIdInconsistent();
        }
        new Thread(() -> {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            LOG.info("shutdown cdc process...");
            LoadContext.getInstance().close();
            ((ConfigurableApplicationContext) context).close();
            System.exit(0);
        }).start();
        return ResponseEntityBuilder.ok();
    }
}
