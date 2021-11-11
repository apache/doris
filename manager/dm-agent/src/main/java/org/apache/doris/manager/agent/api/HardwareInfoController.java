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

package org.apache.doris.manager.agent.api;

import org.apache.doris.manager.common.domain.HardwareInfo;
import org.apache.doris.manager.common.domain.RResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import oshi.SystemInfo;
import oshi.util.FormatUtil;

/**
 * hardware info
 **/
@RestController
@RequestMapping("/hardware/view")
public class HardwareInfoController {

    @GetMapping
    public RResult info() {
        HardwareInfo hardwareInfo = new HardwareInfo();
        SystemInfo systemInfo = new SystemInfo();
        hardwareInfo.setCpu(systemInfo.getHardware().getProcessor().toString());
        hardwareInfo.setTotalMemory(FormatUtil.formatBytes(systemInfo.getHardware().getMemory().getTotal()));
        return RResult.success(hardwareInfo);
    }
}
