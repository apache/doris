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

package org.apache.doris.stack.model.response;

import lombok.Data;
import org.apache.doris.stack.constants.ExecutionStatus;
import org.apache.doris.stack.constants.ProcessTypeEnum;

import javax.persistence.EnumType;
import javax.persistence.Enumerated;
import java.util.Date;

/**
 * current process resp
 **/
@Data
public class CurrentProcessResp {

    private int id;

    private int clusterId;

    private int processStep;

    @Enumerated(EnumType.STRING)
    private ProcessTypeEnum processType;

    @Enumerated(EnumType.STRING)
    private ExecutionStatus status;

    private Date createTime;

    private Date updateTime;
}
