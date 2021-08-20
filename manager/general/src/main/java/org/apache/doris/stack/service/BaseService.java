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

package org.apache.doris.stack.service;

import org.apache.doris.stack.exception.RequestFieldNullException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BaseService {

    /**
     * Check whether there is an illegal empty field in the request body
     * @param isNull
     * @throws Exception
     */
    protected void checkRequestBody(boolean isNull) throws Exception {
        log.debug("check request body is null");
        if (isNull) {
            log.error("The request body is error,some field can't be null.");
            throw new RequestFieldNullException();
        }
    }
}
