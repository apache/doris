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

package org.apache.doris.analysis;

import org.apache.doris.common.Id;
import org.apache.doris.common.IdGenerator;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;

public class ExprId extends Id<ExprId> {
    private final static Logger LOG = LogManager.getLogger(ExprId.class);

    // Construction only allowed via an IdGenerator.
    public ExprId(int id) {
        super(id);
    }

    public static IdGenerator<ExprId> createGenerator() {
        return new IdGenerator<ExprId>() {
            @Override
            public ExprId getNextId() { return new ExprId(nextId_++); }
            @Override
            public ExprId getMaxId() { return new ExprId(nextId_ - 1); }
        };
    }
}
