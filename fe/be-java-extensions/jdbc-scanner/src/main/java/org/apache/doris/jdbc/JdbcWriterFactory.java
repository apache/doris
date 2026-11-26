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

package org.apache.doris.jdbc;

import org.apache.doris.jni.spi.JniWriter;
import org.apache.doris.jni.spi.JniWriterFactory;

import java.util.Map;

/** Builds {@link JdbcJniWriter}. BE reaches it as {@code (jdbc, writer)}, paired with reader. */
public class JdbcWriterFactory implements JniWriterFactory {

    public static final String NAME = "writer";

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public JniWriter create(int batchSize, Map<String, String> params) {
        return new JdbcJniWriter(batchSize, params);
    }
}
