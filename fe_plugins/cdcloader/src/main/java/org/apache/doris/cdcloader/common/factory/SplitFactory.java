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

package org.apache.doris.cdcloader.common.factory;

import org.apache.doris.cdcloader.mysql.loader.MySqlSplitAssigner;
import org.apache.doris.cdcloader.mysql.loader.MySqlSplitReader;
import org.apache.doris.cdcloader.mysql.loader.SplitAssigner;
import org.apache.doris.cdcloader.mysql.loader.SplitReader;
import org.apache.doris.cdcloader.mysql.config.LoaderOptions;

public class SplitFactory {

    public static SplitAssigner createSplitAssigner(DataSource source, LoaderOptions options) {
        switch (source){
            case MYSQL:
                return new MySqlSplitAssigner(options);
            default:
                throw new IllegalArgumentException("Unsupported SplitAssigner with datasource : " + source);
        }
    }

    public static SplitReader createSplitReader(DataSource source, LoaderOptions options, SplitAssigner splitAssigner) {
        switch (source){
            case MYSQL:
                return new MySqlSplitReader(options, splitAssigner);
            default:
                throw new IllegalArgumentException("Unsupported SplitAssigner with datasource : " + source);
        }
    }

}
