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

package org.apache.doris.flink.cfg;

import com.google.common.base.Preconditions;
import org.apache.flink.configuration.Configuration;


import java.io.Serializable;
import java.util.Properties;

public class FlinkSettings extends Settings implements Serializable {

    private final Configuration cfg;

    public FlinkSettings(Configuration cfg) {
        Preconditions.checkArgument(cfg != null, "non-null flink configuration expected.");
        this.cfg = cfg;
    }

    public FlinkSettings copy() {
        return new FlinkSettings(cfg.clone());
    }

    public String getProperty(String name) {
        return cfg.getString(name,"");
    }

    public void setProperty(String name, String value) {
        cfg.setString(name,value);
    }

    public Properties asProperties() {
        Properties props = new Properties();
        return props;
    }
}
