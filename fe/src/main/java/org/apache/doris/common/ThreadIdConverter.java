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

package org.apache.doris.common;

import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.pattern.ConverterKeys;
import org.apache.logging.log4j.core.pattern.LogEventPatternConverter;

@Plugin(name = "threadIdConverter", category = "Converter")
@ConverterKeys({ "i", "tid" })
public class ThreadIdConverter extends LogEventPatternConverter {
    protected ThreadIdConverter(final String[] options) {
        super("threadIdConverter", "");
    }

    public static ThreadIdConverter newInstance(final String[] options) {
        return new ThreadIdConverter(options);
    }

    @Override
    public void format(LogEvent arg0, StringBuilder arg1) {
        arg1.append(Thread.currentThread().getId());
    }
}
