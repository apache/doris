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

package org.apache.doris.extension.loader.testplugins;

import org.apache.doris.extension.spi.Plugin;
import org.apache.doris.extension.spi.PluginFactory;

/**
 * Well-behaved test factory; its class bytes are copied into fabricated plugin
 * jars by loader tests.
 */
public class MetadataTestPluginFactory implements PluginFactory {

    @Override
    public String name() {
        return "metadata-test";
    }

    @Override
    public String description() {
        return "Metadata snapshot test plugin";
    }

    @Override
    public Plugin create() {
        return new Plugin() {
        };
    }
}
