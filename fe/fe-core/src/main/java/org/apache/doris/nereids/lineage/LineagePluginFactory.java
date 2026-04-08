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

package org.apache.doris.nereids.lineage;

import org.apache.doris.extension.spi.PluginContext;
import org.apache.doris.extension.spi.PluginFactory;

/**
 * SPI factory interface for creating {@link LineagePlugin} instances.
 * <p>
 * Extends the generic {@link PluginFactory} from {@code fe-extension-spi}.
 * Implementations are discovered via {@link java.util.ServiceLoader} for built-in plugins
 * and via {@link org.apache.doris.extension.loader.DirectoryPluginRuntimeManager}
 * for external plugins at FE startup.
 * </p>
 */
public interface LineagePluginFactory extends PluginFactory {

    /**
     * Returns the unique name of the plugin this factory creates.
     */
    @Override
    String name();

    /**
     * Creates a new instance of the lineage plugin.
     */
    @Override
    LineagePlugin create();

    /**
     * Creates a new instance of the lineage plugin with runtime context.
     * Default behavior delegates to {@link #create()} for backward compatibility.
     */
    @Override
    default LineagePlugin create(PluginContext context) {
        return create();
    }
}
