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

package org.apache.doris.encryption;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Iterator;
import java.util.ServiceLoader;

public class KeyManagerFactory {
    public static final Logger LOG = LogManager.getLogger(KeyManagerFactory.class);

    public static KeyManagerInterface getKeyManager() {
        ServiceLoader<KeyManagerInterface> loader = ServiceLoader.load(KeyManagerInterface.class);
        Iterator<KeyManagerInterface> it = loader.iterator();
        if (it.hasNext()) {
            return it.next();
        } else {
            LOG.warn("no KeyManager implementation found");
            return null;
        }
    }
}
