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

package org.apache.doris.manager.agent.task;

import org.apache.doris.manager.agent.exception.AgentException;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class TaskHandlerFactory {
    private static Map<Class, ITaskHandlerFactory> factoryInstanceMap = new ConcurrentHashMap<>();

    public static ITaskHandlerFactory getTaskHandlerFactory(Class<? extends ITaskHandlerFactory> className) {
        synchronized (factoryInstanceMap) {
            if (!factoryInstanceMap.containsKey(className)) {
                ITaskHandlerFactory factory = null;
                try {
                    Class<? extends ITaskHandlerFactory> factoryClass = (Class<? extends ITaskHandlerFactory>) Class.forName(className.getName());
                    factory = factoryClass.newInstance();
                    factoryInstanceMap.put(className, factory);
                } catch (Exception e) {
                    e.printStackTrace();
                    throw new AgentException("failed create factory:" + className.getName());
                }
                return factory;
            }
            return factoryInstanceMap.get(className);
        }
    }

}
