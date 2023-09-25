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
// This file is copied from
// https://github.com/dremio/dremio-oss/blob/master/services/arrow-flight/src/main/java/com/dremio/service/flight/DremioFlightSessionsManager.java
// and modified by Doris

package org.apache.doris.service.arrowflight.sessions;

/**
 * Manages UserSession creation and UserSession cache.
 */
public interface FlightSessionsManager extends AutoCloseable {

    /**
     * Resolves an existing UserSession for the given token.
     * <p>
     *
     * @param peerIdentity identity after authorization
     * @return The UserSession or null if no sessionId is given.
     */
    FlightUserSession getUserSession(String peerIdentity);

    /**
     * Creates a UserSession object and store it in the local cache, assuming that peerIdentity was already validated.
     *
     * @param peerIdentity identity after authorization
     */
    FlightUserSession createUserSession(String peerIdentity);
}
