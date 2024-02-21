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

package org.apache.doris.spi;

import org.apache.doris.datasource.SplitWeight;

import java.util.List;

/**
 * Split interface. e.g. Tablet for Olap Table.
 */
public interface Split {

    String[] getHosts();

    Object getInfo();

    default SplitWeight getSplitWeight() {
        return SplitWeight.standard();
    }

    default boolean isRemotelyAccessible() {
        return true;
    }

    String getPathString();

    long getStart();

    long getLength();

    List<String> getAlternativeHosts();

    void setAlternativeHosts(List<String> alternativeHosts);

}
