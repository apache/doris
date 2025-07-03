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

package org.apache.doris.mtmv;

import org.apache.doris.catalog.MTMV;
import org.apache.doris.common.DdlException;

/**
 * The base table of the materialized view should implement this interface and do some things when necessary,
 * but it is currently not mandatory
 */
public interface MTMVBaseTableIf {

    /**
     * Do something before refreshing the MTMV
     *
     * @param mtmv
     * @throws DdlException
     */
    void beforeMTMVRefresh(MTMV mtmv) throws DdlException;
}
