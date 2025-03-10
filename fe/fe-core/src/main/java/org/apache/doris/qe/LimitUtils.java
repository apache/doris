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

package org.apache.doris.qe;

import org.apache.doris.common.Status;
import org.apache.doris.thrift.TStatusCode;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.function.Consumer;

/**
 * This is a utility class for limit related operations.
 * Because current there are 2 places need to check limit rows, so put the logic here for unification.
 *  - Coordinator.getNext();
 *  - QueryProcessor.getNext();
 */
public class LimitUtils {
    private static final Logger LOG = LogManager.getLogger(LimitUtils.class);
    private static final Status LIMIT_REACH_STATUS = new Status(TStatusCode.LIMIT_REACH, "query reach limit");

    // if reached limit rows, cancel this query immediately
    // to avoid BE from reading more data.
    public static boolean cancelIfReachLimit(RowBatch resultBatch, long limitRows, long numReceivedRows,
            Consumer<Status> cancelFunc) {
        boolean reachedLimit = false;
        if (limitRows > 0 && numReceivedRows >= limitRows) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("reach limit rows: {}, received rows: {}, cancel query", limitRows, numReceivedRows);
            }
            cancelFunc.accept(LIMIT_REACH_STATUS);
            // set this
            resultBatch.setEos(true);
            reachedLimit = true;
        }
        return reachedLimit;
    }
}
