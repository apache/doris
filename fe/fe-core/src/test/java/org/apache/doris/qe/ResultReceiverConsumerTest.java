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
import org.apache.doris.common.UserException;
import org.apache.doris.proto.InternalService;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.FutureCallback;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

public class ResultReceiverConsumerTest {

    @Injectable
    private ResultReceiver receiver1;
    @Injectable
    private ResultReceiver receiver2;
    @Injectable
    private ResultReceiver receiver3;

    @Test
    public void testEosHandling() throws Exception {
        ResultReceiverConsumer consumer = new ResultReceiverConsumer(
                Lists.newArrayList(receiver1, receiver2, receiver3), System.currentTimeMillis() + 3600);
        Status status = new Status();

        RowBatch normalBatch1 = new RowBatch();
        normalBatch1.setEos(false);
        RowBatch normalBatch2 = new RowBatch();
        normalBatch2.setEos(false);
        RowBatch normalBatch3 = new RowBatch();
        normalBatch3.setEos(false);
        RowBatch eosBatch1 = new RowBatch();
        eosBatch1.setEos(true);
        RowBatch eosBatch2 = new RowBatch();
        eosBatch2.setEos(true);
        RowBatch eosBatch3 = new RowBatch();
        eosBatch3.setEos(true);

        new Expectations() {
            {
                receiver1.createFuture((FutureCallback<InternalService.PFetchDataResult>) any);
                result = new Delegate() {
                    void delegate(FutureCallback<InternalService.PFetchDataResult> callback) {
                        callback.onSuccess(null);
                    }
                };
                receiver2.createFuture((FutureCallback<InternalService.PFetchDataResult>) any);
                result = new Delegate() {
                    void delegate(FutureCallback<InternalService.PFetchDataResult> callback) {
                        callback.onSuccess(null);
                    }
                };
                receiver3.createFuture((FutureCallback<InternalService.PFetchDataResult>) any);
                result = new Delegate() {
                    void delegate(FutureCallback<InternalService.PFetchDataResult> callback) {
                        callback.onSuccess(null);
                    }
                };

                receiver1.getNext((Status) any);
                result = normalBatch1;
                result = eosBatch1;
                receiver2.getNext((Status) any);
                result = normalBatch2;
                result = eosBatch2;
                receiver3.getNext((Status) any);
                result = normalBatch3;
                result = eosBatch3;
            }
        };
        for (int i = 0; i < 5; i++) {
            RowBatch batch = consumer.getNext(status);
            Assert.assertFalse(consumer.isEos());
            Assert.assertFalse(batch.isEos());
        }

        RowBatch batch = consumer.getNext(status);
        Assert.assertTrue(consumer.isEos());
        Assert.assertTrue(batch.isEos());

    }

    @Test
    public void testGetNextExceptionHandling() throws Exception {
        ResultReceiverConsumer consumer = new ResultReceiverConsumer(
                Lists.newArrayList(receiver1, receiver2, receiver3), System.currentTimeMillis() + 3600);
        Status status = new Status();

        RowBatch normalBatch1 = new RowBatch();
        normalBatch1.setEos(false);

        new Expectations() {
            {
                receiver1.createFuture((FutureCallback<InternalService.PFetchDataResult>) any);
                result = new Delegate() {
                    void delegate(FutureCallback<InternalService.PFetchDataResult> callback) {
                        callback.onSuccess(null);
                    }
                };
                receiver2.createFuture((FutureCallback<InternalService.PFetchDataResult>) any);
                result = new Delegate() {
                    void delegate(FutureCallback<InternalService.PFetchDataResult> callback) {
                        callback.onSuccess(null);
                    }
                };
                receiver3.createFuture((FutureCallback<InternalService.PFetchDataResult>) any);
                result = new Delegate() {
                    void delegate(FutureCallback<InternalService.PFetchDataResult> callback) {
                        callback.onSuccess(null);
                    }
                };

                receiver1.getNext((Status) any);
                result = normalBatch1;

                receiver2.getNext((Status) any);
                result = new TException("Network error");
            }
        };
        RowBatch batch = consumer.getNext(status);
        Assert.assertFalse(batch.isEos());
        Assertions.assertThrows(TException.class, () -> consumer.getNext(status));
    }

    @Test
    public void testCreateFutureExceptionHandling() throws Exception {
        ResultReceiverConsumer consumer = new ResultReceiverConsumer(
                Lists.newArrayList(receiver1, receiver2, receiver3), System.currentTimeMillis() + 3600);
        Status status = new Status();

        new Expectations() {
            {
                receiver1.createFuture((FutureCallback<InternalService.PFetchDataResult>) any);
                result = new Delegate() {
                    void delegate(FutureCallback<InternalService.PFetchDataResult> callback) {
                        callback.onSuccess(null);
                    }
                };
                receiver2.createFuture((FutureCallback<InternalService.PFetchDataResult>) any);
                result = new Delegate() {
                    void delegate(FutureCallback<InternalService.PFetchDataResult> callback) throws UserException {
                        throw new UserException("User error");
                    }
                };
            }
        };
        Assertions.assertThrows(UserException.class, () -> consumer.getNext(status));
    }
}
