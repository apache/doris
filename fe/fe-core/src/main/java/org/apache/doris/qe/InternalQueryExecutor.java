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

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.thrift.TResultBatch;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.collect.Queues;

public class InternalQueryExecutor extends StmtExecutor {

    private static final Logger LOG = LogManager.getLogger(InternalQueryExecutor.class);
    private static final int QUEUE_WAIT_SECONDS = 1; 
	private volatile BlockingQueue<TResultBatch> resultQueue = Queues.newLinkedBlockingDeque(10);
	private volatile boolean eos = false;
	private volatile Throwable exception = null;
	private boolean isCancelled = false;
	
	public InternalQueryExecutor(ConnectContext context, String stmt) {
		super(context, stmt);
		eos = false;
	}
	
    // query with a random sql
    public void execute() throws Exception {
    	// Start a new thread to execute the real query, and the thread will save the
    	// result batch to resultQueue.
    	// The thread that call execute(), should call getNext to pull resultbatch from
    	// result queue
    	new Thread(new Runnable() {
			@Override
			public void run() {
				try {
					context.setThreadLocalInfo();
					LOG.info("execute is called");
					InternalQueryExecutor.super.execute();
				} catch (Throwable t) {
					LOG.info("execute is error occurred");
					exception = t;
				}
			}
		}).start();
    }

	/**
	 * Get a result batch from queue, return null if the stream already finished
	 */
	public TResultBatch getNext() throws Exception {
		LOG.info("getNext is called");
		while (exception == null && !isCancelled) {
			LOG.info("getNext is called2");
			TResultBatch res = resultQueue.poll(QUEUE_WAIT_SECONDS, TimeUnit.SECONDS);
			if (res != null) {
				return res;
			}
			if (eos) {
				if (resultQueue.isEmpty()) {
					break;
				}
			}
		}
		if (exception != null) {
			throw new Exception(exception);
		}
		return null;
	}
	
	/**
	 * If already pull as many records as need, but the result queue is still not empty
	 * then should call cancel to stop the running thread
	 */
	public void cancel() {
		isCancelled = true;
	}

    // Process a select statement.
    public void handleQueryStmt() throws Exception {
        QueryDetail queryDetail = new QueryDetail(context.getStartTime(),
                DebugUtil.printId(context.queryId()),
                context.getStartTime(), -1, -1,
                QueryDetail.QueryMemState.RUNNING,
                context.getDatabase(),
                originStmt.originStmt);
        context.setQueryDetail(queryDetail);
        QueryDetailQueue.addOrUpdateQueryDetail(queryDetail);
        RowBatch batch;
        coord = new Coordinator(context, analyzer, planner);
        QeProcessorImpl.INSTANCE.registerQuery(context.queryId(),
                new QeProcessorImpl.QueryInfo(context, originStmt.originStmt, coord));
        coord.exec();
        while (!isCancelled) {
            batch = coord.getNext();
            // for outfile query, there will be only one empty batch send back with eos flag
            if (batch.getBatch() != null) {
            	resultQueue.offer(batch.getBatch(), QUEUE_WAIT_SECONDS, TimeUnit.SECONDS);
            }
            if (batch.isEos()) {
            	eos = true;
                break;
            }
        }
        context.getState().setEof();
    }
}
