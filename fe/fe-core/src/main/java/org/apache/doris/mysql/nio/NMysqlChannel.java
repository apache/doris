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
package org.apache.doris.mysql.nio;

import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.qe.ConnectProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xnio.StreamConnection;
import org.xnio.channels.Channels;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

/**
 * mysql Channel based on nio.
 */
public class NMysqlChannel extends MysqlChannel {
    protected final Logger LOG = LogManager.getLogger(this.getClass());
    private StreamConnection conn;

    public NMysqlChannel(StreamConnection connection) {
        super();
        this.conn = connection;
        if (connection.getPeerAddress() instanceof InetSocketAddress) {
            InetSocketAddress address = (InetSocketAddress) connection.getPeerAddress();
            remoteHostPortString = address.getHostString() + ":" + address.getPort();
            remoteIp = address.getAddress().getHostAddress();
        } else {
            // Reach here, what's it?
            remoteHostPortString = connection.getPeerAddress().toString();
            remoteIp = connection.getPeerAddress().toString();
        }
    }

    /**
     * read packet until whole dstBuf is filled, unless block.
     * Todo: find a better way to avoid block read here.
     *
     * @param dstBuf
     * @return
     */
    @Override
    protected int readAll(ByteBuffer dstBuf) {
        int readLen = 0;
        try {
            while (dstBuf.remaining() != 0) {
                int ret = Channels.readBlocking(conn.getSourceChannel(), dstBuf);
                // return -1 when remote peer close the channel
                if (ret == -1) {
                    return readLen;
                }
                readLen += ret;
            }
        } catch (IOException e) {
            LOG.debug("Read channel exception, ignore.", e);
            return 0;
        }
        return readLen;
    }

    /**
     * write packet until no data is remained, unless block.
     *
     * @param buffer
     * @throws IOException
     */
    @Override
    protected void realNetSend(ByteBuffer buffer) throws IOException {
        long bufLen = buffer.remaining();
        long writeLen = Channels.writeBlocking(conn.getSinkChannel(), buffer);
        if (bufLen != writeLen) {
            throw new IOException("Write mysql packet failed.[write=" + writeLen
                    + ", needToWrite=" + bufLen + "]");
        }
        Channels.flushBlocking(conn.getSinkChannel());
        isSend = true;
    }

    @Override
    public void close() {
        try {
            conn.close();
        } catch (IOException e) {
            LOG.warn("Close channel exception, ignore.");
        }
    }

    public void startAcceptQuery(NConnectContext nConnectContext, ConnectProcessor connectProcessor) {
        conn.getSourceChannel().setReadListener(new ReadListener(nConnectContext, connectProcessor));
        conn.getSourceChannel().resumeReads();
    }

    public void suspendAcceptQuery() {
        conn.getSourceChannel().suspendReads();
    }

    public void resumeAcceptQuery() {
        conn.getSourceChannel().resumeReads();
    }

    public void stopAcceptQuery() throws IOException {
        conn.getSourceChannel().shutdownReads();
    }
}
