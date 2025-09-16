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

package org.apache.doris.common;

import org.apache.thrift.TBase;
import org.apache.thrift.TConfiguration;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

// Utility functions for thrift
public class ThriftUtils {
    // Get the size of the binary message of the thrift object
    public static long getBinaryMessageSize(TBase<?, ?> thriftObject) {
        TSizeTransport trans = new TSizeTransport();
        TBinaryProtocol protocol = new TBinaryProtocol(trans);
        try {
            thriftObject.write(protocol);
        } catch (TException e) {
            return -1;
        }
        return trans.getSize();
    }

    // A transport class that only records the size of the message
    private static class TSizeTransport extends TTransport {
        private long size = 0;

        public long getSize() {
            return size;
        }

        @Override
        public void write(byte[] buf, int off, int len) throws TTransportException {
            size += len;
        }

        @Override
        public boolean isOpen() {
            return true;
        }

        @Override
        public void open() throws TTransportException {
        }

        @Override
        public void close() {
        }

        @Override
        public int read(byte[] buf, int off, int len) throws TTransportException {
            throw new UnsupportedOperationException("Unimplemented method 'read'");
        }

        @Override
        public TConfiguration getConfiguration() {
            return new TConfiguration();
        }

        @Override
        public void updateKnownMessageSize(long size) throws TTransportException {
        }

        @Override
        public void checkReadBytesAvailable(long numBytes) throws TTransportException {
        }
    }
}
