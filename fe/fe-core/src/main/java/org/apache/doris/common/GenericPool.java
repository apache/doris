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

import org.apache.doris.thrift.TNetworkAddress;

import org.apache.commons.pool2.BaseKeyedPooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericKeyedObjectPool;
import org.apache.commons.pool2.impl.GenericKeyedObjectPoolConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.apache.thrift.transport.layered.TFramedTransport;

import java.lang.reflect.Constructor;

public class GenericPool<VALUE extends org.apache.thrift.TServiceClient>  {
    private static final Logger LOG = LogManager.getLogger(GenericPool.class);
    private GenericKeyedObjectPool<TNetworkAddress, VALUE> pool;
    private String className;
    private int timeoutMs;
    private boolean isNonBlockingIO;

    public GenericPool(String className, GenericKeyedObjectPoolConfig config, int timeoutMs, boolean isNonBlockingIO) {
        this.className = "org.apache.doris.thrift." + className + "$Client";
        ThriftClientFactory factory = new ThriftClientFactory();
        pool = new GenericKeyedObjectPool<>(factory, config);
        this.timeoutMs = timeoutMs;
        this.isNonBlockingIO = isNonBlockingIO;
    }

    public GenericPool(String className, GenericKeyedObjectPoolConfig config, int timeoutMs) {
        this(className, config, timeoutMs, false);
    }

    public boolean reopen(VALUE object, int timeoutMs) {
        boolean ok = true;
        object.getOutputProtocol().getTransport().close();
        try {
            object.getOutputProtocol().getTransport().open();
            // transport.open() doesn't set timeout, Maybe the timeoutMs change.
            // here we cannot set timeoutMs for TFramedTransport, just skip it
            if (!isNonBlockingIO) {
                TSocket socket = (TSocket) object.getOutputProtocol().getTransport();
                socket.setTimeout(timeoutMs);
            }
        } catch (TTransportException e) {
            ok = false;
        }
        return ok;
    }

    public boolean reopen(VALUE object) {
        boolean ok = true;
        object.getOutputProtocol().getTransport().close();
        try {
            object.getOutputProtocol().getTransport().open();
        } catch (TTransportException e) {
            LOG.warn("reopen error", e);
            ok = false;
        }
        return ok;
    }

    public void clearPool(TNetworkAddress addr) {
        pool.clear(addr);
    }

    public boolean peak(VALUE object) {
        return object.getOutputProtocol().getTransport().peek();
    }

    public VALUE borrowObject(TNetworkAddress address) throws Exception {
        return pool.borrowObject(address);
    }

    public VALUE borrowObject(TNetworkAddress address, int timeoutMs) throws Exception {
        VALUE value = pool.borrowObject(address);
        // here we cannot set timeoutMs for TFramedTransport, just skip it
        if (!isNonBlockingIO) {
            TSocket socket = (TSocket) (value.getOutputProtocol().getTransport());
            socket.setTimeout(timeoutMs);
        }
        return value;
    }

    public void returnObject(TNetworkAddress address, VALUE object) {
        if (address == null || object == null) {
            return;
        }
        pool.returnObject(address, object);
    }

    public void invalidateObject(TNetworkAddress address, VALUE object) {
        if (address == null || object == null) {
            return;
        }
        try {
            pool.invalidateObject(address, object);
        } catch (Exception e) {
            LOG.warn("failed to invalidate object. address: {}", address.toString(), e);
        }
    }

    private class ThriftClientFactory extends BaseKeyedPooledObjectFactory<TNetworkAddress, VALUE> {

        private Object newInstance(String className, TProtocol protocol) throws Exception {
            Class newoneClass = Class.forName(className);
            Constructor cons = newoneClass.getConstructor(TProtocol.class);
            return cons.newInstance(protocol);
        }

        @Override
        public VALUE create(TNetworkAddress key) throws Exception {
            if (LOG.isDebugEnabled()) {
                LOG.debug("before create socket hostname={} key.port={} timeoutMs={}",
                        key.hostname, key.port, timeoutMs);
            }
            TTransport transport = isNonBlockingIO
                    ? new TFramedTransport(new TSocket(key.hostname, key.port, timeoutMs))
                    : new TSocket(key.hostname, key.port, timeoutMs);
            transport.open();
            TProtocol protocol = new TBinaryProtocol(transport);
            VALUE client = (VALUE) newInstance(className, protocol);
            return client;
        }

        @Override
        public PooledObject<VALUE> wrap(VALUE client) {
            return new DefaultPooledObject<VALUE>(client);
        }

        @Override
        public boolean validateObject(TNetworkAddress key, PooledObject<VALUE> p) {
            boolean isOpen = p.getObject().getOutputProtocol().getTransport().isOpen();
            if (LOG.isDebugEnabled()) {
                LOG.debug("isOpen={}", isOpen);
            }
            return isOpen;
        }

        @Override
        public void destroyObject(TNetworkAddress key, PooledObject<VALUE> p) {
            // InputProtocol and OutputProtocol have the same reference in OurCondition
            if (p.getObject().getOutputProtocol().getTransport().isOpen()) {
                p.getObject().getOutputProtocol().getTransport().close();
            }
        }
    }
}
