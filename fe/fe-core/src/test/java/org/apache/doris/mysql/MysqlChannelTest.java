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

package org.apache.doris.mysql;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;

public class MysqlChannelTest {
    int packetId = 0;
    int readIdx = 0;
    @Mocked
    private SocketChannel channel;

    @Before
    public void setUp() throws IOException {
        packetId = 0;
        readIdx = 0;
        new Expectations() {
            {
                channel.getRemoteAddress();
                minTimes = 0;
                result = new InetSocketAddress(1024);
            }
        };
    }

    @Test
    public void testReceive() throws IOException {
        // mock
        new Expectations() {
            {
                channel.read((ByteBuffer) any);
                minTimes = 0;
                result = new Delegate() {
                    int fakeRead(ByteBuffer buffer) {
                        MysqlSerializer serializer = MysqlSerializer.newInstance();
                        if (readIdx == 0) {
                            readIdx++;
                            serializer.writeInt3(10);
                            serializer.writeInt1(packetId++);

                            buffer.put(serializer.toArray());
                            return 4;
                        } else if (readIdx == 1) {
                            readIdx++;
                            byte[] buf = new byte[buffer.remaining()];
                            for (int i = 0; i < buffer.remaining(); ++i) {
                                buf[i] = (byte) ('a' + i);

                            }
                            buffer.put(buf);
                            return 10;
                        }
                        return -1;
                    }
                };
            }
        };

        MysqlChannel channel1 = new MysqlChannel(channel);

        ByteBuffer buf = channel1.fetchOnePacket();
        Assert.assertEquals(10, buf.remaining());
        for (int i = 0; i < 10; ++i) {
            Assert.assertEquals('a' + i, buf.get());
        }
    }

    @Test
    public void testLongPacket() throws IOException {
        // mock
        new Expectations() {
            {
                channel.read((ByteBuffer) any);
                minTimes = 0;
                result = new Delegate() {
                    int fakeRead(ByteBuffer buffer) {
                        int maxLen = MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH;
                        MysqlSerializer serializer = MysqlSerializer.newInstance();
                        if (readIdx == 0) {
                            // packet
                            readIdx++;
                            serializer.writeInt3(maxLen);
                            serializer.writeInt1(packetId++);

                            buffer.put(serializer.toArray());
                            return 4;
                        } else if (readIdx == 1) {
                            readIdx++;
                            int readLen = buffer.remaining();
                            byte[] buf = new byte[readLen];
                            for (int i = 0; i < readLen; ++i) {
                                buf[i] = (byte) ('a' + (i % 26));

                            }
                            buffer.put(buf);
                            return readLen;
                        } else if (readIdx == 2) {
                            // packet
                            readIdx++;
                            serializer.writeInt3(10);
                            serializer.writeInt1(packetId++);

                            buffer.put(serializer.toArray());
                            return 4;
                        } else if (readIdx == 3) {
                            readIdx++;
                            int readLen = buffer.remaining();
                            byte[] buf = new byte[readLen];
                            for (int i = 0; i < readLen; ++i) {
                                buf[i] = (byte) ('a' + (maxLen + i) % 26);

                            }
                            buffer.put(buf);
                            return readLen;
                        }
                        return 0;
                    }
                };
            }
        };

        MysqlChannel channel1 = new MysqlChannel(channel);

        ByteBuffer buf = channel1.fetchOnePacket();
        Assert.assertEquals(MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH + 10, buf.remaining());
        for (int i = 0; i < MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH + 10; ++i) {
            Assert.assertEquals('a' + (i % 26), buf.get());
        }
    }

    @Test(expected = IOException.class)
    public void testBadSeq() throws IOException {
        // mock
        new Expectations() {
            {
                channel.read((ByteBuffer) any);
                minTimes = 0;
                result = new Delegate() {
                    int fakeRead(ByteBuffer buffer) {
                        int maxLen = MysqlChannel.MAX_PHYSICAL_PACKET_LENGTH;
                        MysqlSerializer serializer = MysqlSerializer.newInstance();
                        if (readIdx == 0) {
                            // packet
                            readIdx++;
                            serializer.writeInt3(maxLen);
                            serializer.writeInt1(packetId++);

                            buffer.put(serializer.toArray());
                            return 4;
                        } else if (readIdx == 1) {
                            readIdx++;
                            int readLen = buffer.remaining();
                            byte[] buf = new byte[readLen];
                            for (int i = 0; i < readLen; ++i) {
                                buf[i] = (byte) ('a' + (i % 26));

                            }
                            buffer.put(buf);
                            return readLen;
                        } else if (readIdx == 2) {
                            // packet
                            readIdx++;
                            serializer.writeInt3(10);
                            // NOTE: Bad packet seq
                            serializer.writeInt1(0);

                            buffer.put(serializer.toArray());
                            return 4;
                        } else if (readIdx == 3) {
                            readIdx++;
                            byte[] buf = new byte[buffer.remaining()];
                            for (int i = 0; i < buffer.remaining(); ++i) {
                                buf[i] = (byte) ('a' + (i % 26));

                            }
                            buffer.put(buf);
                            return buffer.remaining();
                        }
                        return 0;
                    }
                };
            }
        };

        MysqlChannel channel1 = new MysqlChannel(channel);

        ByteBuffer buf = channel1.fetchOnePacket();
    }

    @Test(expected = IOException.class)
    public void testException() throws IOException {
        // mock
        new Expectations() {
            {
                channel.read((ByteBuffer) any);
                minTimes = 0;
                result = new IOException();
            }
        };

        MysqlChannel channel1 = new MysqlChannel(channel);

        ByteBuffer buf = channel1.fetchOnePacket();
        Assert.fail("No Exception throws.");
    }

    @Test
    public void testSend() throws IOException {
        // mock
        new Expectations() {
            {
                channel.write((ByteBuffer) any);
                minTimes = 0;
                result = new Delegate() {
                    int fakeWrite(ByteBuffer buffer) {
                        int writeLen = 0;
                        writeLen += buffer.remaining();
                        buffer.position(buffer.limit());
                        return writeLen;
                    }
                };
            }
        };

        MysqlChannel channel1 = new MysqlChannel(channel);
        ByteBuffer buf = ByteBuffer.allocate(1000);
        channel1.sendOnePacket(buf);

        buf = ByteBuffer.allocate(0xffffff0);
        channel1.sendOnePacket(buf);
    }

    @Test(expected = IOException.class)
    public void testSendException() throws IOException {
        // mock
        new Expectations() {
            {
                channel.write((ByteBuffer) any);
                minTimes = 0;
                result = new IOException();
            }
        };
        MysqlChannel channel1 = new MysqlChannel(channel);
        ByteBuffer buf = ByteBuffer.allocate(1000);
        channel1.sendOnePacket(buf);

        buf = ByteBuffer.allocate(0xffffff0);
        channel1.sendAndFlush(buf);
    }

    @Test(expected = IOException.class)
    public void testSendFail() throws IOException {
        // mock
        new Expectations() {
            {
                channel.write((ByteBuffer) any);
                minTimes = 0;
                result = new Delegate() {
                    int fakeWrite(ByteBuffer buffer) {
                        int writeLen = 0;
                        writeLen += buffer.remaining();
                        buffer.position(buffer.limit());
                        return writeLen - 1;
                    }
                };
            }
        };
        MysqlChannel channel1 = new MysqlChannel(channel);
        ByteBuffer buf = ByteBuffer.allocate(1000);
        channel1.sendAndFlush(buf);
        Assert.fail("No Exception throws.");
    }

}
