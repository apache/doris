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

package org.apache.doris.common.io;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;

public class ByteBufferNetworkInputStreamTest {
    @Test
    public void testMultiByteBuffer() throws IOException, InterruptedException {
        ByteBufferNetworkInputStream inputStream = new ByteBufferNetworkInputStream(2);

        inputStream.fillByteBuffer(ByteBuffer.wrap("1\t2\n".getBytes()));
        inputStream.fillByteBuffer(ByteBuffer.wrap("2\t3\n".getBytes()));
        inputStream.markFinished();

        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
        Assert.assertEquals(bufferedReader.readLine(), "1\t2");
        Assert.assertEquals(bufferedReader.readLine(), "2\t3");
        Assert.assertNull(bufferedReader.readLine());
        bufferedReader.close();
    }

    @Test
    public void testMultiThreadByteBuffer() throws IOException, InterruptedException {
        int num = 5;
        ByteBufferNetworkInputStream inputStream = new ByteBufferNetworkInputStream(2);
        Thread thread1 = new Thread(() -> {
            try {
                for (int i = 0; i < num; i++) {
                    inputStream.fillByteBuffer(ByteBuffer.wrap(String.format("%d\t%d\n", i, i + 1).getBytes()));
                    Thread.sleep(500);
                }
                inputStream.markFinished();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread1.start();


        Thread thread2 = new Thread(() -> {
            try {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                int count = 0;
                String line = bufferedReader.readLine();
                while (line != null) {
                    Assert.assertEquals(line, String.format("%d\t%d", count, count + 1));
                    count++;
                    line = bufferedReader.readLine();
                }
                Assert.assertEquals(count, num);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread2.start();
        thread2.join();
        Assert.assertFalse(thread1.isAlive());
        inputStream.close();
    }


    @Test
    public void testMultiThreadByteBuffer2() throws IOException, InterruptedException {
        int num = 5;
        ByteBufferNetworkInputStream inputStream = new ByteBufferNetworkInputStream(2);
        Thread thread1 = new Thread(() -> {
            try {
                for (int i = 0; i < num; i++) {
                    inputStream.fillByteBuffer(ByteBuffer.wrap(String.format("%d\t%d\n", i, i + 1).getBytes()));
                }
                inputStream.markFinished();
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread1.start();


        Thread thread2 = new Thread(() -> {
            try {
                BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));
                int count = 0;
                String line = bufferedReader.readLine();
                while (line != null) {
                    Assert.assertEquals(line, String.format("%d\t%d", count, count + 1));
                    count++;
                    Thread.sleep(500);
                    line = bufferedReader.readLine();
                }
                Assert.assertEquals(count, num);
            } catch (Exception e) {
                e.printStackTrace();
            }
        });
        thread2.start();
        thread2.join();
        Assert.assertFalse(thread1.isAlive());
        inputStream.close();
    }
}
