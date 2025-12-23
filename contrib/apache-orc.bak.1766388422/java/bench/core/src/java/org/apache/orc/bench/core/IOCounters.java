/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.bench.core;

import org.openjdk.jmh.annotations.AuxCounters;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;

/**
 * A class to track the number of rows, bytes, and read/write operations that have
 * been read/write.
 * The read/write is named as "IO" instead of "RW" to avoid ambiguity as the latter could indicate the
 * counts including both read and write, instead of either read or write.
 */
@AuxCounters(AuxCounters.Type.EVENTS)
@State(Scope.Thread)
public class IOCounters {
  long bytesIO;
  long io;
  RecordCounters recordCounters;

  @Setup(Level.Iteration)
  public void setup(RecordCounters records) {
    bytesIO = 0;
    io = 0;
    recordCounters = records;
  }

  @TearDown(Level.Iteration)
  public void print() {
    if (recordCounters != null) {
      recordCounters.print();
    }
    System.out.println("io: " + io);
    System.out.println("Bytes: " + bytesIO);
  }

  public double bytesPerRecord() {
    return recordCounters == null || recordCounters.records == 0 ?
        0 : ((double) bytesIO) / recordCounters.records;
  }

  public long records() {
    return recordCounters == null || recordCounters.invocations == 0 ?
        0 : recordCounters.records / recordCounters.invocations;
  }

  /**
   * Capture the number of I/O on average in each invocation.
   */
  public long ops() {
    return recordCounters == null || recordCounters.invocations == 0 ?
        0 : io / recordCounters.invocations;
  }

  public void addRecords(long value) {
    if (recordCounters != null) {
      recordCounters.records += value;
    }
  }

  public void addInvocation() {
    if (recordCounters != null) {
      recordCounters.invocations += 1;
    }
  }

  public void addBytes(long newIOs, long newBytes) {
    bytesIO += newBytes;
    io += newIOs;
  }
}
