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

/**
 * A class to track the number of rows that have been read.
 */
@AuxCounters(AuxCounters.Type.OPERATIONS)
@State(Scope.Thread)
public class RecordCounters {
  long records;
  long invocations;

  @Setup(Level.Iteration)
  public void setup() {
    records = 0;
    invocations = 0;
  }

  public long perRecord() {
    return records;
  }

  public void print() {
    System.out.println();
    System.out.println("Records: " + records);
    System.out.println("Invocations: " + invocations);
  }
}

