/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.orc.mapred;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.Date;

/**
 * A Timestamp implementation that implements Writable.
 */
public class OrcTimestamp extends Timestamp implements WritableComparable<Date> {

  public OrcTimestamp() {
    super(0);
  }

  public OrcTimestamp(long time) {
    super(time);
  }

  public OrcTimestamp(String timeStr) {
    super(0);
    Timestamp t = Timestamp.valueOf(timeStr);
    setTime(t.getTime());
    setNanos(t.getNanos());
  }

  @Override
  public void write(DataOutput output) throws IOException {
    output.writeLong(getTime());
    output.writeInt(getNanos());
  }

  @Override
  public void readFields(DataInput input) throws IOException {
    setTime(input.readLong());
    setNanos(input.readInt());
  }

  public void set(String timeStr) {
    Timestamp t = Timestamp.valueOf(timeStr);
    setTime(t.getTime());
    setNanos(t.getNanos());
  }
}
