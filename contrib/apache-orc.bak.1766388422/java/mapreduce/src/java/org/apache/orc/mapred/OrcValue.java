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

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.orc.OrcConf;
import org.apache.orc.TypeDescription;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * This type provides a wrapper for OrcStruct so that it can be sent through
 * the MapReduce shuffle as a value.
 * <p>
 * The user should set the JobConf with orc.mapred.value.type with the type
 * string of the type.
 */
public final class OrcValue implements Writable, JobConfigurable {

  public WritableComparable value;

  public OrcValue(WritableComparable value) {
    this.value = value;
  }

  public OrcValue() {
    value = null;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    value.write(dataOutput);
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    value.readFields(dataInput);
  }

  @Override
  public void configure(JobConf conf) {
    if (value == null) {
      TypeDescription schema =
          TypeDescription.fromString(OrcConf.MAPRED_SHUFFLE_VALUE_SCHEMA
              .getString(conf));
      value = OrcStruct.createValue(schema);
    }
  }
}
