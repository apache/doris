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

import java.io.DataOutput;
import java.io.IOException;

/**
 * Any class that requires persistence should implement the Writable interface.
 * This interface requires only a uniform writable method "write()",
 * but does not require a uniform read method.
 * The usage of writable interface implementation class is as follows:
 * 
 * Class A implements Writable {
 *      @Override
 *      public void write(DataOutput out) throws IOException {
 *          in.write(x);
 *          in.write(y);
 *          ...
 *      }
 *      
 *      private void readFields(DataInput in) throws IOException {
 *          x = in.read();
 *          y = in.read();
 *          ...
 *      }
 *      
 *      public static A read(DataInput in) throws IOException {
 *          A a = new A();
 *          a.readFields();
 *          return a;
 *      }
 * }
 * 
 * A a = new A();
 * a.write(out);
 * ...
 * A other = A.read(in);
 * 
 * The "readFields()" can be implemented as whatever you like, or even without it
 * by just implementing the static read method.
 */
public interface Writable {
    /** 
     * Serialize the fields of this object to <code>out</code>.
     * 
     * @param out <code>DataOutput</code> to serialize this object into.
     * @throws IOException
     */
    void write(DataOutput out) throws IOException;
}
