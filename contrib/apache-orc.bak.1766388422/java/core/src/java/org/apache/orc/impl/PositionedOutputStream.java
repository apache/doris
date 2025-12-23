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
package org.apache.orc.impl;

import java.io.IOException;
import java.io.OutputStream;
import java.util.function.Consumer;

public abstract class PositionedOutputStream extends OutputStream {

  /**
   * Record the current position to the recorder.
   * @param recorder the object that receives the position
   * @throws IOException
   */
  public abstract void getPosition(PositionRecorder recorder
                                   ) throws IOException;

  /**
   * Get the memory size currently allocated as buffer associated with this
   * stream.
   * @return the number of bytes used by buffers.
   */
  public abstract long getBufferSize();

  /**
   * Change the current Initialization Vector (IV) for the encryption.
   * Has no effect if the stream is not encrypted.
   * @param modifier a function to modify the IV in place
   */
  public abstract void changeIv(Consumer<byte[]> modifier);
}
