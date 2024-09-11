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

package org.apache.doris.fs.remote;

import org.apache.hadoop.fs.FileSystem;

import java.lang.ref.PhantomReference;
import java.lang.ref.ReferenceQueue;

public class RemoteFileSystemPhantomReference extends PhantomReference<RemoteFileSystem> {

    private FileSystem fs;

    /**
     * Creates a new phantom reference that refers to the given object and
     * is registered with the given queue.
     *
     * <p> It is possible to create a phantom reference with a {@code null}
     * queue.  Such a reference will never be enqueued.
     *
     * @param referent the object the new phantom reference will refer to
     * @param q        the queue with which the reference is to be registered,
     *                 or {@code null} if registration is not required
     */
    public RemoteFileSystemPhantomReference(RemoteFileSystem referent, ReferenceQueue<? super RemoteFileSystem> q) {
        super(referent, q);
        this.fs = referent.dfsFileSystem;
    }
}
