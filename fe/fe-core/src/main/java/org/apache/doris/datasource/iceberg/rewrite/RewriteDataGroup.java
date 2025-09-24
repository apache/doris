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

package org.apache.doris.datasource.iceberg.rewrite;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Group of file scan tasks to be rewritten together
 */
public class RewriteDataGroup {
    private final List<FileScanTask> tasks;
    private long totalSize;

    public RewriteDataGroup() {
        this.tasks = new ArrayList<>();
        this.totalSize = 0;
    }

    /**
     * Check if a task can be added to this group
     */
    public boolean canAddTask(FileScanTask task, RewriteDataFileManager.Parameters parameters) {
        long taskSize = task.file().fileSizeInBytes();
        return (totalSize + taskSize) <= parameters.getMaxFileGroupSizeBytes();
    }

    /**
     * Add a task to this group
     */
    public void addTask(FileScanTask task) {
        tasks.add(task);
        totalSize += task.file().fileSizeInBytes();
    }

    /**
     * Get all tasks in this group
     */
    public List<FileScanTask> getTasks() {
        return tasks;
    }

    /**
     * Get number of tasks in this group
     */
    public int getTaskCount() {
        return tasks.size();
    }

    /**
     * Get total size of all files in this group
     */
    public long getTotalSize() {
        return totalSize;
    }

    /**
     * Get all data files in this group
     */
    public Set<DataFile> getDataFiles() {
        Set<DataFile> dataFiles = new HashSet<>();
        for (FileScanTask task : tasks) {
            dataFiles.add(task.file());
        }
        return dataFiles;
    }

    /**
     * Check if this group is empty
     */
    public boolean isEmpty() {
        return tasks.isEmpty();
    }

    @Override
    public String toString() {
        return "RewriteDataGroup{"
                + "taskCount=" + tasks.size()
                + ", totalSize=" + totalSize
                + '}';
    }
}
