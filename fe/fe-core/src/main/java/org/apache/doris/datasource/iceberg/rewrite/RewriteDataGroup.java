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
import java.util.List;

/**
 * Group of file scan tasks to be rewritten together
 */
public class RewriteDataGroup {
    private final List<FileScanTask> tasks;
    private long totalSize;
    private int deleteFileCount;

    public RewriteDataGroup() {
        this.tasks = new ArrayList<>();
        this.totalSize = 0;
        this.deleteFileCount = 0;
    }

    public RewriteDataGroup(List<FileScanTask> tasks) {
        this.tasks = tasks;
        this.totalSize = tasks.stream().mapToLong(task -> task.file().fileSizeInBytes()).sum();
        this.deleteFileCount = tasks.stream().mapToInt(task -> task.deletes() != null ? task.deletes().size() : 0)
                .sum();
    }

    /**
     * Add a task to this group
     */
    public void addTask(FileScanTask task) {
        tasks.add(task);
        totalSize += task.file().fileSizeInBytes();
        if (task.deletes() != null) {
            deleteFileCount += task.deletes().size();
        }
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
     * Get total number of delete files in this group
     */
    public int getDeleteFileCount() {
        return deleteFileCount;
    }

    /**
     * Get all data files in this group
     */
    public List<DataFile> getDataFiles() {
        List<DataFile> dataFiles = new ArrayList<>();
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
                + ", deleteFileCount=" + deleteFileCount
                + '}';
    }
}
