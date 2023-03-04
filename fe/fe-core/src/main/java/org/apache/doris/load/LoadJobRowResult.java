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

package org.apache.doris.load;

public class LoadJobRowResult {
    private long records;
    private long deleted;
    private int skipped;
    private int warnings;

    public LoadJobRowResult() {
        this.records = 0;
        this.deleted = 0;
        this.skipped = 0;
        this.warnings = 0;
    }

    public long getRecords() {
        return records;
    }

    public void setRecords(long records) {
        this.records = records;
    }

    public void incRecords(long records) {
        this.records += records;
    }

    public int getSkipped() {
        return skipped;
    }

    public void setSkipped(int skipped) {
        this.skipped = skipped;
    }

    public void incSkipped(int skipped) {
        this.skipped += skipped;
    }

    public int getWarnings() {
        return warnings;
    }

    @Override
    public String toString() {
        return "Records: " + records + "  Deleted: " + deleted + "  Skipped: " + skipped + "  Warnings: " + warnings;
    }
}
