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

package org.apache.doris.load.sync.position;

import com.alibaba.otter.canal.protocol.CanalEntry;

import com.google.common.base.Strings;

public class EntryPosition {
    private String journalName;
    private Long position;
    private String gtid;
    private Long executeTime;

    public static final EntryPosition MIN_POS = new EntryPosition("", -1L, null);

    public EntryPosition() {
        this(null, (Long)null, (Long)null);
    }

    public EntryPosition(String journalName, Long position, Long timestamp) {
        this.gtid = null;
        this.journalName = journalName;
        this.position = position;
        this.executeTime = timestamp;
    }

    public EntryPosition(String journalName, Long position) {
        this(journalName, position, (Long)null);
    }

    public String getJournalName() {
        return this.journalName;
    }

    public void setJournalName(String journalName) {
        this.journalName = journalName;
    }

    public Long getPosition() {
        return this.position;
    }

    public void setPosition(Long position) {
        this.position = position;
    }

    public Long getExecuteTime() {
        return this.executeTime;
    }

    public void setExecuteTime(Long timeStamp) {
        this.executeTime = timeStamp;
    }

    public String getGtid() {
        return this.gtid;
    }

    public void setGtid(String gtid) {
        this.gtid = gtid;
    }

    public int hashCode() {
        int result = 1;
        result = 31 * result + (this.journalName == null ? 0 : this.journalName.hashCode());
        result = 31 * result + (this.position == null ? 0 : this.position.hashCode());
        result = 31 * result + (this.executeTime == null ? 0 : this.executeTime.hashCode());
        return result;
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }  else if (!(obj instanceof EntryPosition)) {
            return false;
        } else {
            EntryPosition other = (EntryPosition) obj;
            if (this.journalName == null) {
                if (other.journalName != null) {
                    return false;
                }
            } else if (!this.journalName.equals(other.journalName)) {
                return false;
            }

            if (this.position == null) {
                if (other.position != null) {
                    return false;
                }
            } else if (!this.position.equals(other.position)) {
                return false;
            }

            if (this.executeTime == null) {
                if (other.executeTime != null) {
                    return false;
                }
            } else if (!this.executeTime.equals(other.executeTime)) {
                return false;
            }

            return true;
        }
    }
    @Override
    public String toString() {
        return "[" + journalName + ":" + position + "]";
    }

    public int compareTo(EntryPosition o) {
        final int val = journalName.compareTo(o.journalName);

        if (val == 0) {
            return (int) (position - o.position);
        }
        return val;
    }

    public static EntryPosition min(EntryPosition position1, EntryPosition position2) {
        if (position1.getJournalName().compareTo(position2.getJournalName()) > 0) {
            return position2;
        } else if (position1.getJournalName().compareTo(position2.getJournalName()) < 0) {
            return position1;
        } else {
            if (position1.getPosition() > position2.getPosition()) {
                return position2;
            } else {
                return position1;
            }
        }
    }

    // --------helper methods---------

    public static EntryPosition createPosition(CanalEntry.Entry entry) {
        final CanalEntry.Header header = entry.getHeader();
        EntryPosition position = new EntryPosition();
        position.setJournalName(header.getLogfileName());
        position.setPosition(header.getLogfileOffset());
        position.setExecuteTime(header.getExecuteTime());
        position.setGtid(header.getGtid());
        return position;
    }

    public static boolean checkPosition(CanalEntry.Entry entry, EntryPosition entryPosition) {
        return checkPosition(entry.getHeader(), entryPosition);
    }

    public static boolean checkPosition(CanalEntry.Header header, EntryPosition entryPosition) {
        boolean result = entryPosition.getExecuteTime().equals(header.getExecuteTime());
        boolean isEmptyPosition = (Strings.isNullOrEmpty(entryPosition.getJournalName()) && entryPosition.getPosition() == null);
        if (!isEmptyPosition) {
            result &= entryPosition.getPosition().equals(header.getLogfileOffset());
            if (result) {
                result &= header.getLogfileName().equals(entryPosition.getJournalName());
            }
        }
        return result;
    }
}