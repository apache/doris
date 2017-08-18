// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package com.baidu.palo.qe;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

public class JournalObserver implements Comparable<JournalObserver> {
    private static AtomicLong idGen = new AtomicLong(0L);

    private Long id;
    private Long targetJournalVersion;
    private CountDownLatch latch;
    
    public JournalObserver(Long targetJournalVersion) {
        this.id = idGen.getAndIncrement();
        this.targetJournalVersion = targetJournalVersion;
        this.latch = new CountDownLatch(1);
    }
    
    @Override
    public int compareTo(JournalObserver jo) {
        if (this.targetJournalVersion < jo.targetJournalVersion) {
            return -1;
        } else if (this.targetJournalVersion > jo.targetJournalVersion) {
            return 1;
        } else {
            if (this.id < jo.id) {
                return -1;
            } else if (this.id > jo.id) {
                return 1;
            } else {
                return 0;
            }
        }
    }
    
    @Override
    public boolean equals(Object obj) {
        if (this.hashCode() != obj.hashCode()) {
            return false;
        }
        if (!(obj instanceof JournalObserver)) {
            return false;
        }

        JournalObserver obs = ((JournalObserver) obj);
        return this.targetJournalVersion == obs.targetJournalVersion && this.id == obs.id;
    }

    @Override
    public int hashCode() {
        return this.id.hashCode();
    }

    @Override
    public String toString() {
        return "target: " + targetJournalVersion;
    }

    public void update() {
        latch.countDown();
    }
    
    public Long getTargetJournalVersion() {
        return targetJournalVersion;
    }

    public CountDownLatch getLatch() {
        return latch;
    }
}
