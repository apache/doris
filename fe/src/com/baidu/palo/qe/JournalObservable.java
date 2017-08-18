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

import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.baidu.palo.catalog.Catalog;
import com.baidu.palo.common.DdlException;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;

public class JournalObservable {
    private static final Logger LOG = LogManager.getLogger(JournalObservable.class);
    private Multiset<JournalObserver> obs;

    public JournalObservable() {
        obs = TreeMultiset.create();
    }
    
    private synchronized void addObserver(JournalObserver o) {
        if (o == null) {
            throw new NullPointerException();
        }

        obs.add(o);
        LOG.debug("JournalObservable addObserver=[" + o + "] the size is " + obs.size());
    }

    private synchronized void deleteObserver(JournalObserver o) {
        obs.remove(o);
        LOG.debug("JournalObservable deleteObserver=[" + o + "] the size is " + obs.size());
    } 
    
    public void waitOn(Long journalVersion, int timeoutMs) throws DdlException {
        long replayedJournalId = Catalog.getInstance().getReplayedJournalId();
        if (replayedJournalId >= journalVersion || timeoutMs <= 0) {
            LOG.debug("follower no need to sync getReplayedJournalId={}, journalVersion={}, timeoutMs={}",
                      replayedJournalId, journalVersion, timeoutMs);
            return;
        } else {
            LOG.info("waiting observer to replay journal from {} to {}", replayedJournalId, journalVersion);
            JournalObserver observer = new JournalObserver(journalVersion);
            addObserver(observer);
            try {
                boolean ok = observer.getLatch().await(timeoutMs, TimeUnit.MILLISECONDS);
                if (!ok) {
                    throw new DdlException("Execute timeout, the command may be succeed, you'd better retry");
                }
            } catch (InterruptedException e) {
                throw new DdlException("Interrupted exception happens, "
                        + "the command may be succeed, you'd better retry");
            } finally {
                deleteObserver(observer);
            }
        }
    }
    
    // return min pos which is bigger than value
    public static int upperBound(Object[] array, int size, Long value) {
        int left = 0;
        int right = size - 1;
        while (left < right) {
            int middle = left + ((right - left) >> 1);
            if (value >= ((JournalObserver) array[middle]).getTargetJournalVersion()) {
                left = middle + 1;
            } else {
                right = middle - 1;
            }
        }
        if (right == -1) {
            return 0;
        }
        Long rightValue = ((JournalObserver) array[right]).getTargetJournalVersion();
        return value >= rightValue ? right + 1  : right;  
    }
    
    public void notifyObservers(Long journalId) {
        Object[] arrLocal;
        int size;
        synchronized (this) {
            size = obs.size();
            arrLocal = obs.toArray();
        }
        
        int pos = upperBound(arrLocal, size, journalId);
        LOG.debug("notify observers: journal: {}, pos: {}, size: {}, obs: {}", journalId, pos, size, obs);

        for (int i = 0; i < pos; i ++) {
            JournalObserver observer = ((JournalObserver) arrLocal[i]);
            observer.update();
        }
    }
}
