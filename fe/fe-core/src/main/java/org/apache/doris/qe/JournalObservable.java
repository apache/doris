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

package org.apache.doris.qe;

import org.apache.doris.common.DdlException;

import com.google.common.collect.Multiset;
import com.google.common.collect.TreeMultiset;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

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
        if (LOG.isDebugEnabled()) {
            LOG.debug("JournalObservable addObserver=[{}], the size is {}", o, obs.size());
        }
    }

    private synchronized void deleteObserver(JournalObserver o) {
        obs.remove(o);
        if (LOG.isDebugEnabled()) {
            LOG.debug("JournalObservable deleteObserver=[{}], the size is {}", o, obs.size());
        }
    }

    public void waitOn(Long expectedJournalVersion, int timeoutMs) throws DdlException {
        LOG.info("waiting for the observer to replay journal to {} with timeout: {} ms",
                 expectedJournalVersion, timeoutMs);

        JournalObserver observer = new JournalObserver(expectedJournalVersion);
        addObserver(observer);
        try {
            observer.waitForReplay(timeoutMs);
        } finally {
            deleteObserver(observer);
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
        if (LOG.isDebugEnabled()) {
            LOG.debug("notify observers: journal: {}, pos: {}, size: {}, obs: {}", journalId, pos, size, obs);
        }

        for (int i = 0; i < pos; i++) {
            JournalObserver observer = ((JournalObserver) arrLocal[i]);
            observer.update();
        }
    }
}
