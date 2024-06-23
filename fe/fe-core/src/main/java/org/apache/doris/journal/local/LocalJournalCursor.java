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

package org.apache.doris.journal.local;

import org.apache.doris.common.Pair;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.EditLogFileInputStream;
import org.apache.doris.persist.OperationType;
import org.apache.doris.persist.Storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * For unit test only.
 * Use local file save edit logs.
 */
public final class LocalJournalCursor implements JournalCursor {
    private static final Logger LOG = LogManager.getLogger(LocalJournalCursor.class);
    private String imageDir;
    private long toKey;
    private long currentKey;
    private List<Long> editFileSequenceNumbers;
    private int nextFilePositionIndex;
    private Storage storage;
    private DataInputStream currentStream;

    public static LocalJournalCursor getJournalCursor(String imageDir, long fromKey, long toKey) {
        if (toKey < fromKey && toKey != -1 || fromKey < 0) {
            LOG.warn("Invalid fromKey:{} toKey:{}", fromKey, toKey);
            return null;
        }
        long newToKey = toKey;
        if (newToKey == -1) {
            newToKey = Long.MAX_VALUE;
        }
        LocalJournalCursor cursor;
        try {
            cursor = new LocalJournalCursor(imageDir, fromKey, newToKey);
        } catch (IOException e) {
            LOG.error(e);
            return null;
        }
        return cursor;
    }

    /*
     * edits file name is the minimum journal id in this file.
     * For example:
     * an edit file contains journal from 100 to 200. its file name is edits.100
     */
    private LocalJournalCursor(String imageDir, long fromKey, long toKey) throws IOException {
        this.imageDir = imageDir;
        this.currentKey = fromKey;
        this.toKey = toKey;
        this.storage = new Storage(imageDir);
        this.editFileSequenceNumbers = storage.getEditsFileSequenceNumbers();
        this.nextFilePositionIndex = 0;
        long scannedKey = 0;

        // find the file which may contain the journal with journalId=fromKey
        String fileName = null;
        for (long minJournalKey : editFileSequenceNumbers) {
            if (fromKey >= minJournalKey) {
                fileName = Long.toString(minJournalKey);
                nextFilePositionIndex++;
                scannedKey = minJournalKey;
                continue;
            } else {
                break;
            }
        }

        if (fileName == null) {
            LOG.warn("Can not find the key:{}", fromKey);
            throw new IOException();
        }

        this.currentStream = new DataInputStream(new BufferedInputStream(
                new EditLogFileInputStream(new File(imageDir, "edits." + fileName))));

        while (scannedKey < fromKey) {
            getJournalEntity(currentStream);
            scannedKey++;
        }
    }

    @Override
    public Pair<Long, JournalEntity> next() {
        if (currentKey > toKey) {
            return null;
        }

        Long key = currentKey;
        JournalEntity entity = null;
        try {
            while (true) {
                try {
                    entity = getJournalEntity(currentStream);
                    if (entity.getOpCode() == OperationType.OP_LOCAL_EOF) {
                        if (nextFilePositionIndex < editFileSequenceNumbers.size()) {
                            currentStream.close();
                            currentStream = new DataInputStream(new BufferedInputStream(new EditLogFileInputStream(
                                    new File(imageDir,
                                            "edits." + editFileSequenceNumbers.get(nextFilePositionIndex)))));
                            nextFilePositionIndex++;
                            continue;
                        } else {
                            return null;
                        }
                    }
                } catch (EOFException e) {
                    if (nextFilePositionIndex < editFileSequenceNumbers.size()) {
                        currentStream.close();
                        currentStream = new DataInputStream(new BufferedInputStream(new EditLogFileInputStream(
                                new File(imageDir, "edits." + editFileSequenceNumbers.get(nextFilePositionIndex)))));
                        nextFilePositionIndex++;
                        continue;
                    } else {
                        return null;
                    }
                }
                break;
            }

            currentKey++;
            return Pair.of(key, entity);
        } catch (IOException e) {
            LOG.error("something wrong. {}", e);
            try {
                currentStream.close();
            } catch (IOException e1) {
                LOG.error(e1);
            }
            LOG.error(e);
        }
        return Pair.of(key, entity);
    }

    @Deprecated
    private JournalEntity getJournalEntity(DataInputStream in) throws IOException {
        JournalEntity ret = new JournalEntity();
        ret.readFields(in);
        return ret;
    }

    @Override
    public void close() {

    }
}
