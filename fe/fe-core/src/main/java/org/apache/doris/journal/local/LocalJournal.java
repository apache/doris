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

import org.apache.doris.common.io.Writable;
import org.apache.doris.journal.Journal;
import org.apache.doris.journal.JournalBatch;
import org.apache.doris.journal.JournalCursor;
import org.apache.doris.journal.JournalEntity;
import org.apache.doris.persist.EditLogFileOutputStream;
import org.apache.doris.persist.EditLogOutputStream;
import org.apache.doris.persist.Storage;

import com.google.common.base.Preconditions;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class LocalJournal implements Journal {
    private static final Logger LOG = LogManager.getLogger(LocalJournal.class);

    private EditLogOutputStream outputStream = null;
    private AtomicLong journalId = new AtomicLong(1);
    private String imageDir;
    private File currentEditFile;

    public LocalJournal(String imageDir) {
        this.imageDir = imageDir;
    }

    @Override
    public void open() {
        if (outputStream == null) {
            try {
                Storage storage = new Storage(imageDir);

                this.journalId.set(getCurrentJournalId(storage.getEditsFileSequenceNumbers()));

                long id = journalId.get();
                if (storage.getEditsSeq() == 0) {
                    // there is no edits file, create first one
                    Preconditions.checkState(id == 1, id);
                    currentEditFile = new File(imageDir, "edits.1");
                    currentEditFile.createNewFile();
                    outputStream = new EditLogFileOutputStream(currentEditFile);
                } else if (id == storage.getEditsSeq()) {
                    // there exist edits files, point to the latest one and set position to the end of file.
                    this.currentEditFile = storage.getEditsFile(id);
                    this.outputStream = new EditLogFileOutputStream(currentEditFile);
                } else {
                    // create next edits file
                    currentEditFile = new File(imageDir, "edits." + (id + 1));
                    currentEditFile.createNewFile();
                    outputStream = new EditLogFileOutputStream(currentEditFile);
                }
            } catch (IOException e) {
                LOG.error(e);
            }
        }
    }

    @Override
    public synchronized void rollJournal() {
        Storage storage;
        try {
            storage = new Storage(imageDir);
            if (journalId.get() == storage.getEditsSeq()) {
                LOG.warn("Does not need to roll! journalId: {}, editsSeq: {}", journalId.get(), storage.getEditsSeq());
                return;
            }
            if (outputStream != null) {
                outputStream.flush();
                outputStream.close();
            }
            currentEditFile = new File(imageDir, "edits." + journalId.get());
            currentEditFile.createNewFile();
            outputStream = new EditLogFileOutputStream(currentEditFile);
        } catch (IOException e) {
            LOG.error(e);
        }
    }

    @Override
    public long getMaxJournalId() {
        return 0;
    }

    @Override
    public long getMinJournalId() {
        return 0;
    }

    @Override
    public long getJournalNum() {
        return 0;
    }

    @Override
    public void close() {
        if (outputStream == null) {
            return;
        }

        try {
            outputStream.setReadyToFlush();
            outputStream.flush();
            outputStream.close();
        } catch (IOException e) {
            LOG.error(e);
        }
    }

    @Override
    public JournalEntity read(long journalId) {
        return null;
    }

    @Override
    public JournalCursor read(long fromKey, long toKey) {
        JournalCursor cursor = LocalJournalCursor.getJournalCursor(imageDir, fromKey, toKey);
        return cursor;
    }

    @Override
    public synchronized long write(JournalBatch batch) throws IOException {
        List<JournalBatch.Entity> entities = batch.getJournalEntities();
        for (JournalBatch.Entity entity : entities) {
            outputStream.write(entity.getOpCode(), entity.getBinaryData());
        }
        outputStream.setReadyToFlush();
        outputStream.flush();
        return journalId.getAndAdd(entities.size());
    }

    @Override
    public synchronized long write(short op, Writable writable) throws IOException {
        outputStream.write(op, writable);
        outputStream.setReadyToFlush();
        outputStream.flush();
        return journalId.incrementAndGet();
    }

    @Override
    public void deleteJournals(long deleteJournalToId) {
        try {
            Storage storage = new Storage(imageDir);
            List<Long> numbers = storage.getEditsFileSequenceNumbers();
            for (long number : numbers) {
                if (number < deleteJournalToId) {
                    File file = new File(imageDir, "edits." + number);
                    if (file.exists()) {
                        file.delete();
                    }
                }
            }
        } catch (IOException e) {
            LOG.error(e);
        }
    }

    @Override
    public long getFinalizedJournalId() {
        try {
            Storage storage = new Storage(imageDir);
            List<Long> numbers = storage.getEditsFileSequenceNumbers();
            int size = numbers.size();
            if (size > 1) {
                return numbers.get(size - 1) - 1;
            }
        } catch (IOException e) {
            LOG.error(e);
        }
        return 0;
    }

    private long getCurrentJournalId(List<Long> editFileNames) {
        if (editFileNames.size() == 0) {
            return 1;
        }

        long ret = editFileNames.get(editFileNames.size() - 1);
        JournalCursor cursor = read(ret, -1);
        while (cursor.next() != null) {
            ret++;
        }

        return ret;
    }

    @Override
    public List<Long> getDatabaseNames() {
        throw new RuntimeException("Not Support");
    }

    @Override
    public boolean exceedMaxJournalSize(short op, Writable writable) throws IOException  {
        return false;
    }
}
