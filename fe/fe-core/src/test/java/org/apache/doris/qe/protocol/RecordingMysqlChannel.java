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

package org.apache.doris.qe.protocol;

import org.apache.doris.mysql.DummyMysqlChannel;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;

/**
 * A MysqlChannel that plays canned request packets to a ConnectProcessor and keeps every response
 * packet the server wrote, so a test can compare the wire bytes against a golden file.
 *
 * <p>The real channel frames a packet in {@link org.apache.doris.mysql.MysqlChannel#sendOnePacket}:
 * it writes a 4-byte header (3-byte payload length + 1-byte sequence id) into the send buffer, then
 * the payload, then advances the sequence id. There is no send buffer here, so the header is not
 * materialized; the sequence id each packet would have carried is recorded next to the payload
 * instead, and the golden renders the header from the two.
 *
 * <p>What the send buffer does is modeled, though: a flush pushes everything written so far, and
 * {@link #reset()} drops what was written after the last flush, the way
 * {@link org.apache.doris.mysql.MysqlChannel#reset()} clears the buffer. So the golden shows what
 * reaches the client, not everything the server wrote, and a packet that was written and then
 * dropped leaves a gap in the sequence ids, as it does on the wire.
 */
public class RecordingMysqlChannel extends DummyMysqlChannel {

    /** One response packet: the payload the server wrote and the sequence id it was framed with. */
    public static class RecordedPacket {
        private final int sequenceId;
        private final byte[] payload;
        private boolean flushed;

        RecordedPacket(int sequenceId, byte[] payload, boolean flushed) {
            this.sequenceId = sequenceId;
            this.payload = payload;
            this.flushed = flushed;
        }

        public int getSequenceId() {
            return sequenceId;
        }

        public byte[] getPayload() {
            return payload;
        }

        /**
         * True when the response was pushed to the client at this packet, either by sendAndFlush or
         * by a later flush(). A response that never gets flushed leaves the client waiting, so where
         * the flush lands is part of the contract.
         */
        public boolean isFlushed() {
            return flushed;
        }

        void markFlushed() {
            this.flushed = true;
        }
    }

    private final Deque<ByteBuffer> inbound = new ArrayDeque<>();
    private final List<RecordedPacket> outbound = new ArrayList<>();

    /**
     * Queue one request packet. The buffer holds the payload only (command byte first), the way
     * fetchOnePacket() hands it to the processor after stripping the header.
     */
    public void queueRequest(byte[] payload) {
        inbound.addLast(ByteBuffer.wrap(payload));
    }

    public List<RecordedPacket> getOutbound() {
        return outbound;
    }

    public void clearOutbound() {
        outbound.clear();
    }

    public boolean hasPendingRequest() {
        return !inbound.isEmpty();
    }

    @Override
    public ByteBuffer fetchOnePacket() {
        ByteBuffer packet = inbound.pollFirst();
        if (packet == null) {
            // An empty buffer is how the real channel reports "peer sent nothing more"; processOnce()
            // turns that into ctx.setKilled().
            return ByteBuffer.allocate(0);
        }
        // The real channel advances the sequence id once per packet it reads, so the first response
        // packet is framed with the request's id plus one.
        accSequenceId();
        return packet;
    }

    @Override
    public void sendOnePacket(ByteBuffer packet) {
        record(packet, false);
    }

    @Override
    public void sendAndFlush(ByteBuffer packet) {
        record(packet, true);
        isSend = true;
    }

    @Override
    public void flush() {
        // Nothing to push when the last packet already went out with a flush.
        if (!outbound.isEmpty() && !outbound.get(outbound.size() - 1).isFlushed()) {
            outbound.get(outbound.size() - 1).markFlushed();
            isSend = true;
        }
    }

    @Override
    public void reset() {
        isSend = false;
        // A flush pushes everything written so far; what was written after the last one is still
        // in the buffer, and that is what a reset throws away.
        int end = outbound.size();
        while (end > 0 && !outbound.get(end - 1).isFlushed()) {
            end--;
        }
        outbound.subList(end, outbound.size()).clear();
    }

    private void record(ByteBuffer packet, boolean flushed) {
        byte[] payload = new byte[packet.remaining()];
        packet.duplicate().get(payload);
        outbound.add(new RecordedPacket(sequenceId, payload, flushed));
        accSequenceId();
    }

    private void accSequenceId() {
        sequenceId++;
        if (sequenceId > 255) {
            sequenceId = 0;
        }
    }
}
