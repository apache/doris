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

import org.apache.doris.qe.protocol.RecordingMysqlChannel.RecordedPacket;

import org.junit.jupiter.api.Assertions;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

/**
 * Renders recorded protocol traffic as text and compares it with a checked-in golden file.
 *
 * <p>Run with -Ddoris.protocol.golden.regenerate=true to rewrite the golden files from the current
 * behavior. Every rewritten byte is a behavior change that has to be justified in review, which is
 * the whole point of the file: the session and result layers are being refactored protocol by
 * protocol, and these bytes are the contract that must not move while they are.
 */
public final class ProtocolGolden {
    public static final String REGENERATE_PROPERTY = "doris.protocol.golden.regenerate";

    private static final String GOLDEN_DIR = "src/test/resources/protocol-golden";
    private static final int BYTES_PER_LINE = 16;
    private static final int MESSAGE_PREFIX_LENGTH = 72;

    private ProtocolGolden() {
    }

    /**
     * Compare the rendered traffic with the golden file of that name, or rewrite it when
     * regeneration is requested.
     */
    public static void verify(String goldenName, String actual) throws IOException {
        Path golden = resolve(goldenName);
        if (Boolean.getBoolean(REGENERATE_PROPERTY)) {
            Files.createDirectories(golden.getParent());
            Files.write(golden, actual.getBytes(StandardCharsets.UTF_8));
            System.out.println("regenerated golden file " + golden.toAbsolutePath());
            return;
        }
        Assertions.assertTrue(Files.exists(golden),
                "golden file " + golden.toAbsolutePath() + " is missing, regenerate it with -D"
                        + REGENERATE_PROPERTY + "=true");
        String expected = new String(Files.readAllBytes(golden), StandardCharsets.UTF_8);
        if (expected.equals(actual)) {
            return;
        }
        Path dump = Paths.get("target", "protocol-golden", goldenName);
        Files.createDirectories(dump.getParent());
        Files.write(dump, actual.getBytes(StandardCharsets.UTF_8));
        Assertions.fail("protocol golden " + goldenName + " changed. " + firstDifference(expected, actual)
                + "\nEvery difference is a change in what a client receives, not a test artifact."
                + " The current traffic was written to " + dump.toAbsolutePath()
                + "; copy it over " + GOLDEN_DIR + "/" + goldenName
                + " (or rerun with -D" + REGENERATE_PROPERTY + "=true) once every changed byte is"
                + " explained and intended.");
    }

    private static Path resolve(String goldenName) {
        Path fromModule = Paths.get(GOLDEN_DIR, goldenName);
        if (Files.isDirectory(fromModule.getParent())) {
            return fromModule;
        }
        // Surefire runs with the module directory as the working directory, but an IDE may use the
        // repository root instead.
        return Paths.get("fe/fe-core", GOLDEN_DIR, goldenName);
    }

    private static String firstDifference(String expected, String actual) {
        String[] expectedLines = expected.split("\n", -1);
        String[] actualLines = actual.split("\n", -1);
        for (int i = 0; i < Math.max(expectedLines.length, actualLines.length); i++) {
            String expectedLine = i < expectedLines.length ? expectedLines[i] : "<end of file>";
            String actualLine = i < actualLines.length ? actualLines[i] : "<end of file>";
            if (!expectedLine.equals(actualLine)) {
                return "First difference at line " + (i + 1) + ":\n  golden: " + expectedLine
                        + "\n  actual: " + actualLine;
            }
        }
        return "Files differ but no differing line was found.";
    }

    /**
     * How much of a response to keep in the golden.
     *
     * <p>BYTES keeps every byte and is the default: that is the contract a client parses. SUMMARY
     * keeps only the shape -- the sequence of packet kinds, plus the error code and the start of the
     * message when the command failed -- and exists for the two responses whose payload legitimately
     * moves between builds: a parser error carries the full keyword list of the grammar, and an
     * EXPLAIN carries the current plan. Recording those byte for byte would break the golden on
     * changes that have nothing to do with the protocol.
     */
    public enum Fidelity {
        BYTES,
        SUMMARY
    }

    /** Render one command's response packets the way they would hit the wire. */
    public static String renderPackets(List<RecordedPacket> packets, Fidelity fidelity) {
        StringBuilder builder = new StringBuilder();
        if (packets.isEmpty()) {
            builder.append("  <no response packet>\n");
            return builder.toString();
        }
        if (fidelity == Fidelity.SUMMARY) {
            return renderSummary(packets);
        }
        for (RecordedPacket packet : packets) {
            byte[] payload = packet.getPayload();
            builder.append(String.format("  packet seq=%d len=%d kind=%s%s\n",
                    packet.getSequenceId(), payload.length, classify(payload),
                    packet.isFlushed() ? " flushed" : ""));
            builder.append(hexDump(payload));
        }
        return builder.toString();
    }

    private static String renderSummary(List<RecordedPacket> packets) {
        StringBuilder shape = new StringBuilder();
        String previousKind = null;
        boolean repeatedNoted = false;
        for (RecordedPacket packet : packets) {
            String kind = classify(packet.getPayload()) + (packet.isFlushed() ? " flushed" : "");
            if (kind.equals(previousKind)) {
                if (!repeatedNoted) {
                    // No count: an EXPLAIN emits one row packet per plan line, and that number is
                    // not a protocol property.
                    shape.append(" (repeated)");
                    repeatedNoted = true;
                }
                continue;
            }
            if (previousKind != null) {
                shape.append(", ");
            }
            shape.append(kind);
            previousKind = kind;
            repeatedNoted = false;
        }
        StringBuilder builder = new StringBuilder();
        builder.append("  kinds: ").append(shape).append('\n');
        for (RecordedPacket packet : packets) {
            if ("ERR".equals(classify(packet.getPayload()))) {
                builder.append("  ").append(describeError(packet.getPayload())).append('\n');
            }
        }
        return builder.toString();
    }

    /**
     * Decode the fixed head of an ERR packet: error code, SQL state, and the start of the message.
     * Only a prefix of the message is kept, for the reason SUMMARY exists at all.
     */
    private static String describeError(byte[] payload) {
        if (payload.length < 9) {
            return "err: malformed, " + payload.length + " bytes";
        }
        int errorCode = (payload[1] & 0xFF) | ((payload[2] & 0xFF) << 8);
        String sqlState = new String(payload, 4, 5, StandardCharsets.UTF_8);
        String message = new String(payload, 9, payload.length - 9, StandardCharsets.UTF_8);
        message = message.replace("\n", "\\n").replace("\r", "\\r");
        if (message.length() > MESSAGE_PREFIX_LENGTH) {
            message = message.substring(0, MESSAGE_PREFIX_LENGTH) + "...";
        }
        return "err: errorCode=" + errorCode + " sqlState=" + sqlState + " message=\"" + message + "\"";
    }

    /**
     * Name the packet by the same first-byte rule a client uses. A result-set header carries the
     * column count, so anything that is not an OK, an ERR or an EOF is left as payload: the golden
     * keeps the bytes either way, the name is only there to make a diff readable.
     */
    public static String classify(byte[] payload) {
        if (payload.length == 0) {
            return "EMPTY";
        }
        int first = payload[0] & 0xFF;
        if (first == 0x00 && payload.length >= 7) {
            return "OK";
        }
        if (first == 0xFF) {
            return "ERR";
        }
        if (first == 0xFE && payload.length < 9) {
            return "EOF";
        }
        return "PAYLOAD";
    }

    /** Classic hexdump: offset, bytes, printable gutter. */
    public static String hexDump(byte[] bytes) {
        StringBuilder builder = new StringBuilder();
        for (int offset = 0; offset < bytes.length; offset += BYTES_PER_LINE) {
            int end = Math.min(offset + BYTES_PER_LINE, bytes.length);
            StringBuilder hex = new StringBuilder();
            StringBuilder text = new StringBuilder();
            for (int i = offset; i < end; i++) {
                hex.append(String.format("%02x ", bytes[i]));
                int value = bytes[i] & 0xFF;
                text.append(value >= 0x20 && value < 0x7F ? (char) value : '.');
            }
            builder.append(String.format("    %04x  %-48s |%s|\n", offset, hex.toString().trim(), text));
        }
        return builder.toString();
    }
}
