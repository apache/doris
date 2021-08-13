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

package org.apache.doris.common.io;

import com.google.common.base.Strings;
import org.slf4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintStream;
import java.net.Socket;

/**
 * An utility class for I/O related functionality.
 */
public class IOUtils {
    public static long copyBytes(InputStream in, OutputStream out,
            int buffSize, long len) throws IOException {
        byte buf[] = new byte[buffSize];
        int totalRead = 0;
        int toRead = 0;
        int bytesRead = 0;
        try {
            toRead = (int) Math.min(buffSize, len - totalRead);
            bytesRead = in.read(buf, 0, toRead);
            totalRead += bytesRead;
            while (bytesRead >= 0) {
                out.write(buf, 0, bytesRead);
                toRead = (int) Math.min(buffSize, len - totalRead);
                if (toRead == 0) {
                    break;
                }
                bytesRead = in.read(buf, 0, toRead);
                totalRead += bytesRead;
            }
            return totalRead;
        } finally {
            // TODO
        }
    }

    /**
     * Copies from one stream to another.
     * 
     * @param in
     *            InputStream to read from
     * @param out
     *            OutputStream to write to
     * @param buffSize
     *            the size of the buffer
     * @param speed
     *            limit of the copy (B/s)
     * @param close
     *            whether or not close the InputStream and OutputStream at the
     *            end. The streams are closed in the finally clause.
     */
    public static long copyBytes(InputStream in, OutputStream out,
            int buffSize, int speed, boolean close) throws IOException {

        PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        byte buf[] = new byte[buffSize];
        long bytesReadTotal = 0;
        long startTime = 0;
        long sleepTime = 0;
        long curTime = 0;
        try {
            if (speed > 0) {
                startTime = System.currentTimeMillis();
            }
            int bytesRead = in.read(buf);
            while (bytesRead >= 0) {
                out.write(buf, 0, bytesRead);
                if ((ps != null) && ps.checkError()) {
                    throw new IOException("Unable to write to output stream.");
                }
                bytesReadTotal += bytesRead;
                if (speed > 0) {
                    curTime = System.currentTimeMillis();
                    sleepTime = bytesReadTotal / speed * 1000 / 1024
                            - (curTime - startTime);
                    if (sleepTime > 0) {
                        try {
                            Thread.sleep(sleepTime);
                        } catch (InterruptedException ie) {
                        }
                    }
                }

                bytesRead = in.read(buf);
            }
            return bytesReadTotal;
        } finally {
            if (close) {
                out.close();
                in.close();
            }
        }
    }

    /**
     * Copies from one stream to another.
     * 
     * @param in
     *            InputStream to read from
     * @param out
     *            OutputStream to write to
     * @param buffSize
     *            the size of the buffer
     * @param close
     *            whether or not close the InputStream and OutputStream at the
     *            end. The streams are closed in the finally clause.
     */
    public static long copyBytes(InputStream in, OutputStream out,
            int buffSize, boolean close) throws IOException {

        PrintStream ps = out instanceof PrintStream ? (PrintStream) out : null;
        byte buf[] = new byte[buffSize];
        long totalBytes = 0;
        try {
            int bytesRead = in.read(buf);
            while (bytesRead >= 0) {
                out.write(buf, 0, bytesRead);
                totalBytes += bytesRead;
                if ((ps != null) && ps.checkError()) {
                    throw new IOException("Unable to write to output stream.");
                }
                bytesRead = in.read(buf);
            }
            return totalBytes;
        } finally {
            if (close) {
                out.close();
                in.close();
            }
        }
    }

    /**
     * Reads len bytes in a loop.
     * 
     * @param in
     *            The InputStream to read from
     * @param buf
     *            The buffer to fill
     * @param off
     *            offset from the buffer
     * @param len
     *            the length of bytes to read
     * @throws IOException
     *             if it could not read requested number of bytes for any reason
     *             (including EOF)
     */
    public static void readFully(InputStream in, byte buf[], int off, int len)
            throws IOException {
        int toRead = len;
        int tmpOff = off;
        while (toRead > 0) {
            int ret = in.read(buf, tmpOff, toRead);
            if (ret < 0) {
                throw new IOException("Premature EOF from inputStream");
            }
            toRead -= ret;
            tmpOff += ret;
        }
    }

    /**
     * Similar to readFully(). Skips bytes in a loop.
     * 
     * @param in
     *            The InputStream to skip bytes from
     * @param len
     *            number of bytes to skip.
     * @throws IOException
     *             if it could not skip requested number of bytes for any reason
     *             (including EOF)
     */
    public static void skipFully(InputStream in, long len) throws IOException {
        long tmpLen = len;
        while (tmpLen > 0) {
            long ret = in.skip(tmpLen);
            if (ret < 0) {
                throw new IOException("Premature EOF from inputStream");
            }
            tmpLen -= ret;
        }
    }

    /**
     * Close the Closeable objects and <b>ignore</b> any {@link IOException} or
     * null pointers. Must only be used for cleanup in exception handlers.
     * 
     * @param log
     *            the log to record problems to at debug level. Can be null.
     * @param closeables
     *            the objects to close
     */
    public static void cleanup(Logger log, java.io.Closeable... closeables) {
        for (java.io.Closeable c : closeables) {
            if (c != null) {
                try {
                    c.close();
                } catch (IOException e) {
                    if (log != null && log.isDebugEnabled()) {
                        log.debug("Exception in closing {} {}", c, e);
                    }
                }
            }
        }
    }

    /**
     * Closes the stream ignoring {@link IOException}. Must only be called in
     * cleaning up from exception handlers.
     * 
     * @param stream
     *            the Stream to close
     */
    public static void closeStream(java.io.Closeable stream) {
        cleanup(null, stream);
    }

    /**
     * Closes the socket ignoring {@link IOException}
     * 
     * @param sock
     *            the Socket to close
     */
    public static void closeSocket(Socket sock) {
        // avoids try { close() } dance
        if (sock != null) {
            try {
                sock.close();
            } catch (IOException ignored) {
            }
        }
    }

    public static void writeOptionString(DataOutput output, String value) throws IOException {
        boolean hasValue = !Strings.isNullOrEmpty(value);
        output.writeBoolean(hasValue);
        if (hasValue) {
            Text.writeString(output, value);
        }
    }
    public static String readOptionStringOrNull(DataInput input) throws IOException {
        if (input.readBoolean()) {
            return Text.readString(input);
        }
        return null;
    }
}
