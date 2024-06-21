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

import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.longlong.Roaring64NavigableMap;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * Prepare the table and data in Doris:
 *
 * <pre>
 * {@code
 * CREATE TABLE `t_bitmap` (
 *   `rn` int(11) NULL,
 *   `uids` bitmap BITMAP_UNION 
 * ) AGGREGATE KEY(`rn`)
 * DISTRIBUTED BY HASH(`rn`) BUCKETS 1
 * PROPERTIES (
 *   "replication_num" = "1"
 * );
 *
 * INSERT INTO t_bitmap VALUES
 * (0, bitmap_empty()),
 * (1, to_bitmap(243)),
 * (2, bitmap_from_array([1,2,3,4,5,434543])),
 * (3, to_bitmap(287667876573)),
 * (4, bitmap_from_array([487667876573, 387627876573, 987567876573, 187667876573]));
 * }
 * </pre>
 *
 * The pom.xml dependency:
 * <pre>
 * {@code
 * <dependency>
 *    <groupId>mysql</groupId>
 *    <artifactId>mysql-connector-java</artifactId>
 *    <version>8.0.28</version>
 * </dependency>
 * <dependency>
 *    <groupId>org.roaringbitmap</groupId>
 *    <artifactId>RoaringBitmap</artifactId>
 *    <version>0.9.39</version>
 * </dependency>
 * }
 * </pre>
 */
public class ReadBitmap {
    public static void main(String[] args) throws Exception {

        try (Connection conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1:9030/test?user=root");
             Statement stmt = conn.createStatement()
        ) {
            stmt.execute("set return_object_data_as_binary=true"); // IMPORTANT!!!
            ResultSet rs = stmt.executeQuery("select uids from t_bitmap");
            while (rs.next()) {
                byte[] bytes = rs.getBytes(1);

                RoaringBitmap bitmap32 = new RoaringBitmap();
                // Only Roaring64NavigableMap can work, Roaring64Bitmap can't work!!!
                Roaring64NavigableMap bitmap64 = new Roaring64NavigableMap();
                switch (bytes[0]) {
                    case 0: // for empty bitmap
                        break;
                    case 1: // for only 1 element in bitmap32
                        bitmap32.add(ByteBuffer.wrap(bytes, 1, bytes.length - 1)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .getInt());
                        break;
                    case 2: // for more than 1 elements in bitmap32
                        bitmap32.deserialize(ByteBuffer.wrap(bytes, 1, bytes.length - 1));
                        break;
                    case 3: // for only 1 element in bitmap64
                        bitmap64.add(ByteBuffer.wrap(bytes, 1, bytes.length - 1)
                            .order(ByteOrder.LITTLE_ENDIAN)
                            .getLong());
                        break;
                    case 4: // for more than 1 elements in bitmap64
                        Object[] tuple2 = decodeVarint64(bytes);
                        int offset = (int) tuple2[1];
                        int newLen = 8 + bytes.length - offset;

                        try (ByteArrayOutputStream baos = new ByteArrayOutputStream(newLen);
                             DataOutputStream dos = new DataOutputStream(baos)) {
                            dos.write((byte[]) tuple2[0]);
                            dos.write(bytes, offset, bytes.length - offset);
                            dos.flush();
                            try (DataInputStream dis = new DataInputStream(
                                new ByteArrayInputStream(baos.toByteArray()))) {
                                bitmap64.deserializePortable(dis);
                            }
                        }
                        break;
                }
                System.out.println(bytes[0] <= 2 ? bitmap32 : bitmap64);
            }
        }
    }

    static Object[] decodeVarint64(byte[] bt) { // nolint
        long result = 0;
        int shift = 0;
        short B = 128;
        int idx = 1;
        for (; ; ) {
            short readByte = bt[idx];
            idx++;
            boolean isEnd = (readByte & B) == 0;
            result |= (long) (readByte & (B - 1)) << (shift * 7);
            if (isEnd) {
                break;
            }
            shift++;
        }
        byte[] bytes = new byte[8];
        for (int i = 0; i < bytes.length; i++) {
            // LITTLE_ENDIAN
            bytes[i] = (byte) (result >> 8 * i);
        }
        return new Object[]{bytes, idx};
    }
}
