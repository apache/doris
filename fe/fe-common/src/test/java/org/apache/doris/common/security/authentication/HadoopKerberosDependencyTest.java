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

package org.apache.doris.common.security.authentication;

import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.apache.kerby.asn1.Asn1;
import org.apache.kerby.kerberos.kerb.keytab.Keytab;
import org.apache.kerby.kerberos.kerb.keytab.KeytabEntry;
import org.apache.kerby.kerberos.kerb.type.KerberosTime;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionKey;
import org.apache.kerby.kerberos.kerb.type.base.EncryptionType;
import org.apache.kerby.kerberos.kerb.type.base.PrincipalName;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.regex.Pattern;

public class HadoopKerberosDependencyTest {
    @TempDir
    private Path tempDir;

    @Test
    public void testHadoopReadsKerbyKeytab() throws Exception {
        String principal = "HTTP/localhost@EXAMPLE.COM";
        EncryptionKey key = new EncryptionKey(EncryptionType.AES128_CTS_HMAC_SHA1_96, new byte[16]);
        Keytab keytab = new Keytab();
        keytab.addEntry(new KeytabEntry(new PrincipalName(principal), KerberosTime.now(), 1, key));
        Path keytabPath = tempDir.resolve("service.keytab");
        keytab.store(keytabPath.toFile());

        Assertions.assertArrayEquals(new String[] {principal},
                KerberosUtil.getPrincipalNames(keytabPath.toString(), Pattern.compile("HTTP/.*")));
        Assertions.assertEquals(0,
                KerberosUtil.getPrincipalNames(keytabPath.toString(), Pattern.compile("hdfs/.*")).length);
    }

    @Test
    public void testShallowAsn1StillParses() throws Exception {
        Assertions.assertNotNull(Asn1.parse(nestedSequence(8, false)));
        Assertions.assertNotNull(Asn1.parse(nestedSequence(8, true)));
    }

    @Test
    public void testRejectDeeplyNestedDefiniteAsn1() throws Exception {
        byte[] encoded = nestedSequence(8192, false);
        Assertions.assertThrows(IOException.class, () -> Asn1.parse(encoded));
    }

    @Test
    public void testRejectDeeplyNestedIndefiniteAsn1() throws Exception {
        byte[] encoded = nestedSequence(8192, true);
        Assertions.assertThrows(IOException.class, () -> Asn1.parse(encoded));
    }

    private static byte[] nestedSequence(int depth, boolean indefinite) throws IOException {
        byte[] value = {0x02, 0x01, 0x01};
        for (int i = 0; i < depth; i++) {
            ByteArrayOutputStream encoded = new ByteArrayOutputStream(value.length + 8);
            encoded.write(0x30);
            if (indefinite) {
                encoded.write(0x80);
                encoded.write(value);
                encoded.write(0);
                encoded.write(0);
                value = encoded.toByteArray();
                continue;
            }

            int length = value.length;
            if (length < 128) {
                encoded.write(length);
                encoded.write(value);
                value = encoded.toByteArray();
                continue;
            }

            int lengthBytes = (Integer.SIZE - Integer.numberOfLeadingZeros(length) + 7) / 8;
            encoded.write(0x80 | lengthBytes);
            for (int shift = (lengthBytes - 1) * 8; shift >= 0; shift -= 8) {
                encoded.write(length >>> shift);
            }
            encoded.write(value);
            value = encoded.toByteArray();
        }
        return value;
    }
}
