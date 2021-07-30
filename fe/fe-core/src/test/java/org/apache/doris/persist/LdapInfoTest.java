package org.apache.doris.persist;

import org.junit.Assert;
import org.junit.Test;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

public class LdapInfoTest {

    @Test
    public void test() throws IOException {
        LdapInfo ldapInfo = new LdapInfo("123456");

        // 1. Write objects to file
        File file = new File("./ldapInfo");
        file.createNewFile();
        DataOutputStream dos = new DataOutputStream(new FileOutputStream(file));
        ldapInfo.write(dos);
        dos.flush();
        dos.close();

        // 2. Read objects from file
        DataInputStream dis = new DataInputStream(new FileInputStream(file));
        Assert.assertEquals("123456", LdapInfo.read(dis).getLdapPasswd());

        // 3. delete files
        dis.close();
        file.delete();
    }
}
