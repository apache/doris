package org.apache.doris.mysql;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;

public class ProxyProtocolTest {
    private static final Logger LOG = org.slf4j.LoggerFactory.getLogger(ProxyProtocolTest.class);

    @Test
    public void testProcessProtocolHeaderV1() throws Exception {
        // test for v4
        ProxyProtocol.ConnectInfo connInfo = new ProxyProtocol.ConnectInfo();
        ByteBuffer buff_v4 = ByteBuffer.allocate(108);
        buff_v4.put("PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535".getBytes());
        buff_v4.put((byte)0x0D);buff_v4.put((byte)0x0A);
        ProxyProtocol.processProtocolHeaderV1(buff_v4, connInfo);
        Assert.assertTrue(connInfo.sourceAddress.getHostAddress().compareToIgnoreCase("255.255.255.255") == 0);
        Assert.assertTrue(connInfo.destAddress.getHostAddress().compareToIgnoreCase("255.255.255.255") == 0);
        Assert.assertTrue(connInfo.destPort == 65535);
        Assert.assertTrue(connInfo.sourcePort == 65535);

        // test for v6
        ByteBuffer buff_v6 = ByteBuffer.allocate(1000);
        buff_v6.put("PROXY TCP6 2001:db8:86a3:8d3:1319:8a2e:370:7344 2001:db8:86a3:8d3:1319:8a2e:370:7324 65534 65535".getBytes());
        buff_v6.put((byte)0x0D); buff_v6.put((byte)0x0A);
        ProxyProtocol.processProtocolHeaderV1(buff_v6, connInfo);
        Assert.assertTrue(connInfo.sourceAddress.getHostAddress().compareToIgnoreCase("2001:db8:86a3:8d3:1319:8a2e:370:7344") == 0);
        Assert.assertTrue(connInfo.destAddress.getHostAddress().compareToIgnoreCase("2001:db8:86a3:8d3:1319:8a2e:370:7324") == 0);
        Assert.assertTrue(connInfo.destPort == 65535);
        Assert.assertTrue(connInfo.sourcePort == 65534);
    }

    @Test(expected = IOException.class)
    public void testProcessProtocolHeaderV1_v4() throws Exception {
        ProxyProtocol.ConnectInfo connInfo = new ProxyProtocol.ConnectInfo();
        ByteBuffer buff_v4 = ByteBuffer.allocate(1000);
        buff_v4.put("PROXY TCP4 255.255.255.255 255.255.255.255 65535 65535".getBytes());
        buff_v4.put((byte)0x0A);
        ProxyProtocol.processProtocolHeaderV1(buff_v4, connInfo);
        Assert.fail("No exception throws");
    }

    @Test(expected = IOException.class)
    public void testProcessProtocolHeaderV1_v6() throws Exception {
        ProxyProtocol.ConnectInfo connInfo = new ProxyProtocol.ConnectInfo();
        ByteBuffer buff_v6 = ByteBuffer.allocate(1000);
        buff_v6.put("PROXY TCP6 2001:0db8:86a3:08d3:1319:8a2e:0370:7344 2001:0db8:86a3:08d3:1319:8a2e:0370:7324 65534 65535".getBytes());
        buff_v6.put((byte)0x0A);
        ProxyProtocol.processProtocolHeaderV1(buff_v6, connInfo);
        Assert.fail("No exception throws");
    }
}
