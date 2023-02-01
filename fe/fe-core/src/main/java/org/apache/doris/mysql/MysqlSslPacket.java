package org.apache.doris.mysql;

import org.apache.doris.common.Config;

import java.nio.ByteBuffer;

public class MysqlSslPacket extends MysqlPacket {

    private int maxPacketSize;
    private int characterSet;
    private byte[] randomString;
    private MysqlCapability capability;

    public boolean readFrom(ByteBuffer buffer) {
        // read capability four byte, which CLIENT_PROTOCOL_41 must be set
        capability = new MysqlCapability(MysqlProto.readInt4(buffer));
        if (!capability.isProtocol41()) {
            return false;
        }
        // max packet size
        maxPacketSize = MysqlProto.readInt4(buffer);
        // character set. only support 33(utf-8)
        characterSet = MysqlProto.readInt1(buffer);
        // reserved 23 bytes
        if (new String(MysqlProto.readFixedString(buffer, 3)).equals(Config.proxy_auth_magic_prefix)) {
            randomString = new byte[MysqlPassword.SCRAMBLE_LENGTH];
            buffer.get(randomString);
        } else {
            buffer.position(buffer.position() + 20);
        }
        return true;
    }

    @Override
    public void writeTo(MysqlSerializer serializer) {

    }
}
