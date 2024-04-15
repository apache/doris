package org.apache.doris.mysql.authenticate;

import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.ldap.LdapAuthenticateController;
import org.apache.doris.qe.ConnectContext;

import java.io.IOException;

public class AuthenticateControllerManager {
    private AuthenticateController defaultController;
    private AuthenticateController authTypeController;

    public AuthenticateControllerManager(AuthenticateType type) {
        this.defaultController = new DefaultAuthenticateController();
        switch (type) {
            case LDAP:
                this.authTypeController = new LdapAuthenticateController();
                break;
            case DEFAULT:
            default:
                this.authTypeController = defaultController;
                break;
        }
    }

    public boolean authenticate(ConnectContext context,
            String qualifiedUser,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        if (authTypeController.canDeal(qualifiedUser)) {
            return authTypeController.authenticate(context, qualifiedUser, channel, serializer, authPacket, handshakePacket);
        } else {
            return defaultController.authenticate(context, qualifiedUser, channel, serializer, authPacket, handshakePacket);
        }
    }


}
