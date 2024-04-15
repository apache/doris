package org.apache.doris.mysql.authenticate.ldap;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.cluster.ClusterNamespace;
import org.apache.doris.common.ErrorCode;
import org.apache.doris.common.ErrorReport;
import org.apache.doris.mysql.MysqlAuthPacket;
import org.apache.doris.mysql.MysqlAuthSwitchPacket;
import org.apache.doris.mysql.MysqlChannel;
import org.apache.doris.mysql.MysqlClearTextPacket;
import org.apache.doris.mysql.MysqlHandshakePacket;
import org.apache.doris.mysql.MysqlProto;
import org.apache.doris.mysql.MysqlSerializer;
import org.apache.doris.mysql.authenticate.AuthenticateController;
import org.apache.doris.mysql.privilege.Auth;
import org.apache.doris.qe.ConnectContext;

import com.google.common.base.Strings;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * This class is used for LDAP authentication login and LDAP group authorization.
 * This means that users can log in to Doris with a user name and LDAP password,
 * and the user will get the privileges of all roles corresponding to the LDAP group.
 */
public class LdapAuthenticateController implements AuthenticateController {
    private static final Logger LOG = LogManager.getLogger(LdapAuthenticateController.class);

    /*
     * ldap:
     * server ---AuthSwitch---> client
     * server <--- clear text password --- client
     */
    @Override
    public boolean authenticate(ConnectContext context,
            String qualifiedUser,
            MysqlChannel channel,
            MysqlSerializer serializer,
            MysqlAuthPacket authPacket,
            MysqlHandshakePacket handshakePacket) throws IOException {
        if (LOG.isDebugEnabled()) {
            LOG.debug("user:{} start to ldap authenticate.", qualifiedUser);
        }
        // server send authentication switch packet to request password clear text.
        // https://dev.mysql.com/doc/internals/en/authentication-method-change.html
        serializer.reset();
        MysqlAuthSwitchPacket mysqlAuthSwitchPacket = new MysqlAuthSwitchPacket();
        mysqlAuthSwitchPacket.writeTo(serializer);
        channel.sendAndFlush(serializer.toByteBuffer());

        // Server receive password clear text.
        ByteBuffer authSwitchResponse = channel.fetchOnePacket();
        if (authSwitchResponse == null) {
            return false;
        }
        MysqlClearTextPacket clearTextPacket = new MysqlClearTextPacket();
        if (!clearTextPacket.readFrom(authSwitchResponse)) {
            ErrorReport.report(ErrorCode.ERR_NOT_SUPPORTED_AUTH_MODE);
            MysqlProto.sendResponsePacket(context);
            return false;
        }
        if (!internalAuthenticate(context, clearTextPacket.getPassword(), qualifiedUser)) {
            MysqlProto.sendResponsePacket(context);
            return false;
        }
        return true;
    }

    @Override
    public boolean canDeal(String qualifiedUser) {
        if (qualifiedUser.equals(Auth.ROOT_USER) || qualifiedUser.equals(Auth.ADMIN_USER)) {
            return false;
        }
        if (!Env.getCurrentEnv().getAuth().getLdapManager().doesUserExist(qualifiedUser)) {
            return false;
        }
        return true;
    }

    /**
     * The LDAP authentication process is as follows:
     * step1: Check the LDAP password.
     * step2: Get the LDAP groups privileges as a role, saved into ConnectContext.
     * step3: Set current userIdentity. If the user account does not exist in Doris, login as a temporary user.
     * Otherwise, login to the Doris account.
     */
    private boolean internalAuthenticate(ConnectContext context, String password, String qualifiedUser) {
        String usePasswd = (Strings.isNullOrEmpty(password)) ? "NO" : "YES";
        String userName = ClusterNamespace.getNameFromFullName(qualifiedUser);
        if (LOG.isDebugEnabled()) {
            LOG.debug("user:{}", userName);
        }

        // check user password by ldap server.
        try {
            if (!Env.getCurrentEnv().getAuth().getLdapManager().checkUserPasswd(qualifiedUser, password)) {
                LOG.info("user:{} use check LDAP password failed.", userName);
                ErrorReport.report(ErrorCode.ERR_ACCESS_DENIED_ERROR, qualifiedUser, context.getRemoteIP(), usePasswd);
                return false;
            }
        } catch (Exception e) {
            LOG.error("Check ldap password error.", e);
            return false;
        }

        String remoteIp = context.getMysqlChannel().getRemoteIp();
        UserIdentity tempUserIdentity = UserIdentity.createAnalyzedUserIdentWithIp(qualifiedUser, remoteIp);
        // Search the user in doris.
        List<UserIdentity> userIdentities = Env.getCurrentEnv().getAuth()
                .getUserIdentityForLdap(qualifiedUser, remoteIp);
        UserIdentity userIdentity;
        if (userIdentities.isEmpty()) {
            userIdentity = tempUserIdentity;
            if (LOG.isDebugEnabled()) {
                LOG.debug("User:{} does not exists in doris, login as temporary users.", userName);
            }
            context.setIsTempUser(true);
        } else {
            userIdentity = userIdentities.get(0);
        }

        context.setCurrentUserIdentity(userIdentity);
        context.setRemoteIP(remoteIp);
        if (LOG.isDebugEnabled()) {
            LOG.debug("ldap authentication success: identity:{}", context.getCurrentUserIdentity());
        }
        return true;
    }
}
