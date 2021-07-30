package org.apache.doris.ldap;

import com.clearspring.analytics.util.Lists;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Mocked;
import org.apache.doris.common.LdapConfig;
import org.apache.doris.common.util.SymmetricEncryption;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.ldap.core.LdapTemplate;
import org.springframework.ldap.core.support.AbstractContextMapper;
import org.springframework.ldap.query.LdapQuery;

import java.util.List;

public class LdapClientTest {
    private static final String ADMIN_PASSWORD = "admin";

    @Mocked
    private LdapTemplate ldapTemplate;

    @Before
    public void setUp() {
        LdapConfig.ldap_authentication_enabled = true;
        LdapConfig.ldap_host = "127.0.0.1";
        LdapConfig.ldap_port = 389;
        LdapConfig.ldap_admin_name = "cn=admin,dc=baidu,dc=com";
        LdapConfig.ldap_user_basedn = "dc=baidu,dc=com";
        LdapConfig.ldap_group_basedn = "ou=group,dc=baidu,dc=com";
        LdapConfig.ldap_user_filter = "(&(uid={login}))";
        LdapClient.init(SymmetricEncryption.encrypt(ADMIN_PASSWORD));
    }
    
    private void mockLdapTemplateSearch(List list) {
        new Expectations() {
            {
                ldapTemplate.search((LdapQuery) any, (AbstractContextMapper) any);
                minTimes = 0;
                result = list;
            }
        };
    }

    private void mockLdapTemplateAuthenticate(String password) {
        new Expectations() {
            {
                ldapTemplate.authenticate((LdapQuery) any, anyString);
                minTimes = 0;
                result = new Delegate() {
                    void fakeAuthenticate(LdapQuery query, String passwd) {
                        if (passwd.equals(password)) {
                            return;
                        } else {
                            throw new RuntimeException("exception");
                        }
                    }
                };
            }
        };
    }

    @Test
    public void testDoesUserExist() {
        List<String> list = Lists.newArrayList();
        list.add("zhangsan");
        mockLdapTemplateSearch(list);
        Assert.assertTrue(LdapClient.doesUserExist("zhangsan"));
    }

    @Test
    public void testDoesUserExistFail() {
        mockLdapTemplateSearch(null);
        Assert.assertFalse(LdapClient.doesUserExist("zhangsan"));
    }

    @Test(expected = RuntimeException.class)
    public void testDoesUserExistException() {
        List<String> list = Lists.newArrayList();
        list.add("zhangsan");
        list.add("zhangsan");
        mockLdapTemplateSearch(list);
        Assert.assertTrue(LdapClient.doesUserExist("zhangsan"));
        Assert.fail("No Exception throws.");
    }

    @Test
    public void testCheckPassword() {
        mockLdapTemplateAuthenticate(ADMIN_PASSWORD);
        Assert.assertTrue(LdapClient.checkPassword("zhangsan", ADMIN_PASSWORD));
        Assert.assertFalse(LdapClient.checkPassword("zhangsan", "123"));
    }

    @Test
    public void testGetGroups() {
        List<String> list = Lists.newArrayList();
        list.add("cn=groupName,ou=groups,dc=example,dc=com");
        mockLdapTemplateSearch(list);
        Assert.assertEquals(1, LdapClient.getGroups("zhangsan").size());
    }
}
