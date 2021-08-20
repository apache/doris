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

package org.apache.doris.stack.component;

import static org.mockito.Mockito.when;

import org.apache.doris.stack.model.request.config.EmailInfo;
import org.apache.doris.stack.entity.CoreUserEntity;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.exception.EmailSendException;
import org.apache.doris.stack.service.config.ConfigConstant;
import lombok.extern.slf4j.Slf4j;
import org.assertj.core.util.Lists;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.List;

@RunWith(JUnit4.class)
@Slf4j
public class MailComponentTest {

    @InjectMocks
    private MailComponent mailComponent;

    @Mock
    private SettingComponent settingComponent;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * Mail service enable
     */
    @Test
    public void testEmailInfo() {
        // Mail service not enable
        when(settingComponent.readSetting(ConfigConstant.EMAIL_CONFIGURED_KEY)).thenReturn(null);
        Assert.assertEquals(mailComponent.getEmailInfo(), null);

        // mock mail configuration
        readConfigMock();

        EmailInfo emailInfo = mailComponent.getEmailInfo();
        Assert.assertEquals(emailInfo.getSmtpHost(), "123.34.434.43");
        Assert.assertEquals(emailInfo.getSmtpPort(), "8080");
        Assert.assertEquals(emailInfo.getFromAddress(), "mail.com");
        Assert.assertEquals(emailInfo.getSmtpSecurity(), "ssl");
        Assert.assertEquals(emailInfo.getSmtpUsername(), "user");
        Assert.assertEquals(emailInfo.getSmtpPassword(), "passwd");

        // No security protocol
        SettingEntity secSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.none.name());
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(secSettingEntity);
        emailInfo = mailComponent.getEmailInfo();

        Assert.assertEquals(emailInfo.getSmtpSecurity(), "none");
        Assert.assertEquals(emailInfo.getSmtpUsername(), null);
        Assert.assertEquals(emailInfo.getSmtpPassword(), null);

        // Security protocol configuration failure
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(null);
        emailInfo = mailComponent.getEmailInfo();

        Assert.assertEquals(emailInfo.getSmtpSecurity(), null);
        Assert.assertEquals(emailInfo.getSmtpUsername(), null);
        Assert.assertEquals(emailInfo.getSmtpPassword(), null);

    }

    /**
     * Testing the sending reset password method will test the getemailsession method,
     * so there is no need to test other sending mail methods
     */
    @Test
    public void testSendRestPasswordMail() {
        String emailDest = "destMail";
        String resetUrl = "url";
        String hostName = "host";

        // No mail configuration
        when(settingComponent.readSetting(ConfigConstant.EMAIL_CONFIGURED_KEY)).thenReturn(null);
        mailComponent.sendRestPasswordMail(emailDest, resetUrl, hostName);

        // siteUrl excpetion
        readConfigMock();
        mailComponent.sendRestPasswordMail(emailDest, resetUrl, hostName);

        // mock siteUrl
        siteUrlMock();
        mailComponent.sendRestPasswordMail(emailDest, resetUrl, hostName);

        // Mail security protocol configuration exception
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(null);
        mailComponent.sendRestPasswordMail(emailDest, resetUrl, hostName);

        // Testing different mail security protocols
        // none
        SettingEntity secSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.none.name());
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(secSettingEntity);
        mailComponent.sendRestPasswordMail(emailDest, resetUrl, hostName);

        // tls
        secSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.tls.name());
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(secSettingEntity);
        mailComponent.sendRestPasswordMail(emailDest, resetUrl, hostName);

        // starttls
        secSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.starttls.name());
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(secSettingEntity);
        mailComponent.sendRestPasswordMail(emailDest, resetUrl, hostName);
    }

    @Test
    public void testSendInvitationMail() {
        String emailDest = "emailDest";
        String invitorName = "invitorName";
        String joinUrl = "joinUrl";
        // No mail configuration
        when(settingComponent.readSetting(ConfigConstant.EMAIL_CONFIGURED_KEY)).thenReturn(null);
        mailComponent.sendInvitationMail(emailDest, invitorName, joinUrl);

        // siteUrl exception
        readConfigMock();
        mailComponent.sendInvitationMail(emailDest, invitorName, joinUrl);

        // mock siteUrl
        siteUrlMock();
        mailComponent.sendInvitationMail(emailDest, invitorName, joinUrl);
    }

    @Test
    public void testSendUserJoineNote() {
        List<CoreUserEntity> activeAdminUsers = Lists.newArrayList(new CoreUserEntity());
        CoreUserEntity joinUser = new CoreUserEntity();

        // No mail configuration
        when(settingComponent.readSetting(ConfigConstant.EMAIL_CONFIGURED_KEY)).thenReturn(null);
        mailComponent.sendUserJoineNote(activeAdminUsers, joinUser);

        readConfigMock();
        mailComponent.sendUserJoineNote(activeAdminUsers, joinUser);

        siteUrlMock();
        mailComponent.sendUserJoineNote(activeAdminUsers, joinUser);
    }

    @Test
    public void testSendTestEmail() {
        String userMail = "userMail";

        // No mail configuration
        when(settingComponent.readSetting(ConfigConstant.EMAIL_CONFIGURED_KEY)).thenReturn(null);
        try {
            mailComponent.sendTestEmail(userMail);
        } catch (Exception e) {
            Assert.assertEquals(EmailSendException.MESSAGE, e.getMessage());
        }

        // mail configuration normal
        readConfigMock();
        try {
            mailComponent.sendTestEmail(userMail);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        // mail configuration exception
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(null);
        try {
            mailComponent.sendTestEmail(userMail);
        } catch (Exception e) {
            Assert.assertEquals(EmailSendException.MESSAGE, e.getMessage());
        }
    }

    // mock mail configuration
    private void readConfigMock() {
        SettingEntity settingEntity =
                new SettingEntity(ConfigConstant.EMAIL_CONFIGURED_KEY, "true");
        when(settingComponent.readSetting(ConfigConstant.EMAIL_CONFIGURED_KEY)).thenReturn(settingEntity);

        SettingEntity hostSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_HOST_KEY, "123.34.434.43");
        when(settingComponent.readSetting(ConfigConstant.EMAIL_HOST_KEY)).thenReturn(hostSettingEntity);

        SettingEntity portSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_PORT_KEY, "8080");
        when(settingComponent.readSetting(ConfigConstant.EMAIL_PORT_KEY)).thenReturn(portSettingEntity);

        SettingEntity addressSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_ADDRESS_KEY, "mail.com");
        when(settingComponent.readSetting(ConfigConstant.EMAIL_ADDRESS_KEY)).thenReturn(addressSettingEntity);

        SettingEntity secSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.ssl.name());
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(secSettingEntity);

        SettingEntity passwdSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_PASSWORD_KEY, "passwd");
        when(settingComponent.readSetting(ConfigConstant.EMAIL_PASSWORD_KEY)).thenReturn(passwdSettingEntity);

        SettingEntity userSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_USER_NAME_KEY, "user");
        when(settingComponent.readSetting(ConfigConstant.EMAIL_USER_NAME_KEY)).thenReturn(userSettingEntity);
    }

    // mock site-url configuration
    private void siteUrlMock() {
        SettingEntity settingEntity =
                new SettingEntity(ConfigConstant.SITE_URL_KEY, "12.32.43");
        when(settingComponent.readSetting(ConfigConstant.SITE_URL_KEY)).thenReturn(settingEntity);
    }

}
