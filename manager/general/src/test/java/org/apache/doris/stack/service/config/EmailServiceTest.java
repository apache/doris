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

package org.apache.doris.stack.service.config;

import static org.mockito.Mockito.when;

import org.apache.doris.stack.model.request.config.EmailInfo;
import org.apache.doris.stack.component.MailComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.exception.RequestFieldNullException;
import lombok.extern.slf4j.Slf4j;
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

@RunWith(JUnit4.class)
@Slf4j
public class EmailServiceTest {

    @InjectMocks
    private EmailService emailService;

    @Mock
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
     * test delete
     */
    @Test
    public void testDeleteEmailInfo() {
        log.debug("Test delete email config info.");
        // No mail configuration
        when(mailComponent.checkConfigExist()).thenReturn(false);
        emailService.deleteEmailInfo();

        // mail configuration
        when(mailComponent.checkConfigExist()).thenReturn(true);
        emailService.deleteEmailInfo();

        // Different security protocol tests
        SettingEntity secSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.ssl.name());
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(secSettingEntity);
        emailService.deleteEmailInfo();

        secSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.none.name());
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(secSettingEntity);
        emailService.deleteEmailInfo();
    }

    /**
     * The test updates the email information, and the request information is abnormal
     */
    @Test
    public void testUpdateEmailInfoRequestError() {
        log.debug("Test update or add email config info request error.");
        EmailInfo emailInfo = new EmailInfo();
        // request exception
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        emailInfo.setSmtpHost("12.324.343.4");
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        emailInfo.setSmtpPort("8080");
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        emailInfo.setSmtpSecurity(EmailInfo.SmtpSecurity.ssl.name());
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        emailInfo.setFromAddress("address");
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        emailInfo.setSmtpUsername("user");
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            Assert.assertEquals(RequestFieldNullException.MESSAGE, e.getMessage());
        }

        emailInfo.setSmtpPassword("password");
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * The test updates the email information, and the request information is abnormal
     */
    @Test
    public void testUpdateEmailInfo() {
        log.debug("Test update or add email config info.");
        EmailInfo emailInfo = new EmailInfo();
        emailInfo.setSmtpHost("12.324.343.4");
        emailInfo.setSmtpPort("8080");
        emailInfo.setSmtpSecurity(EmailInfo.SmtpSecurity.none.name());
        emailInfo.setFromAddress("address");

        // mail configuration not enable
        when(mailComponent.checkConfigExist()).thenReturn(false);
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        // mail configuration enable
        // The old security protocol configuration is none,
        // and the new security protocol configuration is none
        SettingEntity oldSecSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.none.name());
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(oldSecSettingEntity);
        when(mailComponent.checkConfigExist()).thenReturn(true);
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        // The old security protocol configuration is SSL and
        // the new security protocol configuration is none
        oldSecSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.ssl.name());
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(oldSecSettingEntity);
        when(mailComponent.checkConfigExist()).thenReturn(true);
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }

        // The old security protocol configuration is SSL,
        // and the new security protocol configuration is also SSL
        oldSecSettingEntity =
                new SettingEntity(ConfigConstant.EMAIL_SECURITY_KEY, EmailInfo.SmtpSecurity.ssl.name());
        emailInfo.setSmtpSecurity(EmailInfo.SmtpSecurity.ssl.name());
        emailInfo.setSmtpUsername("user");
        emailInfo.setSmtpPassword("password");
        when(settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY)).thenReturn(oldSecSettingEntity);
        when(mailComponent.checkConfigExist()).thenReturn(true);
        try {
            emailService.updateEmailInfo(emailInfo);
        } catch (Exception e) {
            log.error(e.getMessage());
            e.printStackTrace();
        }
    }
}
