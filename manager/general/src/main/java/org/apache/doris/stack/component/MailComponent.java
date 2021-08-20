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

import org.apache.doris.stack.model.request.config.EmailInfo;
import org.apache.doris.stack.entity.CoreUserEntity;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.exception.EmailSendException;
import org.apache.doris.stack.service.config.ConfigConstant;
import com.github.mustachejava.DefaultMustacheFactory;
import com.github.mustachejava.Mustache;
import com.github.mustachejava.MustacheFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.mail.Authenticator;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeUtility;

import java.io.StringWriter;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Component
@Slf4j
public class MailComponent {

    protected static final String LOG_URL = "http://static.metabase.com/logo.png";

    @Autowired
    protected SettingComponent settingComponent;

    public void sendRestPasswordMail(String emailDest, String resetUrl, String hostName) {
        try {
            EmailInfo emailInfo = getEmailInfo();
            if (emailInfo == null) {
                log.warn("The email not config.");
                return;
            }

            Session mailSession = getEmailSession(emailInfo);
            if (mailSession == null) {
                log.warn("send reset password fail,get session error.");
                return;
            }
            Message message = new MimeMessage(mailSession);
            message.setFrom(new InternetAddress(emailInfo.getFromAddress()));
            message.setRecipient(MimeMessage.RecipientType.TO, new InternetAddress(emailDest));

            // TODO:The name is temporarily in Chinese
            message.setSubject(MimeUtility.encodeText("[Doris Studio] 密码重置请求", "utf-8", null));

            MustacheFactory mf = new DefaultMustacheFactory();
            Mustache m = mf.compile("mail/password_reset.mustache");
            MailComponent.ResetPassword resetPassword = new MailComponent.ResetPassword();
            resetPassword.setHostname(hostName);

            String siteUrl = settingComponent.readSetting(ConfigConstant.SITE_URL_KEY).getValue();
            log.debug("Get site url {} of doris studio.", siteUrl);

            resetPassword.setPasswordResetUrl(siteUrl + resetUrl);

            StringWriter writer = new StringWriter();
            m.execute(writer, resetPassword).flush();
            String htmlBody = writer.toString();

            message.setContent(htmlBody, "text/html;charset=utf-8");

            log.debug("Send reset password email.");
            // Send mail asynchronously
            sendEmail(message);
        } catch (Exception e) {
            log.error("Send reset password error {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    public void sendInvitationMail(String emailDest, String invitorName, String joinUrl) {
        try {
            log.debug("Invitor {} send invitation mail to {}.", invitorName, emailDest);
            log.debug("Join url is {}.", joinUrl);
            EmailInfo emailInfo = getEmailInfo();
            if (emailInfo == null) {
                log.warn("The email not config.");
                return;
            }

            Session mailSession = getEmailSession(emailInfo);
            if (mailSession == null) {
                log.warn("send invitation mail fail,get session error.");
                return;
            }
            Message message = new MimeMessage(mailSession);
            if (emailInfo.getFromAddress() == null) {
                message.setFrom(new InternetAddress());
            } else {
                message.setFrom(new InternetAddress(emailInfo.getFromAddress()));
            }
            message.setRecipient(MimeMessage.RecipientType.TO, new InternetAddress(emailDest));

            // TODO:The name is temporarily in Chinese
            message.setSubject(MimeUtility.encodeText("[Doris Studio] 新用户邀请", "utf-8", null));

            MustacheFactory mf = new DefaultMustacheFactory();
            Mustache m = mf.compile("mail/new_user_invite.mustache");

            MailComponent.NewUserInvitation userInvitation = new MailComponent.NewUserInvitation();
            userInvitation.setInvitorName(invitorName);

            String siteUrl = settingComponent.readSetting(ConfigConstant.SITE_URL_KEY).getValue();

            log.debug("Get site url {} of doris studio.", siteUrl);

            userInvitation.setJoinUrl(siteUrl + joinUrl);

            StringWriter writer = new StringWriter();
            m.execute(writer, userInvitation).flush();
            String htmlBody = writer.toString();

            message.setContent(htmlBody, "text/html;charset=utf-8");

            log.debug("Send invite email.");

            sendEmail(message);
        } catch (Exception e) {
            log.error("Send reset password error {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    public void sendUserJoineNote(List<CoreUserEntity> activeAdminUsers, CoreUserEntity joinUser) {
        try {
            log.debug("send user join notification mail");
            EmailInfo emailInfo = getEmailInfo();
            if (emailInfo == null) {
                log.warn("The email not config.");
                return;
            }

            Session mailSession = getEmailSession(emailInfo);
            if (mailSession == null) {
                log.warn("send reset password fail,get session error.");
                return;
            }

            String siteUrl = settingComponent.readSetting(ConfigConstant.SITE_URL_KEY).getValue();
            log.debug("Get site url {} of doris studio.", siteUrl);
            String joinedUserEidtUrl = siteUrl + "/admin/people";

            for (CoreUserEntity adminUser : activeAdminUsers) {
                log.debug("Send user joined notification email to admin user {}.", adminUser.getEmail());
                Message message = new MimeMessage(mailSession);
                if (emailInfo.getFromAddress() == null) {
                    message.setFrom(new InternetAddress());
                } else {
                    message.setFrom(new InternetAddress(emailInfo.getFromAddress()));
                }
                message.setRecipient(MimeMessage.RecipientType.TO, new InternetAddress(adminUser.getEmail()));

                // TODO:The name is temporarily in Chinese
                message.setSubject(MimeUtility.encodeText("[Doris Studio] 用户加入通知", "utf-8", null));
                message.setSentDate(new Date());

                MustacheFactory mf = new DefaultMustacheFactory();
                Mustache m = mf.compile("mail/user_joined_notification.mustache");

                MailComponent.UserJoinedNotice joinedNotice = new MailComponent.UserJoinedNotice();
                joinedNotice.setAdminEmail(adminUser.getEmail());
                joinedNotice.setJoinedUserName(joinUser.getFirstName());
                joinedNotice.setJoinedUserEmail(joinUser.getEmail());
                joinedNotice.setJoinedUserEditUrl(joinedUserEidtUrl);

                StringWriter writer = new StringWriter();
                m.execute(writer, joinedNotice).flush();

                String htmlBody = writer.toString();

                message.setContent(htmlBody, "text/html;charset=utf-8");
                log.debug("Send user joined notification email.");

                sendEmail(message);
            }
        } catch (Exception e) {
            log.error("Send user join notification error {}.", e.getMessage());
            e.printStackTrace();
        }
    }

    public void sendTestEmail(String userMail) throws Exception {
        EmailInfo emailInfo = getEmailInfo();
        if (emailInfo == null) {
            log.error("The email not config.");
            throw new EmailSendException();
        }

        Session mailSession = getEmailSession(emailInfo);
        if (mailSession == null) {
            log.error("send reset password fail,get session error.");
            throw new EmailSendException();
        }

        Message message = new MimeMessage(mailSession);
        if (emailInfo.getFromAddress() == null) {
            message.setFrom(new InternetAddress());
        } else {
            message.setFrom(new InternetAddress(emailInfo.getFromAddress()));
        }
        message.setRecipient(MimeMessage.RecipientType.TO, new InternetAddress(userMail));

        // TODO:The name is temporarily in Chinese
        message.setSubject(MimeUtility.encodeText("[Doris Studio] 测试邮件", "utf-8", null));

        String testContent = "Your Doris Studio emails are working — hooray!";
        // Text content of the message
        message.setContent(testContent, "text/html;charset=UTF-8");

        Transport.send(message);
        log.debug("send mail success.");
    }

    /**
     * Check whether the cache exists
     * @return
     */
    public boolean checkConfigExist() {
        SettingEntity emailConfiguredKey = settingComponent.readSetting(ConfigConstant.EMAIL_CONFIGURED_KEY);
        if (emailConfiguredKey == null) {
            log.debug("The email config not exist");
            return false;
        } else {
            log.debug("The email config exist");
            return true;
        }
    }

    /**
     * Get configured mailbox service information
     * @return
     */
    public EmailInfo getEmailInfo() {
        log.debug("Get email info from setting.");

        boolean configured = checkConfigExist();
        if (!configured) {
            return null;
        }

        EmailInfo emailInfo = new EmailInfo();

        SettingEntity hostSettingEntity = settingComponent.readSetting(ConfigConstant.EMAIL_HOST_KEY);
        if (hostSettingEntity != null) {
            emailInfo.setSmtpHost(hostSettingEntity.getValue());
        }

        SettingEntity portSettingEntity = settingComponent.readSetting(ConfigConstant.EMAIL_PORT_KEY);
        if (portSettingEntity != null) {
            emailInfo.setSmtpPort(portSettingEntity.getValue());
        }

        SettingEntity addressSettingEntity = settingComponent.readSetting(ConfigConstant.EMAIL_ADDRESS_KEY);
        if (addressSettingEntity != null) {
            emailInfo.setFromAddress(addressSettingEntity.getValue());
        }

        SettingEntity secSettingEntity = settingComponent.readSetting(ConfigConstant.EMAIL_SECURITY_KEY);
        if (secSettingEntity != null) {
            emailInfo.setSmtpSecurity(secSettingEntity.getValue());
        }

        if (emailInfo.getSmtpSecurity() != null
                && !emailInfo.getSmtpSecurity().equals(EmailInfo.SmtpSecurity.none.name())) {
            SettingEntity passwdSettingEntity = settingComponent.readSetting(ConfigConstant.EMAIL_USER_NAME_KEY);
            if (passwdSettingEntity != null) {
                emailInfo.setSmtpUsername(passwdSettingEntity.getValue());
            }

            SettingEntity userSettingEntity = settingComponent.readSetting(ConfigConstant.EMAIL_PASSWORD_KEY);
            if (userSettingEntity != null) {
                emailInfo.setSmtpPassword(userSettingEntity.getValue());
            }
        }

        return emailInfo;
    }

    /**
     * Send mail asynchronously
     *
     * @param message
     */
    protected void sendEmail(Message message) {
        ExecutorService threadPool = Executors.newFixedThreadPool(1);
        MailComponent.MailSender sender = new MailComponent.MailSender(message);
        threadPool.submit(sender);
    }

    /**
     * Get send mail session information
     * @param emailInfo
     * @return
     */
    protected Session getEmailSession(EmailInfo emailInfo) {
        try {
            log.info("Get email session by email info.");

            Properties props = new Properties();
            props.put("mail.transport.protocol", ConfigConstant.EMAIL_PROTOCOL_KEY);
            props.put("mail.smtp.host", emailInfo.getSmtpHost());
            props.put("mail.smtp.port", emailInfo.getSmtpPort());
            EmailInfo.SmtpSecurity smtpSecurity = EmailInfo.SmtpSecurity.valueOf(emailInfo.getSmtpSecurity());
            switch (smtpSecurity) {
                case ssl:
                    props.put("mail.smtp.auth", true);
                    props.put("mail.smtp.ssl.enable", true);
                    props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
                    break;
                case tls:
                    props.put("mail.smtp.starttls.enable", true);
                    break;
                case starttls:
                    props.put("mail.smtp.auth", false);
                    props.put("mail.smtp.socketFactory.port", emailInfo.getSmtpHost());
                    props.put("mail.smtp.socketFactory.class", "javax.net.ssl.SSLSocketFactory");
                    props.put("mail.smtp.socketFactory.fallback", false);
                    props.put("mail.smtp.starttls.enable", true);
                    break;
                case none:
                    props.put("mail.smtp.auth", false);
                    break;
                default:
                    break;
            }

            Session session = null;
            if (smtpSecurity == EmailInfo.SmtpSecurity.none) {
                session = Session.getDefaultInstance(props);
            } else {
                Authenticator auth = new Authenticator() {
                    public PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(emailInfo.getSmtpUsername(), emailInfo.getSmtpPassword());
                    }
                };
                session = Session.getInstance(props, auth);
            }
            return session;
        } catch (Exception e) {
            log.error("Get Email session exception {}", e.getMessage());
            e.printStackTrace();
            return null;
        }
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class Application {

        private String applicationName = "Doris Studio";

        private String applicationLogoUrl = LOG_URL;
    }

    /**
     * Reset password message content
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class ResetPassword extends MailComponent.Application {

        private String hostname;

        private String passwordResetUrl;
    }

    /**
     * Invite new users to add mail content
     */
    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    private static class NewUserInvitation extends MailComponent.Application {

        private String invitorName;

        private String joinUrl;

        private Date today = new Date();
    }

    /**
     * User has added reminder email content
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class UserJoinedNotice extends MailComponent.Application {

        private String joinedUserName;

        private String adminEmail;

        private String joinedUserEmail;

        private Date joinedDate = new Date();

        private String joinedUserEditUrl;

        private String joinedViaSSO;

    }

    protected class MailSender implements Runnable {

        private Message message;

        public MailSender(Message message) {
            this.message = message;
        }

        @Override
        public void run() {
            try {
                Transport.send(message);
                log.debug("send mail success.");
            } catch (MessagingException e) {
                log.error("Send message exception {}", e.getMessage());
                e.printStackTrace();
            }
        }
    }
}
