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

package org.apache.doris.stack.service;

import static org.apache.doris.stack.service.user.AuthenticationService.failedLoginMap;

import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.util.CredsUtil;
import org.apache.doris.stack.util.UuidUtil;
import org.apache.doris.stack.entity.CoreUserEntity;
import org.apache.doris.stack.exception.InputFormatException;
import org.apache.doris.stack.exception.PasswordFormatException;
import org.apache.doris.stack.exception.RequestFieldNullException;
import org.apache.doris.stack.exception.UserDisabledException;
import org.apache.doris.stack.exception.UserLoginException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
public class UtilService extends BaseService {

    private static final String EMAIL_REGEX = "[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*@"
            + "(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?";

    @Autowired
    public UtilService() {
    }

    /**
     * Password strength check: the password can be composed of letters (case), numbers and special characters
     * Weak password strength: only the minimum password length of 6 is met, and there is only one character (letter)
     * General password strength: it meets the minimum password length of 6 and has two lengths (characters and numbers)
     * Strong password strength: the minimum length of the password is 8, and it is composed of at least one lowercase,
     * uppercase, number and special characters
     * @param passwd
     * TODO:At present, only the length of the password and whether it contains numbers are checked.
     * In the future, the password strength check needs to be increased
     * @return 是否合格
     */
    public boolean passwordCheck(String passwd) throws Exception {
        log.debug("Check password format.");
        if (passwd == null || passwd.isEmpty()) {
            log.error("The password is empty.");
            throw new RequestFieldNullException();
        }
        int lenght = passwd.length();
        // Check whether the password meets the minimum count requirements for each character class
        if (passwd.length() < 6) {
            log.error("Password length is less than 6.");
            throw new PasswordFormatException();
        }

        // Check that the password contains only one character
        // Contains only numbers
        if (passwd.length() - passwd.replaceAll("[0-9]", "").length() == lenght) {
            log.error("The password contains only numbers.");
            throw new PasswordFormatException();
        }

        // Contains only uppercase letters
        if (passwd.length() - passwd.replaceAll("[A-Z]", "").length() == lenght) {
            log.error("The password contains only uppercase letters.");
            throw new PasswordFormatException();
        }

        // Contains only lowercase letters
        if (passwd.length() - passwd.replaceAll("[a-z]", "").length() == lenght) {
            log.error("The password contains only lowercase letters.");
            throw new PasswordFormatException();
        }

        log.debug("The password is qualified.");
        return true;
    }

    /**
     * Verify whether the password entered by the user is consistent with that stored in the database
     * @param salt
     * @param inputPasswd,Password plaintext
     * @param hash,Password and ciphertext stored in the database
     * @return
     * @throws Exception
     */
    public boolean verifyPassword(String salt, String inputPasswd, String hash) throws Exception {
        log.debug("Verify password.");
        String password = passwordSalt(salt, inputPasswd);

        boolean passwordCheck = CredsUtil.bcryptVerify(password, hash);
        if (!passwordCheck) {
            log.error("Password error.");
            throw new UserLoginException();
        }
        return true;
    }

    // Verify the user name and password. Because verifypassword is used to reset the password, it will not pass ID
    public boolean verifyLoginPassword(String salt, String inputPasswd, String hash, int userId) throws Exception {
        log.debug("Verify password.");
        String password = passwordSalt(salt, inputPasswd);

        boolean passwordCheck = CredsUtil.bcryptVerify(password, hash);
        if (!passwordCheck) {
            log.error("Password error.");
            List<Long> loginAttempts = failedLoginMap.getOrDefault(userId, new ArrayList<>());
            loginAttempts.add(System.currentTimeMillis());
            failedLoginMap.put(userId, loginAttempts);
            throw new UserLoginException();
        }
        return true;
    }

    /**
     * Check whether the email format is qualified
     * @param email
     * @return
     * @throws Exception
     */
    public boolean emailCheck(String email) throws Exception {
        log.debug("Check email format.");
        if (email == null || email.isEmpty()) {
            log.error("The email is empty.");
            throw new RequestFieldNullException();
        }

        boolean isMatch = email.matches(EMAIL_REGEX);

        if (!isMatch) {
            log.error("The email {} format is error.", email);
            throw new InputFormatException();
        }

        log.debug("The mail format is qualified.");

        return true;
    }

    /**
     * Detect whether the user is acitve
     * @param user
     * @throws Exception
     */
    public void checkUserActive(CoreUserEntity user) throws Exception {
        log.debug("Check user {} is active.", user.getId());
        if (!user.isActive()) {
            log.error("The user not active.");
            throw new UserDisabledException();
        }
    }

    public String resetUserToken(CoreUserEntity userEntity, boolean isNew) {
        log.debug("Reset user {} token.", userEntity.getId());
        String resetTokenStr = UuidUtil.newUuid();
        String resetToken = CredsUtil.hashBcrypt(resetTokenStr, -1);

        if (isNew) {
            resetToken = ConstantDef.NEW_TOKEN_MARK + resetToken;
        }

        userEntity.setResetToken(resetToken);
        userEntity.setResetTriggered(System.currentTimeMillis());

        return resetTokenStr;
    }

    public String getUserJoinUrl(int userId, String token) {
        StringBuffer joinUrl = new StringBuffer();
        joinUrl.append(getResetPasswordUrl(userId, token));
        joinUrl.append("#new");
        return joinUrl.toString();
    }

    public String getResetPasswordUrl(int userId, String token) {
        StringBuffer joinUrl = new StringBuffer();
        joinUrl.append("/auth/reset_password/");
        joinUrl.append(userId);
        joinUrl.append(ConstantDef.UNDERLINE);
        joinUrl.append(token);
        return joinUrl.toString();
    }

    public String encryptPassword(String salt, String password) {
        String saltPasswd = passwordSalt(salt, password);
        return CredsUtil.hashBcrypt(saltPasswd, -1);
    }

    public void setPassword(CoreUserEntity userEntity, String password) throws Exception {
        log.debug("update user {} password.", userEntity.getId());
        String salt = UuidUtil.newUuid();
        userEntity.setPasswordSalt(salt);
        String storePassword = encryptPassword(salt, password);
        userEntity.setPassword(storePassword);
        userEntity.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
    }

    /**
     * Add a salt value to the password
     * @param salt
     * @param password,
     * @return
     */
    private String passwordSalt(String salt, String password) {
        return salt + password;
    }

}
