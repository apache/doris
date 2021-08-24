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

package org.apache.doris.stack.service.user;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.doris.stack.constant.ConstantDef;
import org.apache.doris.stack.model.ldap.LdapConnectionInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfo;
import org.apache.doris.stack.model.ldap.LdapUserInfoReq;
import org.apache.doris.stack.model.request.config.InitStudioReq;
import org.apache.doris.stack.model.request.config.IdaasSettingReq;
import org.apache.doris.stack.model.request.user.PasswordResetReq;
import org.apache.doris.stack.model.request.user.PasswordUpdateReq;
import org.apache.doris.stack.model.request.user.UserLoginReq;
import org.apache.doris.stack.model.response.config.IdaasResult;
import org.apache.doris.stack.model.response.config.IdaasSettingResp;
import org.apache.doris.stack.model.response.config.LdapSettingResp;
import org.apache.doris.stack.model.response.user.PasswordResetResp;
import org.apache.doris.stack.model.response.user.UserInfo;
import org.apache.doris.stack.util.CredsUtil;
import org.apache.doris.stack.constant.PropertyDefine;
import org.apache.doris.stack.util.UuidUtil;
import org.apache.doris.stack.component.IdaasComponent;
import org.apache.doris.stack.component.LdapComponent;
import org.apache.doris.stack.component.MailComponent;
import org.apache.doris.stack.component.SettingComponent;
import org.apache.doris.stack.component.UserActivityComponent;
import org.apache.doris.stack.connector.LdapClient;
import org.apache.doris.stack.dao.CoreSessionRepository;
import org.apache.doris.stack.dao.CoreUserRepository;
import org.apache.doris.stack.dao.LoginHistoryRepository;
import org.apache.doris.stack.dao.PermissionsGroupMembershipRepository;
import org.apache.doris.stack.dao.SuperUserRepository;
import org.apache.doris.stack.entity.CoreSessionEntity;
import org.apache.doris.stack.entity.CoreUserEntity;
import org.apache.doris.stack.entity.LoginHistoryEntity;
import org.apache.doris.stack.entity.PermissionsGroupMembershipEntity;
import org.apache.doris.stack.entity.SettingEntity;
import org.apache.doris.stack.entity.SuperUserEntity;
import org.apache.doris.stack.exception.AuthorizationException;
import org.apache.doris.stack.exception.BadRequestException;
import org.apache.doris.stack.exception.LdapConnectionException;
import org.apache.doris.stack.exception.NoAdminPermissionException;
import org.apache.doris.stack.exception.ResetPasswordTokenException;
import org.apache.doris.stack.exception.UserFailedLoginTooManyException;
import org.apache.doris.stack.exception.UserLoginException;
import org.apache.doris.stack.exception.UserLoginTooManyException;
import org.apache.doris.stack.service.BaseService;
import org.apache.doris.stack.service.config.ConfigConstant;
import org.apache.doris.stack.service.UtilService;
import com.unboundid.ldap.sdk.LDAPConnection;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.client.RestTemplate;

import nl.bitwalker.useragentutils.UserAgent;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Cookie;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

@Service
@Slf4j
public class AuthenticationService extends BaseService {
    // Define the user information of the super administrator and store it in the system setting information
    private static final String SUPER_USER_NAME_KEY = "super-user-name";
    public static final String SUPER_USER_NAME_VALUE = "Admin";

    // Password salt value of super administrator
    private static final String SUPER_USER_SLAT = "super-user-salt";

    private static final String SUPER_USER_PASSWORD_KEY = "super-user-password";
    private static final String SUPER_USER_PASSWORD_VALUE = "Admin@123";

    // Whether the super administrator is initialized is checked when the system is started for the first time.
    // If it is true, it indicates that the initialization is completed
    private static final String SUPER_USER_BE_ADD = "super-user-added";

    // Latest login time of super administrator
    private static final String SUPER_USER_LOGIN_TIME = "super-user-login-time";

    // Name of cookie
    private static final String COOKIE_NAME = "studio.SESSION";
    //
    private static final String SET_COOKIE = "Set-Cookie";

    //Token validity of reset password (hours)
    private static final int TOKEN_AGE = 48;

    // The failed login record is placed in memory and the application is restarted to empty
    public static ConcurrentHashMap<Integer, List<Long>> failedLoginMap = new ConcurrentHashMap<>();

    // There are too many failures. The next login time is allowed
    public static ConcurrentHashMap<Integer, Long> loginNotAllowMap = new ConcurrentHashMap<>();

    // Too many login times at the same time. The next login time is allowed
    public static ConcurrentHashMap<Integer, Long> loginNotAllowSessionMap = new ConcurrentHashMap<>();

    // access_token The validity period is 3600s by default.
    // There is an upper limit on the quantity. It is recommended to apply once every half an hour
    public static long lastLoginTime;

    // overall situation access_token
    public static String accessToken;

    // environment variable
    private static int loginDelayTime;
    private static int maxLoginFailedTimes;
    private static int maxLoginTimes;
    private static int maxLoginTimesInFiveMinutes;

    @Autowired
    private CoreUserRepository userRepository;

    @Autowired
    private CoreSessionRepository sessionRepository;

    @Autowired
    private SuperUserRepository superUserRepository;

    @Autowired
    private UtilService utilService;

    @Autowired
    private MailComponent mailComponent;

    @Autowired
    private Environment environment;

    @Autowired
    private SettingComponent settingComponent;

    @Autowired
    private LdapComponent ldapComponent;

    @Autowired
    private IdaasComponent idaasComponent;

    @Autowired
    private LdapClient ldapClient;

    @Autowired
    private PermissionsGroupMembershipRepository permissionsGroupMembership;

    @Autowired
    private UserActivityComponent activityComponent;

    @Autowired
    private LoginHistoryRepository loginHistoryRepository;

    /**
     * The super administrator is initialized when the service is started for the first time
     */
    @Transactional
    public void initSuperUser() {
        log.info("Init super user.");

        // Determine whether the super administrator already exists
        Optional<SuperUserEntity> superUserOldName = superUserRepository.findById(SUPER_USER_NAME_KEY);
        if (!superUserOldName.equals(Optional.empty())) {
            log.info("The super user already exists.");
            boolean passwordReset = Boolean.parseBoolean(environment.getProperty(PropertyDefine.SUPER_USER_PASS_RESET_PROPERTY));
            if (passwordReset) {
                log.info("Super user password rest.");
                String salt = UuidUtil.newUuid();
                SuperUserEntity superSalt = new SuperUserEntity(SUPER_USER_SLAT, salt);
                superUserRepository.save(superSalt);

                String passwd = utilService.encryptPassword(salt, SUPER_USER_PASSWORD_VALUE);
                SuperUserEntity superPassWord = new SuperUserEntity(SUPER_USER_PASSWORD_KEY, passwd);
                superUserRepository.save(superPassWord);
            }
        } else {
            // Initialize super administrator information
            SuperUserEntity superName = new SuperUserEntity(SUPER_USER_NAME_KEY, SUPER_USER_NAME_VALUE);
            superUserRepository.save(superName);

            String salt = UuidUtil.newUuid();
            SuperUserEntity superSalt = new SuperUserEntity(SUPER_USER_SLAT, salt);
            superUserRepository.save(superSalt);

            String passwd = utilService.encryptPassword(salt, SUPER_USER_PASSWORD_VALUE);
            SuperUserEntity superPassWord = new SuperUserEntity(SUPER_USER_PASSWORD_KEY, passwd);
            superUserRepository.save(superPassWord);

            log.info("Init new super user success.");
        }
        log.info("Init super user success.");
    }

    /**
     * get request Id address
     * @param request
     * @return
     */
    private  String getIpAdrress(HttpServletRequest request) {
        String realIP = request.getHeader("X-Real-IP");
        String xForIP = request.getHeader("X-Forwarded-For");
        if (StringUtils.isNotEmpty(xForIP) && !"unKnown".equalsIgnoreCase(xForIP)) {
            //After multiple reverse proxies, there will be multiple IP values,
            // and the first IP is the real IP ,X-Forwarded-For: client, proxy1, proxy2，proxy
            int index = xForIP.indexOf(",");
            if (index != -1) {
                return xForIP.substring(0, index);
            } else {
                return xForIP;
            }
        }
        xForIP = realIP;
        if (StringUtils.isNotEmpty(xForIP) && !"unKnown".equalsIgnoreCase(xForIP)) {
            return xForIP;
        }
        if (StringUtils.isBlank(xForIP) || "unknown".equalsIgnoreCase(xForIP)) {
            xForIP = request.getHeader("Proxy-Client-IP");
        }
        if (StringUtils.isBlank(xForIP) || "unknown".equalsIgnoreCase(xForIP)) {
            xForIP = request.getHeader("WL-Proxy-Client-IP");
        }
        if (StringUtils.isBlank(xForIP) || "unknown".equalsIgnoreCase(xForIP)) {
            xForIP = request.getHeader("HTTP_CLIENT_IP");
        }
        if (StringUtils.isBlank(xForIP) || "unknown".equalsIgnoreCase(xForIP)) {
            xForIP = request.getHeader("HTTP_X_FORWARDED_FOR");
        }
        // Finally, the IP address is obtained through request. Getremoteaddr()
        if (StringUtils.isBlank(xForIP) || "unknown".equalsIgnoreCase(xForIP)) {
            xForIP = request.getRemoteAddr();
        }
        return xForIP;
    }

    // Check the current number of failures for the user
    private void checkUserFailedTimes(int userId) throws Exception {
        log.debug("check failed login times for user {} .", userId);

        long timeHead = System.currentTimeMillis() - loginDelayTime;

        if (failedLoginMap.containsKey(userId)) {
            // Sort by timestamp in descending order
            List<Long> loginAttempts = failedLoginMap.get(userId);
            int index = 0;
            int size = loginAttempts.size();

            log.debug("user {} ,current time is {}, the failed login list size is {}.", userId,
                    new Timestamp(System.currentTimeMillis()), size);

            for (int i = size - 1; i >= 0; i--) {
                if (timeHead > loginAttempts.get(i)) {
                    index = i;
                    break;
                }
            }

            log.debug("user {} fail login times is {} in five minutes.", userId, size - index);

            // Clear the failed login record five minutes ago
            loginAttempts = loginAttempts.subList(index, size);
            failedLoginMap.put(userId, loginAttempts);
            // Maximum number of failures exceeded
            if (size - index >= maxLoginFailedTimes) {
                long currentTime = System.currentTimeMillis();
                loginNotAllowMap.put(userId, currentTime + loginDelayTime);
                log.debug("user {} login failed more than {} times, add to forbiden login map.", userId, maxLoginFailedTimes);
                throw new UserFailedLoginTooManyException();
            }
        }

    }

    // Verify whether the current time can be logged in and whether the password is correct
    private void checkLogin(String salt, String password, String passwdHash, int userId) throws Exception {
        log.debug("check whether user {} could login at current time.", userId);
        if (loginNotAllowSessionMap.containsKey(userId) && System.currentTimeMillis() < loginNotAllowSessionMap.get(userId)) {
            // Too many current accounts are online at the same time &
            // the current time is not up to the allowed login time
            throw new UserLoginTooManyException();
        }
        loginNotAllowSessionMap.remove(userId);
        if (!loginNotAllowMap.containsKey(userId) || System.currentTimeMillis() >= loginNotAllowMap.get(userId)) {
            // Cumulative failure times of verifying the current user in the last five minutes
            checkUserFailedTimes(userId);
            // Verify that the password entered is correct
            utilService.verifyLoginPassword(salt, password, passwdHash, userId);

        } else {
            // The current account exists & the account is delayed &
            // the current time is not up to the allowed login time
            throw new UserFailedLoginTooManyException();
        }

    }

    // If the login is successful, clear the failed login history and times
    private void cleanFailedLoginHistory(int userId) {
        log.debug("user {} login success, clean failed login record.", userId);
        loginNotAllowMap.remove(userId);
        failedLoginMap.remove(userId);
    }

    // Check the number of users online at the same time
    private void checkLoginCount(int userId) throws Exception {
        // Check the login times of the user within 5 minutes. If the threshold is exceeded,
        // login is prohibited. You can log in after 5 minutes
        Timestamp timeBefore = new Timestamp(System.currentTimeMillis() - 5 * 60 * 1000);
        int loginTimesBefore = sessionRepository.getSessionCountBeforeByUserId(userId, timeBefore);

        // Check the number of times the user has logged in. If the threshold is exceeded,
        // it is forbidden to log in. You can log in after five minutes
        int loginTimes = sessionRepository.getSessionCountByUserId(userId);
        log.debug("user id {} login count is {} in five minutes and total count is {}.", userId, loginTimesBefore,
                loginTimes);

        boolean flag = false;
        if (loginTimesBefore >= maxLoginTimesInFiveMinutes) {
            // Clean up the earliest batch of sessions of the user
            sessionRepository.deleteSessionBeforeByUserId(userId, timeBefore,
                    maxLoginTimesInFiveMinutes / 10);
            flag = true;
        }
        if (loginTimes >= maxLoginTimes) {
            // Clean up the earliest batch of sessions of the user
            sessionRepository.deleteSessionByUserId(userId, maxLoginTimes / 10);
            flag = true;

        }
        if (flag) {
            loginNotAllowSessionMap.put(userId, System.currentTimeMillis() + loginDelayTime);
            throw new UserLoginTooManyException();
        }
    }

    // Check whether to log in remotely: non first login & the first time for a new device
    private void checkIfLoginOtherPlace(int userId, String deviceId) {
        if (loginHistoryRepository.getLoginCountByUserId(userId) > 1
                && loginHistoryRepository.getLoginCountByUserIdAndDeviceId(userId, deviceId) == 0) {
            log.warn("user {} login at a different place.", userId);
        }
    }

    /**
     * Email password login / super administrator login
     *
     * @param loginReq
     * @return
     */
    @Transactional
    public String login(UserLoginReq loginReq, HttpServletRequest request) throws Exception {
        log.debug("user login.");
        loginDelayTime = Integer.parseInt(environment.getProperty(PropertyDefine.LOGIN_DELAY_TIME_PROPERTY));
        maxLoginTimesInFiveMinutes = Integer.parseInt(environment.getProperty(PropertyDefine.MAX_LOGIN_TIMES_IN_FIVE_MINUTES_PROPERTY));
        maxLoginTimes = Integer.parseInt(environment.getProperty(PropertyDefine.MAX_LOGIN_TIMES_PROPERTY));
        maxLoginFailedTimes = Integer.parseInt(environment.getProperty(PropertyDefine.MAX_LOGIN_FAILED_TIMES_PROPERTY));
        checkRequestBody(loginReq.hasEmptyField());

        // get service http address
        String sitUrl = request.getHeader("Origin");
        settingComponent.addNewSetting(ConfigConstant.SITE_URL_KEY, sitUrl);
        log.debug("The site url is {}.", sitUrl);

        String requestUserAgent = request.getHeader("User-Agent");
        log.debug("user agent is {}.", requestUserAgent);
        UserAgent userAgent = UserAgent.parseUserAgentString(requestUserAgent);
        // Get device ID
        String deviceId = userAgent.getId() == 0 ? "unknown" : String.valueOf(userAgent.getId());
        // Get browser information
        String description = StringUtils.isEmpty(userAgent.getBrowser().getName()) ? "unknown" : userAgent.getBrowser().getName();
        // Get the real IP address of the remote machine
        String ipAddress = StringUtils.isEmpty(getIpAdrress(request)) ? "unknown" : getIpAdrress(request);

        log.debug("remote request device id is {}, browser is {}, ip address is {}.", deviceId, description, ipAddress);

        String username = loginReq.getUsername();

        // user id
        int userId;

        if (username.equals(SUPER_USER_NAME_VALUE)) {
            log.debug("super admin user login.");
            userId = ConstantDef.SUPER_USER_ID;
            // Verify that the password entered is correct
            String salt = superUserRepository.findById(SUPER_USER_SLAT).get().getValue();
            String passwdHash = superUserRepository.findById(SUPER_USER_PASSWORD_KEY).get().getValue();
           // Check whether the login is allowed and the password is correct
            checkLogin(salt, loginReq.getPassword(), passwdHash, userId);
            // Verify the number of people online at the same time
            checkLoginCount(userId);
            // Verify remote login
            checkIfLoginOtherPlace(userId, deviceId);
            // Login successful, clear failure history
            cleanFailedLoginHistory(userId);

            // Add login time of super administrator user
            long currTime = System.currentTimeMillis();
            SuperUserEntity loginTime = new SuperUserEntity(SUPER_USER_LOGIN_TIME,
                    String.valueOf(currTime));
            superUserRepository.save(loginTime);
        } else {
            log.debug("user login by email and password.");

            List<CoreUserEntity> coreUserEntities = userRepository.getByEmailAndLdapAuth(username, ldapComponent.enabled());
            boolean notExisted = (coreUserEntities == null || coreUserEntities.size() != 1);

            List<CoreUserEntity> idaasCoreUserEntities = userRepository.getByEmailAndIdaasAuth(username,
                    idaasComponent.enabled());
            boolean idaasNotExisted = (idaasCoreUserEntities == null || idaasCoreUserEntities.size() != 1);

            CoreUserEntity user;

            if (ldapComponent.enabled()) {
                // If the user has enabled LDAP authentication, he can only log in through LDAP authentication
                if (notExisted) {
                    user = loginByLdap(loginReq);
                    // The first login does not have an ID, so you do not need to verify whether it is disabled
                    log.debug("The user {} is first login ldap user.", username);
                } else {
                    user = coreUserEntities.get(0);
                    loginByLdap(loginReq, user.getId());
                }
            } else if (idaasComponent.enabled()) {
                // If you have enabled idaas authentication, you can only log in through idaas authentication
                if (idaasNotExisted) {
                    user = loginByIdaas(loginReq);
                    // The first login does not have an ID, so you do not need to verify whether it is disabled
                    log.debug("The user {} is first login idaas user.", username);
                } else {
                    user = coreUserEntities.get(0);
                    loginByIdaas(loginReq, user.getId());
                }
            } else {
                // If it is the studio itself, it can only be authenticated through the studio itself
                if (notExisted) {
                    // If the user does not exist
                    log.error("The user {} not exist.", username);
                    throw new UserLoginException();
                }
                user = coreUserEntities.get(0);
                // Detect whether the user is disabled
                utilService.checkUserActive(user);
                checkLogin(user.getPasswordSalt(), loginReq.getPassword(), user.getPassword(), user.getId());

            }

            // Modify the latest login time
            user.setLastLogin(new Timestamp(System.currentTimeMillis()));
            userId = userRepository.save(user).getId();

            // Check the number of users online at the same time
            checkLoginCount(userId);

            // Check whether remote login
            checkIfLoginOtherPlace(userId, deviceId);

            // If the login is successful, clear the failed login history and times
            cleanFailedLoginHistory(userId);

            // If the LDAP user logs in for the first time and does not belong to any space
            if (notExisted || idaasNotExisted) {
                SettingEntity authType = settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY);
                log.debug("{} user {} first login studio, add user in default group.",
                        authType.getValue(), loginReq.getUsername());
                SettingEntity defaultGroup = settingComponent.readSetting(ConfigConstant.DEFAULT_GROUP_KEY);

                PermissionsGroupMembershipEntity permissionsGroupMembershipEntity = new PermissionsGroupMembershipEntity();
                permissionsGroupMembershipEntity.setGroupId(Integer.parseInt(defaultGroup.getValue()));
                permissionsGroupMembershipEntity.setUserId(userId);
                permissionsGroupMembership.save(permissionsGroupMembershipEntity);
            }
        }

        // Add session information
        log.debug("Create user {} login session.", userId);
        String sessionId = UuidUtil.newUuid();
        CoreSessionEntity sessionEntity = new CoreSessionEntity(sessionId, userId,
                new Timestamp(System.currentTimeMillis()), null);
        sessionRepository.save(sessionEntity);

        // Add login history
        LoginHistoryEntity loginHistoryEntity = new LoginHistoryEntity(new Timestamp(System.currentTimeMillis()),
                userId, sessionId, deviceId, description, ipAddress);
        loginHistoryRepository.save(loginHistoryEntity);

        log.debug("Add user {} joined or login activity.", userId);

        activityComponent.userLoginActivity(userId);

        return sessionId;
    }

    // Store LDAP user information
    private CoreUserEntity setCoreUser(LdapUserInfo ldapUserInfo) {
        CoreUserEntity user = new CoreUserEntity();
        user.setEmail(ldapUserInfo.getEmail());
        user.setFirstName(StringUtils.isEmpty(ldapUserInfo.getFirstName()) ? "" : ldapUserInfo.getFirstName());
        user.setLastName(StringUtils.isEmpty(ldapUserInfo.getLastName()) ? "" : ldapUserInfo.getLastName());
        user.setDateJoined(new Timestamp(System.currentTimeMillis()));
        user.setActive(true);
        user.setGoogleAuth(false);
        user.setLdapAuth(true);
        user.setQbnewb(false);
        return user;
    }

    // Store Idaas user information
    private CoreUserEntity setIdaasCoreUser(UserLoginReq loginReq) {
        CoreUserEntity user = new CoreUserEntity();
        // User name as mailbox
        user.setEmail(loginReq.getUsername());
        user.setFirstName(loginReq.getUsername().split("@")[0]);
        user.setLastName(loginReq.getUsername().split("@")[0]);
        user.setDateJoined(new Timestamp(System.currentTimeMillis()));
        user.setActive(true);
        user.setGoogleAuth(false);
        user.setLdapAuth(false);
        user.setIdaasAuth(true);
        user.setQbnewb(false);
        return user;
    }

    // LDAP login for the first time
    private CoreUserEntity loginByLdap(UserLoginReq loginReq) throws Exception {
        LdapUserInfo ldapUserInfo = ldapAuth(loginReq);
        if (null != ldapUserInfo && ldapUserInfo.getAuth()) {

            CoreUserEntity user = setCoreUser(ldapUserInfo);
            utilService.setPassword(user, loginReq.getPassword());
            return user;
        } else {
            log.error("Ldap user {} auth failed.", loginReq.getUsername());
            throw new UserLoginException();
        }
    }

    // get Idaas user info
    public boolean checkIdaasLogin(IdaasSettingReq idaasSettingReq, UserLoginReq loginReq) throws Exception {

        log.debug("check idaas login result.");
        // Apply for access token once every half an hour
        if (System.currentTimeMillis() > lastLoginTime + 1000 * 60 * 30) {
            log.debug("apply for new access token.");
            ResponseEntity<IdaasResult> responseEntity = idaasComponent.getAccessToken(idaasSettingReq);
            accessToken = responseEntity.getBody().getAccessToken();
        }
        // Login attempt time, regardless of whether the login is successful or not
        lastLoginTime = System.currentTimeMillis();
        String verifyURL = idaasSettingReq.getIdaasProjectURL() + "/v1/user/verify";
        log.debug("idaas access token is {}, verify url is {}.", accessToken, verifyURL);
        HttpHeaders headers = new HttpHeaders();

        JSONObject postData = new JSONObject();
        postData.put("name", loginReq.getUsername());
        postData.put("password", loginReq.getPassword());
        postData.put("targetId", idaasSettingReq.getIdaasAppId());
        postData.put("projectId", idaasSettingReq.getIdaasProjectId());

        // Set authentication string
        headers.setBearerAuth(accessToken);
        HttpEntity<Map<String, Object>> httpEntity = new HttpEntity<>(postData, headers);
        RestTemplate restTemplate = new RestTemplate();
        try {
            restTemplate.postForEntity(verifyURL, httpEntity, IdaasResult.class);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    // Idaas login for the first time
    private CoreUserEntity loginByIdaas(UserLoginReq loginReq) throws Exception {

        // get idaas configuration
        IdaasSettingResp resp = idaasComponent.readIdaasConfig();
        IdaasSettingReq req = new IdaasSettingReq();
        BeanUtils.copyProperties(resp, req);
        log.debug("idaas settings are {}.", req);
        if (checkIdaasLogin(req, loginReq)) {
            // store idaas user
            CoreUserEntity user = setIdaasCoreUser(loginReq);

            utilService.setPassword(user, loginReq.getPassword());
            return user;
        } else {
            log.error("Idaas user {} auth failed.", loginReq.getUsername());
            throw new UserLoginException();
        }
    }

    /**
     * Login through idaas. Only userid can be found if it is not the first login
     *
     * @param loginReq
     * @return
     * @throws Exception
     */
    private CoreUserEntity loginByIdaas(UserLoginReq loginReq, int userId) throws Exception {
        if (loginNotAllowSessionMap.containsKey(userId) && System.currentTimeMillis() < loginNotAllowSessionMap.get(userId)) {
            // Too many current accounts are online at the same time
            // the current time is not up to the allowed login time
            throw new UserLoginTooManyException();
        }
        loginNotAllowSessionMap.remove(userId);
        if (!loginNotAllowMap.containsKey(userId) || System.currentTimeMillis() >= loginNotAllowMap.get(userId)) {
            // Cumulative failure times of verifying the current user in the last five minutes
            checkUserFailedTimes(userId);
            // Verify that the password entered is correct
            IdaasSettingResp resp = idaasComponent.readIdaasConfig();
            IdaasSettingReq req = new IdaasSettingReq();
            BeanUtils.copyProperties(resp, req);
            log.debug("idaas settings are {}.", req);
            if (checkIdaasLogin(req, loginReq)) {
                CoreUserEntity user = setIdaasCoreUser(loginReq);
                utilService.setPassword(user, loginReq.getPassword());
                return user;
            } else {
                log.error("idaas user {} auth failed.", loginReq.getUsername());
                List<Long> loginAttempts = failedLoginMap.getOrDefault(userId, new ArrayList<>());
                loginAttempts.add(System.currentTimeMillis());
                failedLoginMap.put(userId, loginAttempts);
                throw new UserLoginException();
            }

        } else {
            // The current account exists
            // the account is delayed & the current time is not up to the allowed login time
            throw new UserFailedLoginTooManyException();
        }

    }

    /**
     * Login through LDAP. Userid is only available if it is not the first login
     *
     * @param loginReq
     * @return
     * @throws Exception
     */
    private CoreUserEntity loginByLdap(UserLoginReq loginReq, int userId) throws Exception {
        if (loginNotAllowSessionMap.containsKey(userId) && System.currentTimeMillis() < loginNotAllowSessionMap.get(userId)) {
            // Too many current accounts are online at the same time
            // the current time is not up to the allowed login time
            throw new UserLoginTooManyException();
        }
        loginNotAllowSessionMap.remove(userId);
        if (!loginNotAllowMap.containsKey(userId) || System.currentTimeMillis() >= loginNotAllowMap.get(userId)) {
            // Cumulative failure times of verifying the current user in the last five minutes
            checkUserFailedTimes(userId);
            // Verify that the password entered is correct
            LdapUserInfo ldapUserInfo = ldapAuth(loginReq);
            if (null != ldapUserInfo && ldapUserInfo.getAuth()) {
                // store LDAP user
                CoreUserEntity user = setCoreUser(ldapUserInfo);
                utilService.setPassword(user, loginReq.getPassword());
                return user;
            } else {
                log.error("Ldap user {} auth failed.", loginReq.getUsername());
                List<Long> loginAttempts = failedLoginMap.getOrDefault(userId, new ArrayList<>());
                loginAttempts.add(System.currentTimeMillis());
                failedLoginMap.put(userId, loginAttempts);
                throw new UserLoginException();
            }

        } else {
            // The current account exists / the account is delayed / the current time is not up to the allowed login time
            throw new UserFailedLoginTooManyException();
        }

    }

    /**
     * ldap user Authentication
     *
     * @param loginReq
     * @return
     */
    private LdapUserInfo ldapAuth(UserLoginReq loginReq) throws Exception {
        // get ldap configuration
        LdapSettingResp resp = ldapComponent.readLdapConfig();

        LdapConnectionInfo connectionInfo = new LdapConnectionInfo();
        connectionInfo.setHost(resp.getLdapHost());
        connectionInfo.setPort(resp.getLdapPort());
        connectionInfo.setBindDn(resp.getLdapBindDn());
        connectionInfo.setPassword(resp.getLdapPassword());

        // get ldap connection
        LDAPConnection ldapConnection = ldapClient.getConnection(connectionInfo);
        if (ldapConnection == null) {
            throw new LdapConnectionException();
        }

        LdapUserInfoReq userInfoReq = new LdapUserInfoReq();
        userInfoReq.setBaseDn(resp.getLdapUserBase());
        userInfoReq.setUserAttribute(resp.getLdapAttributeEmail());
        userInfoReq.setUserValue(loginReq.getUsername());
        userInfoReq.setPassword(loginReq.getPassword());

        // Authentication
        LdapUserInfo userInfo = ldapClient.authenticate(ldapConnection, userInfoReq);
        return userInfo;
    }

    /**
     * Log off the current account and delete the cookie
     *
     * @param request
     * @param response
     */
    @Transactional
    public void logout(HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("User logout.");
        String sessionId = getSessionIdByRequest(request);
        checkCoreSessionCookie(sessionId, response);
        log.debug("Delete core user session {}.", sessionId);
        sessionRepository.deleteById(sessionId);

        clearResponseCookie(response, sessionId);
    }

    /**
     * The user forgets the password and sends a password reset email to the mailbox
     *
     * @param email
     */
    @Transactional
    public void forgetPassword(String email, String hostname) throws Exception {
        log.debug("Send reset password email for user of forget password.");
        if (ldapComponent.enabled()) {
            log.error("Ldap user can't reset password.");
            throw new ResetPasswordTokenException();
        }

        JSONObject jsonObject = JSON.parseObject(email);
        email = jsonObject.getString("email");
        List<CoreUserEntity> userEntities = userRepository.getByEmail(email);

        if (userEntities == null || userEntities.size() != 1) {
            // The account does not exist, but nothing happened. You cannot return user prompt information
            log.error("The email is error.");
            throw new ResetPasswordTokenException();
        }
        CoreUserEntity user = userEntities.get(0);

        // Set password reset token
        String resetTokenStr = utilService.resetUserToken(user, false);
        user.setUpdatedAt(new Timestamp(System.currentTimeMillis()));
        userRepository.save(user);
        log.debug("update reset token to database.");

        String resetUrl = utilService.getResetPasswordUrl(user.getId(), resetTokenStr);

        mailComponent.sendRestPasswordMail(email, resetUrl, hostname);
    }

    /**
     * New invited users reset their passwords via email link
     * @param resetReq
     * @return
     * @throws Exception
     */
    @Transactional
    public PasswordResetResp resetPassword(PasswordResetReq resetReq) throws Exception {
        log.debug("Reset password by email");
        if (ldapComponent.enabled()) {
            log.error("Ldap user can't reset password.");
            throw new ResetPasswordTokenException();
        }
        checkRequestBody(resetReq.hasEmptyField());

        // Verify that the password format is correct
        utilService.passwordCheck(resetReq.getPassword());

        TokenInfo tokenInfo = getTokenInfoByStr(resetReq.getToken());

        try {
            int userId = tokenInfo.getUserId();

            CoreUserEntity userEntity = userRepository.findById(userId).get();

            // Check whether the user is disabled
            utilService.checkUserActive(userEntity);

            // Verify whether the token is valid
            if (!verifyToken(tokenInfo.getToken(), userEntity)) {
                log.error("Token expired, please resend mail.");
                throw new ResetPasswordTokenException();
            }

            // Set user password
            utilService.setPassword(userEntity, resetReq.getPassword());

            // The first time you log in, you need to send mail to all active admin users
            if (userEntity.getLastLogin() == null) {
                log.debug("The user logs in for the first time.");
                List<CoreUserEntity> adminActiveUser = userRepository.getActiveAdminUser(true, true);
                mailComponent.sendUserJoineNote(adminActiveUser, userEntity);

            }
            userEntity.setLastLogin(new Timestamp(System.currentTimeMillis()));

            log.debug("update use info.");
            userRepository.save(userEntity);

            // store session
            log.debug("Create session.");
            String sessionId = UuidUtil.newUuid();
            CoreSessionEntity sessionEntity = new CoreSessionEntity(sessionId, userId,
                    new Timestamp(System.currentTimeMillis()), null);
            sessionRepository.save(sessionEntity);

            PasswordResetResp resetResp = new PasswordResetResp(true, sessionId);
            return resetResp;
        } catch (NullPointerException e) {
            log.error("User not exist error {}, token authentication failed.", e.getMessage());
            throw new ResetPasswordTokenException();
        }

    }

    /**
     * Verify whether the token of the user's reset password is valid
     * @param token
     * @return
     * @throws Exception
     */
    public boolean resetTokenValid(String token) throws Exception {
        log.debug("check reset password token valid.");

        if (ldapComponent.enabled()) {
            log.error("Ldap user can't reset password.");
            throw new ResetPasswordTokenException();
        }

        TokenInfo tokenInfo = getTokenInfoByStr(token);

        try {
            int userId = tokenInfo.getUserId();
            CoreUserEntity userEntity = userRepository.findById(userId).get();

            utilService.checkUserActive(userEntity);
            return  verifyToken(tokenInfo.getToken(), userEntity);
        } catch (NullPointerException e) {
            log.error("User not exist error {}, token authentication failed.", e.getMessage());
            throw new ResetPasswordTokenException();
        }
    }

    /**
     * The interface that can be accessed by a space administrator user
     * requires whether the user is a space administrator
     *
     * @param userId
     * @return user id
     * @throws Exception
     */
    public void checkUserIsAdmin(int userId) throws Exception {
        try {
            CoreUserEntity userEntity = userRepository.findById(userId).get();
            if (!userEntity.isSuperuser()) {
                log.error("The user no have admin permission.");
                throw new NoAdminPermissionException();
            }
        } catch (Exception e) {
            log.error("Get User info by id exception {}.", e);
            throw e;
        }
    }

    /**
     * The interface accessed by ordinary users only needs user cookie authentication
     *
     * @param request
     * @param response
     * @return
     * @throws Exception
     */
    public int checkUserAuthWithCookie(HttpServletRequest request, HttpServletResponse response) throws Exception {
        log.debug("Check authentication information by cookie.");
        String sessionId = getSessionIdByRequest(request);
        int userId = checkCoreSessionCookie(sessionId, response);
        if (userId == ConstantDef.SUPER_USER_ID) {
            log.error("user cookie authentication failed");
            throw new AuthorizationException();
        }
        return userId;
    }

    /**
     * If you are a super administrator to access the interface,
     * you need to verify whether it is a cookie of the super administrator
     *
     * @param request
     * @param response
     * @return
     * @throws Exception
     */
    public void checkSuperAdminUserAuthWithCookie(HttpServletRequest request,
                                                  HttpServletResponse response) throws Exception {
        log.debug("Check superadmin authentication information by cookie.");

        String sessionId = getSessionIdByRequest(request);

        int userId = checkCoreSessionCookie(sessionId, response);

        if (userId != ConstantDef.SUPER_USER_ID) {
            log.error("Super admin cookie authentication failed");
            throw new AuthorizationException();
        }
    }

    /**
     * If you are both a super administrator user and an interface that ordinary users can access, check it uniformly
     * If it is a super administrator, it will return 0, and ordinary users will return ID
     *
     * @param request
     * @param response
     * @return
     * @throws Exception
     */
    public int checkAllUserAuthWithCookie(HttpServletRequest request,
                                          HttpServletResponse response) throws Exception {
        String sessionId = getSessionIdByRequest(request);
        return checkCoreSessionCookie(sessionId, response);
    }

    /**
     * get Super administrator user information
     *
     * @return
     */
    public UserInfo getSuperUserInfo() {
        UserInfo userInfo = new UserInfo();
        userInfo.setName(SUPER_USER_NAME_VALUE);
        SuperUserEntity loginTime = superUserRepository.findById(SUPER_USER_LOGIN_TIME).get();
        long time = Long.valueOf(loginTime.getValue());
        SettingEntity authType = settingComponent.readSetting(ConfigConstant.AUTH_TYPE_KEY);
        if (authType != null && !StringUtils.isEmpty(authType.getValue())) {
            userInfo.setAuthType(InitStudioReq.AuthType.valueOf(authType.getValue()));
        }

        userInfo.setLastLogin(new Timestamp(time));
        userInfo.setActive(true);
        userInfo.setSuperAdmin(true);
        userInfo.setId(ConstantDef.SUPER_USER_ID);
        return userInfo;
    }

    /**
     * Update the password of Super administrator
     */
    public UserInfo updateSuperUserPassword(PasswordUpdateReq updateReq) throws Exception {
        checkRequestBody(updateReq.hasEmptyField());
        log.debug("update super user password.");

        if (updateReq.checkPasswdSame()) {
            log.error("The new password is the same as the old one.");
            throw new BadRequestException("The new password is the same as the old one");
        }

        String salt = superUserRepository.findById(SUPER_USER_SLAT).get().getValue();
        String passwdHash = superUserRepository.findById(SUPER_USER_PASSWORD_KEY).get().getValue();
        utilService.verifyPassword(salt, updateReq.getOldPassword(), passwdHash);

        salt = UuidUtil.newUuid();
        SuperUserEntity superSalt = new SuperUserEntity(SUPER_USER_SLAT, salt);
        superUserRepository.save(superSalt);

        passwdHash = utilService.encryptPassword(salt, updateReq.getPassword());
        SuperUserEntity superPassword = new SuperUserEntity(SUPER_USER_PASSWORD_KEY, passwdHash);
        superUserRepository.save(superPassword);

        log.debug("Delete super user cookie.");
        sessionRepository.deleteByUserId(ConstantDef.SUPER_USER_ID);

        return getSuperUserInfo();
    }

    private int checkCoreSessionCookie(String sessionId, HttpServletResponse response) throws Exception {
        Optional<CoreSessionEntity> sessionEntityOp = sessionRepository.findById(sessionId);

        if (sessionEntityOp.equals(Optional.empty())) {
            log.error("The input cookie not exist.");
            throw new AuthorizationException();
        }

        CoreSessionEntity sessionEntity = sessionEntityOp.get();

        // Judge whether the cookie has expired, and delete it if it has expired
        long time = System.currentTimeMillis() - sessionEntity.getCreatedAt().getTime();
        long effectiveTime = getSessionAgeSecond() * 1000L;
        if (time > effectiveTime) {
            log.error("The cookie {} has been expired,delete it.", sessionId);
            sessionRepository.deleteById(sessionId);
            clearResponseCookie(response, sessionId);
            throw new AuthorizationException();
        }

        int userId = sessionEntity.getUserId();
        return userId;
    }

    /**
     *Delete expired sessions at 0:00 every day
     */
    @Scheduled(cron = "0 0 0 * * ?")
    @Transactional
    public void clearExpiredCookie() {
        try {
            log.info("Delete expire session.");
            long expireMillis = getSessionAgeSecond() * 1000L;
            Timestamp expireTime = new Timestamp(System.currentTimeMillis() - expireMillis);
            sessionRepository.deleteExpireSession(expireTime);
        } catch (Exception e) {
            log.error("clear expired cookie exception.");
            e.printStackTrace();
        }
    }

    /**
     * Set cookie for service response
     *
     * @param response
     * @param sessionId,cookie内容
     */
    public void setResponseCookie(HttpServletResponse response, String sessionId) {
        // If reading the configuration fails, set the browser cookie validity to session level
        int age = getSessionAgeSecond();
        log.debug("Set cookie for response, age is {} seconds.", age);
        Cookie repCookie = new Cookie(COOKIE_NAME, sessionId);
        repCookie.setMaxAge(age);
        repCookie.setPath("/");
        response.addCookie(repCookie);
    }

    /**
     * After the user logs off, clear the cookie
     *
     * @param response
     * @param sessionId
     */
    public void clearResponseCookie(HttpServletResponse response, String sessionId) {
        log.debug("Clear cookie for response.");
        Cookie repCookie = new Cookie(COOKIE_NAME, sessionId);
        // Setting the validity period to 0 is equivalent to clearing cookies
        repCookie.setMaxAge(0);
        response.addCookie(repCookie);
    }

    /**
     * get Request ID
     * @param request
     * @return
     */
    public String getRequestId(HttpServletRequest request) {
        return request.getHeader(ConstantDef.REQUEST_INDENTIFIER);
    }

    /**
     * Read session effective time according to configuration (seconds)
     *
     * @return
     */
    private int getSessionAgeSecond() {
        int age = 20160 * 60;
        try {
            age = Integer.valueOf(environment.getProperty(PropertyDefine.MAX_SESSION_AGE_PROPERTY)) * 60;
        } catch (Exception e) {
            log.error("Get session max age config error, use default config.");
            e.printStackTrace();
        }
        return age;
    }

    private String getSessionIdByRequest(HttpServletRequest request) throws Exception {
        log.debug("Get session id by request.");
        Cookie[] cookies = request.getCookies();
        String sessionId = null;
        if (cookies == null) {
            log.error("No cookie in request.");
            throw new AuthorizationException();
        }
        for (Cookie cookie : cookies) {
            if (cookie.getName() != null && cookie.getName().equals(COOKIE_NAME)) {
                sessionId = cookie.getValue();
                log.debug("get cookie value. {}: {}", cookie.getName(), sessionId);
            }
        }

        if (sessionId == null) {
            log.error("No cookie in request.");
            throw new AuthorizationException();
        }

        return sessionId;
    }

    private TokenInfo getTokenInfoByStr(String token) throws Exception {
        log.debug("Split reset token");
        String[] tokenInfo = token.split(ConstantDef.UNDERLINE);
        if (tokenInfo.length != 2) {
            log.error("Token format error.");
            throw new ResetPasswordTokenException();
        }

        try {
            int userId = Integer.valueOf(tokenInfo[0]);
            TokenInfo tokens = new TokenInfo(userId, tokenInfo[1]);
            return tokens;
        } catch (NumberFormatException e) {
            log.error("Token format error {} ,can't get userId", e.getMessage());
            throw new ResetPasswordTokenException();
        }
    }

    /**
     * Verify that the token for resetting the password is valid
     *
     * @param token
     * @param user
     * @return
     * @throws Exception
     */
    public boolean verifyToken(String token, CoreUserEntity user) throws Exception {
        log.debug("Verify password reset token {}.", token);
        String userResetToken = user.getResetToken();
        // not the first time to join studio,the token is invalid
        if (userResetToken.matches(ConstantDef.NEW_TOKEN_MARK + ".*")) {
            if (user.getLastLogin() != null) {
                return false;
            } else {
                userResetToken = userResetToken.substring(ConstantDef.NEW_TOKEN_MARK.length());
            }
        }

        boolean tokenCheck = CredsUtil.bcryptVerify(token, userResetToken);
        if (!tokenCheck) {
            log.error("Token authentication failed.");
            throw new ResetPasswordTokenException();
        }

        long expireMillis = TOKEN_AGE * 60 * 60 * 1000L;

        long age = System.currentTimeMillis() - user.getResetTriggered();

        if (age > expireMillis) {
            log.error("The reset token be expired.");
            return false;
        }

        return true;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class TokenInfo {
        int userId;

        String token;
    }
}
