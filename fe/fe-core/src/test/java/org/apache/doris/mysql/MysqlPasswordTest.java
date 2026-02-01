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

package org.apache.doris.mysql;

import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.qe.GlobalVariable;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class MysqlPasswordTest {

    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private String originalDictionaryFile;
    private String originalSecurityPluginsDir;

    @Before
    public void setUp() {
        // Save original values
        originalDictionaryFile = GlobalVariable.validatePasswordDictionaryFile;
        originalSecurityPluginsDir = Config.security_plugins_dir;
    }

    @After
    public void tearDown() {
        // Restore original values
        GlobalVariable.validatePasswordDictionaryFile = originalDictionaryFile;
        Config.security_plugins_dir = originalSecurityPluginsDir;
    }

    @Test
    public void testMakePassword() {
        Assert.assertEquals("*6C8989366EAF75BB670AD8EA7A7FC1176A95CEF4",
                new String(MysqlPassword.makeScrambledPassword("mypass")));

        Assert.assertEquals("", new String(MysqlPassword.makeScrambledPassword("")));

        // null
        Assert.assertEquals("", new String(MysqlPassword.makeScrambledPassword(null)));

        Assert.assertEquals("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32",
                new String(MysqlPassword.makeScrambledPassword("aBc@321")));

        Assert.assertEquals(new String(new byte[0]),
                new String(MysqlPassword.getSaltFromPassword(new byte[0])));

    }

    @Test
    public void testCheckPass() throws UnsupportedEncodingException {
        // client
        byte[] publicSeed = MysqlPassword.createRandomString(20);
        byte[] codePass = MysqlPassword.scramble(publicSeed, "mypass");

        Assert.assertTrue(MysqlPassword.checkScramble(codePass,
                publicSeed,
                MysqlPassword.getSaltFromPassword("*6C8989366EAF75BB670AD8EA7A7FC1176A95CEF4".getBytes("UTF-8"))));

        Assert.assertFalse(MysqlPassword.checkScramble(codePass,
                publicSeed,
                MysqlPassword.getSaltFromPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32".getBytes("UTF-8"))));
    }

    @Test
    public void testCheckPassword() throws AnalysisException {
        Assert.assertEquals("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32",
                new String(MysqlPassword.checkPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32")));

        Assert.assertEquals("", new String(MysqlPassword.checkPassword(null)));
    }

    @Test(expected = AnalysisException.class)
    public void testCheckPasswdFail() throws AnalysisException {
        MysqlPassword.checkPassword("*9A6EC1164108A8D3DA3BE3F35A56F6499B6FC32");
        Assert.fail("No exception throws");
    }

    @Test(expected = AnalysisException.class)
    public void testCheckPasswdFail2() throws AnalysisException {
        Assert.assertNotNull(MysqlPassword.checkPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC32"));
        MysqlPassword.checkPassword("*9A6EC51164108A8D3DA3BE3F35A56F6499B6FC3H");
        Assert.fail("No exception throws");
    }

    // ==================== validatePlainPassword Tests ====================

    @Test
    public void testValidatePasswordDisabledPolicy() throws AnalysisException {
        // When policy is DISABLED, any password should pass
        GlobalVariable.validatePasswordDictionaryFile = "";
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_DISABLED, "weak");
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_DISABLED, "");
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_DISABLED, null);
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_DISABLED, "test123");
    }

    @Test
    public void testValidatePasswordStrongPolicyValid() throws AnalysisException {
        // Valid password: 8+ chars, has digit, lowercase, uppercase, special char, no dictionary word
        GlobalVariable.validatePasswordDictionaryFile = "";
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Xk9$mN2@pL");
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "MyP@ss1!");
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Str0ng!Powd");
    }

    @Test
    public void testValidatePasswordTooShort() {
        GlobalVariable.validatePasswordDictionaryFile = "";
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Aa1!abc");
            Assert.fail("Expected AnalysisException for password too short");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("at least 8 characters"));
        }
    }

    @Test
    public void testValidatePasswordNullOrEmpty() {
        GlobalVariable.validatePasswordDictionaryFile = "";
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, null);
            Assert.fail("Expected AnalysisException for null password");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("at least 8 characters"));
        }

        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "");
            Assert.fail("Expected AnalysisException for empty password");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("at least 8 characters"));
        }
    }

    @Test
    public void testValidatePasswordMissingDigit() {
        GlobalVariable.validatePasswordDictionaryFile = "";
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Abcdefgh!");
            Assert.fail("Expected AnalysisException for missing digit");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Missing: numeric"));
        }
    }

    @Test
    public void testValidatePasswordMissingLowercase() {
        GlobalVariable.validatePasswordDictionaryFile = "";
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "ABCDEFG1!");
            Assert.fail("Expected AnalysisException for missing lowercase");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Missing: lowercase"));
        }
    }

    @Test
    public void testValidatePasswordMissingUppercase() {
        GlobalVariable.validatePasswordDictionaryFile = "";
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "abcdefg1!");
            Assert.fail("Expected AnalysisException for missing uppercase");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Missing: uppercase"));
        }
    }

    @Test
    public void testValidatePasswordMissingSpecialChar() {
        GlobalVariable.validatePasswordDictionaryFile = "";
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Abcdefg12");
            Assert.fail("Expected AnalysisException for missing special character");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("Missing: special character"));
        }
    }

    @Test
    public void testValidatePasswordMissingMultipleTypes() {
        GlobalVariable.validatePasswordDictionaryFile = "";
        try {
            // Missing digit, uppercase, special char
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "abcdefghij");
            Assert.fail("Expected AnalysisException for missing multiple types");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("numeric"));
            Assert.assertTrue(e.getMessage().contains("uppercase"));
            Assert.assertTrue(e.getMessage().contains("special character"));
        }
    }

    @Test
    public void testValidatePasswordBuiltinDictionaryWord() {
        GlobalVariable.validatePasswordDictionaryFile = "";
        // Test various built-in dictionary words
        String[] dictionaryPasswords = {
                "Test@123X",      // contains "test"
                "Admin@123X",     // contains "admin"
                "Password1!",     // contains "password"
                "Root@1234X",     // contains "root"
                "User@1234X",     // contains "user"
                "Doris@123X",     // contains "doris"
                "Qwerty@12X",     // contains "qwerty"
                "Welcome1!X",     // contains "welcome"
                "Hello@123X",     // contains "hello"
        };

        for (String password : dictionaryPasswords) {
            try {
                MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, password);
                Assert.fail("Expected AnalysisException for dictionary word in: " + password);
            } catch (AnalysisException e) {
                Assert.assertTrue("Expected dictionary word error for: " + password,
                        e.getMessage().contains("dictionary word"));
            }
        }
    }

    @Test
    public void testValidatePasswordDictionaryWordCaseInsensitive() {
        GlobalVariable.validatePasswordDictionaryFile = "";
        // Dictionary check should be case-insensitive
        String[] caseVariants = {
                "TEST@123Xy",
                "TeSt@123Xy",
                "tEsT@123Xy",
                "ADMIN@12Xy",
                "AdMiN@12Xy",
        };

        for (String password : caseVariants) {
            try {
                MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, password);
                Assert.fail("Expected AnalysisException for case-insensitive dictionary word in: " + password);
            } catch (AnalysisException e) {
                Assert.assertTrue(e.getMessage().contains("dictionary word"));
            }
        }
    }

    @Test
    public void testValidatePasswordWithExternalDictionary() throws IOException, AnalysisException {
        // Set security_plugins_dir to temp folder
        Config.security_plugins_dir = tempFolder.getRoot().getAbsolutePath();

        // Create a temporary dictionary file in the security_plugins_dir
        File dictFile = tempFolder.newFile("test_dictionary.txt");
        try (FileWriter writer = new FileWriter(dictFile)) {
            writer.write("# This is a comment\n");
            writer.write("customword\n");
            writer.write("  secretkey  \n");  // with spaces
            writer.write("\n");  // empty line
            writer.write("forbidden\n");
        }

        // Use just the filename (not full path)
        GlobalVariable.validatePasswordDictionaryFile = "test_dictionary.txt";

        // Password containing custom dictionary word should fail
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Customword1!");
            Assert.fail("Expected AnalysisException for custom dictionary word");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("customword"));
        }

        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Secretkey1!");
            Assert.fail("Expected AnalysisException for custom dictionary word");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("secretkey"));
        }

        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Forbidden1!");
            Assert.fail("Expected AnalysisException for custom dictionary word");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("forbidden"));
        }

        // Password not containing custom dictionary words should pass
        // Note: built-in words like "test" should NOT fail because we're using external dictionary
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Xk9$mN2@pL");
    }

    @Test
    public void testValidatePasswordDictionaryFileNotFound() throws AnalysisException {
        // Set security_plugins_dir to a valid path
        Config.security_plugins_dir = tempFolder.getRoot().getAbsolutePath();

        // When dictionary file doesn't exist, should fall back to built-in dictionary
        GlobalVariable.validatePasswordDictionaryFile = "non_existent_dictionary.txt";

        // Built-in dictionary word should still fail
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Test@123Xy");
            Assert.fail("Expected AnalysisException for built-in dictionary word");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("dictionary word"));
        }

        // Valid password should pass
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Xk9$mN2@pL");
    }

    @Test
    public void testValidatePasswordDictionaryFileReload() throws IOException, AnalysisException {
        // Set security_plugins_dir to temp folder
        Config.security_plugins_dir = tempFolder.getRoot().getAbsolutePath();

        // Create first dictionary file
        File dictFile1 = tempFolder.newFile("dict1.txt");
        try (FileWriter writer = new FileWriter(dictFile1)) {
            writer.write("wordone\n");
        }

        // Use just the filename
        GlobalVariable.validatePasswordDictionaryFile = "dict1.txt";

        // Should fail for wordone
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Wordone12!");
            Assert.fail("Expected AnalysisException for wordone");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("wordone"));
        }

        // Create second dictionary file with different content
        File dictFile2 = tempFolder.newFile("dict2.txt");
        try (FileWriter writer = new FileWriter(dictFile2)) {
            writer.write("wordtwo\n");
        }

        // Change to second dictionary file (just filename)
        GlobalVariable.validatePasswordDictionaryFile = "dict2.txt";

        // Should now pass for wordone (not in new dictionary)
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Wordone12!");

        // Should fail for wordtwo
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Wordtwo12!");
            Assert.fail("Expected AnalysisException for wordtwo");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("wordtwo"));
        }
    }

    @Test
    public void testValidatePasswordEmptyDictionaryFile() throws IOException, AnalysisException {
        // Set security_plugins_dir to temp folder
        Config.security_plugins_dir = tempFolder.getRoot().getAbsolutePath();

        // Use just the filename
        GlobalVariable.validatePasswordDictionaryFile = "empty_dict.txt";

        // With empty dictionary, only character requirements should be checked
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Test@123X");
            Assert.fail("Expected AnalysisException for test");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("test"));
        }
        try {
            MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Admin@12X");
            Assert.fail("Expected AnalysisException for admin");
        } catch (AnalysisException e) {
            Assert.assertTrue(e.getMessage().contains("admin"));
        }
    }

    @Test
    public void testValidatePasswordDictionaryWithCommentsOnly() throws IOException, AnalysisException {
        // Set security_plugins_dir to temp folder
        Config.security_plugins_dir = tempFolder.getRoot().getAbsolutePath();

        // Create a dictionary file with only comments
        File dictFile = tempFolder.newFile("comments_dict.txt");
        try (FileWriter writer = new FileWriter(dictFile)) {
            writer.write("# comment 1\n");
            writer.write("# comment 2\n");
            writer.write("   # comment with leading spaces\n");
        }

        // Use just the filename
        GlobalVariable.validatePasswordDictionaryFile = "comments_dict.txt";

        // Should pass since dictionary effectively has no words
        MysqlPassword.validatePlainPassword(GlobalVariable.VALIDATE_PASSWORD_POLICY_STRONG, "Test@123X");
    }
}
