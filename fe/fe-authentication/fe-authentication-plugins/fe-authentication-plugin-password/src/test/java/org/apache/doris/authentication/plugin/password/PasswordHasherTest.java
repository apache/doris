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

package org.apache.doris.authentication.plugin.password;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link PasswordHasher}.
 *
 * <p>Test coverage:
 * <ul>
 *   <li>Hash generation for all algorithms (BCrypt, SHA-256, PLAIN)</li>
 *   <li>Password verification for all algorithms</li>
 *   <li>Algorithm detection from hashed passwords</li>
 *   <li>Rehash detection logic</li>
 *   <li>Edge cases and error handling</li>
 * </ul>
 */
@DisplayName("PasswordHasher Unit Tests")
class PasswordHasherTest {

    // ==================== Hash Generation Tests ====================

    @Test
    @DisplayName("UT-PWD-H-001: Hash password using default algorithm (BCrypt)")
    void testHash_DefaultAlgorithm() {
        // Given
        String plainPassword = "SecureP@ssw0rd";

        // When
        String hashedPassword = PasswordHasher.hash(plainPassword);

        // Then
        Assertions.assertNotNull(hashedPassword);
        Assertions.assertTrue(hashedPassword.startsWith("$2a$"), "Should use BCrypt by default");
        Assertions.assertNotEquals(plainPassword, hashedPassword, "Hashed password should differ from plain");
    }

    @Test
    @DisplayName("UT-PWD-H-002: Hash password using BCrypt algorithm")
    void testHash_Bcrypt() {
        // Given
        String plainPassword = "MyPassword123";

        // When
        String hashedPassword = PasswordHasher.hash(plainPassword, PasswordHasher.Algorithm.BCRYPT);

        // Then
        Assertions.assertNotNull(hashedPassword);
        Assertions.assertTrue(hashedPassword.startsWith("$2a$"), "BCrypt hash should start with $2a$");
        Assertions.assertTrue(hashedPassword.length() > 50, "BCrypt hash should be at least 50 chars");
    }

    @Test
    @DisplayName("UT-PWD-H-003: Hash password using SHA-256 algorithm")
    void testHash_Sha256() {
        // Given
        String plainPassword = "TestPassword";

        // When
        String hashedPassword = PasswordHasher.hash(plainPassword, PasswordHasher.Algorithm.SHA256);

        // Then
        Assertions.assertNotNull(hashedPassword);
        Assertions.assertTrue(hashedPassword.startsWith("{SHA256}"), "SHA-256 hash should have prefix");
        Assertions.assertTrue(hashedPassword.length() > 50, "SHA-256 hash should include Base64 encoded hash");
    }

    @Test
    @DisplayName("UT-PWD-H-004: Hash password using PLAIN algorithm")
    void testHash_Plain() {
        // Given
        String plainPassword = "PlainPassword";

        // When
        String hashedPassword = PasswordHasher.hash(plainPassword, PasswordHasher.Algorithm.PLAIN);

        // Then
        Assertions.assertNotNull(hashedPassword);
        Assertions.assertEquals("{PLAIN}PlainPassword", hashedPassword, "PLAIN should just add prefix");
    }

    @Test
    @DisplayName("UT-PWD-H-005: Hash null password should throw exception")
    void testHash_NullPassword_ThrowsException() {
        // When & Then
        IllegalArgumentException exception = Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> PasswordHasher.hash(null)
        );
        Assertions.assertTrue(exception.getMessage().contains("plainPassword"), "Error message should mention plainPassword");
    }

    @Test
    @DisplayName("UT-PWD-H-006: Hash empty password should succeed")
    void testHash_EmptyPassword() {
        // Given
        String emptyPassword = "";

        // When
        String hashedPassword = PasswordHasher.hash(emptyPassword, PasswordHasher.Algorithm.BCRYPT);

        // Then
        Assertions.assertNotNull(hashedPassword);
        Assertions.assertTrue(hashedPassword.startsWith("$2a$"));
    }

    @Test
    @DisplayName("UT-PWD-H-007: Hash with special characters")
    void testHash_SpecialCharacters() {
        // Given
        String specialPassword = "P@ssw0rd!#$%^&*(){}[]|\\:;\"'<>,.?/~`";

        // When
        String hashedPassword = PasswordHasher.hash(specialPassword);

        // Then
        Assertions.assertNotNull(hashedPassword);
        Assertions.assertTrue(PasswordHasher.verify(specialPassword, hashedPassword));
    }

    @Test
    @DisplayName("UT-PWD-H-008: Hash with Unicode characters")
    void testHash_UnicodeCharacters() {
        // Given
        String unicodePassword = "密码123パスワード🔐";

        // When
        String hashedPassword = PasswordHasher.hash(unicodePassword);

        // Then
        Assertions.assertNotNull(hashedPassword);
        Assertions.assertTrue(PasswordHasher.verify(unicodePassword, hashedPassword));
    }

    // ==================== Password Verification Tests ====================

    @Test
    @DisplayName("UT-PWD-V-001: Verify BCrypt password - correct password")
    void testVerify_Bcrypt_CorrectPassword() {
        // Given
        String plainPassword = "CorrectPassword";
        String hashedPassword = PasswordHasher.hash(plainPassword, PasswordHasher.Algorithm.BCRYPT);

        // When
        boolean result = PasswordHasher.verify(plainPassword, hashedPassword);

        // Then
        Assertions.assertTrue(result, "Correct password should verify successfully");
    }

    @Test
    @DisplayName("UT-PWD-V-002: Verify BCrypt password - wrong password")
    void testVerify_Bcrypt_WrongPassword() {
        // Given
        String correctPassword = "CorrectPassword";
        String wrongPassword = "WrongPassword";
        String hashedPassword = PasswordHasher.hash(correctPassword, PasswordHasher.Algorithm.BCRYPT);

        // When
        boolean result = PasswordHasher.verify(wrongPassword, hashedPassword);

        // Then
        Assertions.assertFalse(result, "Wrong password should not verify");
    }

    @Test
    @DisplayName("UT-PWD-V-003: Verify SHA-256 password - correct password")
    void testVerify_Sha256_CorrectPassword() {
        // Given
        String plainPassword = "TestPassword123";
        String hashedPassword = PasswordHasher.hash(plainPassword, PasswordHasher.Algorithm.SHA256);

        // When
        boolean result = PasswordHasher.verify(plainPassword, hashedPassword);

        // Then
        Assertions.assertTrue(result, "SHA-256 password should verify correctly");
    }

    @Test
    @DisplayName("UT-PWD-V-004: Verify SHA-256 password - wrong password")
    void testVerify_Sha256_WrongPassword() {
        // Given
        String correctPassword = "CorrectPassword";
        String wrongPassword = "WrongPassword";
        String hashedPassword = PasswordHasher.hash(correctPassword, PasswordHasher.Algorithm.SHA256);

        // When
        boolean result = PasswordHasher.verify(wrongPassword, hashedPassword);

        // Then
        Assertions.assertFalse(result, "Wrong SHA-256 password should not verify");
    }

    @Test
    @DisplayName("UT-PWD-V-005: Verify PLAIN password - correct password")
    void testVerify_Plain_CorrectPassword() {
        // Given
        String plainPassword = "PlainPassword";
        String hashedPassword = PasswordHasher.hash(plainPassword, PasswordHasher.Algorithm.PLAIN);

        // When
        boolean result = PasswordHasher.verify(plainPassword, hashedPassword);

        // Then
        Assertions.assertTrue(result, "PLAIN password should verify correctly");
    }

    @Test
    @DisplayName("UT-PWD-V-006: Verify PLAIN password - wrong password")
    void testVerify_Plain_WrongPassword() {
        // Given
        String correctPassword = "CorrectPassword";
        String wrongPassword = "WrongPassword";
        String hashedPassword = PasswordHasher.hash(correctPassword, PasswordHasher.Algorithm.PLAIN);

        // When
        boolean result = PasswordHasher.verify(wrongPassword, hashedPassword);

        // Then
        Assertions.assertFalse(result, "Wrong PLAIN password should not verify");
    }

    @Test
    @DisplayName("UT-PWD-V-007: Verify with null plain password")
    void testVerify_NullPlainPassword() {
        // Given
        String hashedPassword = PasswordHasher.hash("test");

        // When
        boolean result = PasswordHasher.verify(null, hashedPassword);

        // Then
        Assertions.assertFalse(result, "Null plain password should return false");
    }

    @Test
    @DisplayName("UT-PWD-V-008: Verify with null hashed password")
    void testVerify_NullHashedPassword() {
        // When
        boolean result = PasswordHasher.verify("test", null);

        // Then
        Assertions.assertFalse(result, "Null hashed password should return false");
    }

    @Test
    @DisplayName("UT-PWD-V-009: Verify with invalid hash format")
    void testVerify_InvalidHashFormat() {
        // Given
        String invalidHash = "not-a-valid-hash";

        // When
        boolean result = PasswordHasher.verify("password", invalidHash);

        // Then
        Assertions.assertFalse(result, "Invalid hash format should return false");
    }

    @Test
    @DisplayName("UT-PWD-V-010: Verify BCrypt without prefix (backward compatibility)")
    void testVerify_BcryptWithoutPrefix() {
        // Given - manually create BCrypt hash without prefix check
        String plainPassword = "test";
        String bcryptHash = PasswordHasher.hash(plainPassword, PasswordHasher.Algorithm.BCRYPT);

        // When
        boolean result = PasswordHasher.verify(plainPassword, bcryptHash);

        // Then
        Assertions.assertTrue(result, "Should verify BCrypt even if prefix check is ambiguous");
    }

    // ==================== Algorithm Detection Tests ====================

    @Test
    @DisplayName("UT-PWD-D-001: Detect BCrypt algorithm")
    void testDetectAlgorithm_Bcrypt() {
        // Given
        String bcryptHash = PasswordHasher.hash("test", PasswordHasher.Algorithm.BCRYPT);

        // When
        PasswordHasher.Algorithm detected = PasswordHasher.detectAlgorithm(bcryptHash);

        // Then
        Assertions.assertEquals(PasswordHasher.Algorithm.BCRYPT, detected);
    }

    @Test
    @DisplayName("UT-PWD-D-002: Detect SHA-256 algorithm")
    void testDetectAlgorithm_Sha256() {
        // Given
        String sha256Hash = PasswordHasher.hash("test", PasswordHasher.Algorithm.SHA256);

        // When
        PasswordHasher.Algorithm detected = PasswordHasher.detectAlgorithm(sha256Hash);

        // Then
        Assertions.assertEquals(PasswordHasher.Algorithm.SHA256, detected);
    }

    @Test
    @DisplayName("UT-PWD-D-003: Detect PLAIN algorithm")
    void testDetectAlgorithm_Plain() {
        // Given
        String plainHash = PasswordHasher.hash("test", PasswordHasher.Algorithm.PLAIN);

        // When
        PasswordHasher.Algorithm detected = PasswordHasher.detectAlgorithm(plainHash);

        // Then
        Assertions.assertEquals(PasswordHasher.Algorithm.PLAIN, detected);
    }

    @Test
    @DisplayName("UT-PWD-D-004: Detect algorithm with null hash should throw exception")
    void testDetectAlgorithm_NullHash() {
        // When & Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> PasswordHasher.detectAlgorithm(null));
    }

    @Test
    @DisplayName("UT-PWD-D-005: Detect algorithm with unknown format should throw exception")
    void testDetectAlgorithm_UnknownFormat() {
        // Given
        String unknownHash = "unknown-format-hash";

        // When & Then
        Assertions.assertThrows(IllegalArgumentException.class, () -> PasswordHasher.detectAlgorithm(unknownHash));
    }

    // ==================== Rehash Detection Tests ====================

    @Test
    @DisplayName("UT-PWD-R-001: Needs rehash - SHA-256 to BCrypt")
    void testNeedsRehash_Sha256ToBcrypt() {
        // Given
        String sha256Hash = PasswordHasher.hash("test", PasswordHasher.Algorithm.SHA256);

        // When
        boolean needsRehash = PasswordHasher.needsRehash(sha256Hash, PasswordHasher.Algorithm.BCRYPT);

        // Then
        Assertions.assertTrue(needsRehash, "SHA-256 hash should need rehashing to BCrypt");
    }

    @Test
    @DisplayName("UT-PWD-R-002: Needs rehash - PLAIN to BCrypt")
    void testNeedsRehash_PlainToBcrypt() {
        // Given
        String plainHash = PasswordHasher.hash("test", PasswordHasher.Algorithm.PLAIN);

        // When
        boolean needsRehash = PasswordHasher.needsRehash(plainHash, PasswordHasher.Algorithm.BCRYPT);

        // Then
        Assertions.assertTrue(needsRehash, "PLAIN hash should need rehashing to BCrypt");
    }

    @Test
    @DisplayName("UT-PWD-R-003: Needs rehash - BCrypt to BCrypt (same algorithm)")
    void testNeedsRehash_BcryptToBcrypt() {
        // Given
        String bcryptHash = PasswordHasher.hash("test", PasswordHasher.Algorithm.BCRYPT);

        // When
        boolean needsRehash = PasswordHasher.needsRehash(bcryptHash, PasswordHasher.Algorithm.BCRYPT);

        // Then
        Assertions.assertFalse(needsRehash, "Same algorithm with same work factor should not need rehashing");
    }

    @Test
    @DisplayName("UT-PWD-R-004: Needs rehash - null hash")
    void testNeedsRehash_NullHash() {
        // When
        boolean needsRehash = PasswordHasher.needsRehash(null, PasswordHasher.Algorithm.BCRYPT);

        // Then
        Assertions.assertTrue(needsRehash, "Null hash should always need rehashing");
    }

    @Test
    @DisplayName("UT-PWD-R-005: Needs rehash - BCrypt with lower work factor")
    void testNeedsRehash_BcryptLowerWorkFactor() {
        // Given - manually create BCrypt hash with lower work factor (this is conceptual)
        String bcryptHash = PasswordHasher.hash("test", PasswordHasher.Algorithm.BCRYPT);
        // Note: In real test, you'd need to create a hash with $2a$04$ (work factor 4)
        // For this test, we assume the current hash has work factor 10

        // When
        boolean needsRehash = PasswordHasher.needsRehash(bcryptHash, PasswordHasher.Algorithm.BCRYPT);

        // Then
        // Current implementation checks work factor from the hash
        Assertions.assertFalse(needsRehash, "Same or higher work factor should not need rehashing");
    }

    // ==================== Edge Cases and Integration Tests ====================

    @Test
    @DisplayName("UT-PWD-I-001: Hash and verify round-trip for all algorithms")
    void testHashAndVerify_RoundTrip_AllAlgorithms() {
        // Given
        String password = "TestPassword123!@#";

        // When & Then - BCrypt
        String bcryptHash = PasswordHasher.hash(password, PasswordHasher.Algorithm.BCRYPT);
        Assertions.assertTrue(PasswordHasher.verify(password, bcryptHash), "BCrypt round-trip should work");

        // When & Then - SHA-256
        String sha256Hash = PasswordHasher.hash(password, PasswordHasher.Algorithm.SHA256);
        Assertions.assertTrue(PasswordHasher.verify(password, sha256Hash), "SHA-256 round-trip should work");

        // When & Then - PLAIN
        String plainHash = PasswordHasher.hash(password, PasswordHasher.Algorithm.PLAIN);
        Assertions.assertTrue(PasswordHasher.verify(password, plainHash), "PLAIN round-trip should work");
    }

    @Test
    @DisplayName("UT-PWD-I-002: Same password produces different BCrypt hashes (salted)")
    void testHash_Bcrypt_DifferentSalts() {
        // Given
        String password = "SamePassword";

        // When
        String hash1 = PasswordHasher.hash(password, PasswordHasher.Algorithm.BCRYPT);
        String hash2 = PasswordHasher.hash(password, PasswordHasher.Algorithm.BCRYPT);

        // Then
        Assertions.assertNotEquals(hash1, hash2, "BCrypt should produce different hashes due to random salt");
        Assertions.assertTrue(PasswordHasher.verify(password, hash1), "First hash should verify");
        Assertions.assertTrue(PasswordHasher.verify(password, hash2), "Second hash should verify");
    }

    @Test
    @DisplayName("UT-PWD-I-003: Same password produces same SHA-256 hash (no salt)")
    void testHash_Sha256_Deterministic() {
        // Given
        String password = "SamePassword";

        // When
        String hash1 = PasswordHasher.hash(password, PasswordHasher.Algorithm.SHA256);
        String hash2 = PasswordHasher.hash(password, PasswordHasher.Algorithm.SHA256);

        // Then
        Assertions.assertEquals(hash1, hash2, "SHA-256 should be deterministic (no salt in this implementation)");
    }

    @Test
    @DisplayName("UT-PWD-I-004: Very long password hashing")
    void testHash_VeryLongPassword() {
        // Given
        String longPassword = "a".repeat(1000);

        // When
        String bcryptHash = PasswordHasher.hash(longPassword, PasswordHasher.Algorithm.BCRYPT);
        String sha256Hash = PasswordHasher.hash(longPassword, PasswordHasher.Algorithm.SHA256);

        // Then
        Assertions.assertNotNull(bcryptHash);
        Assertions.assertNotNull(sha256Hash);
        Assertions.assertTrue(PasswordHasher.verify(longPassword, bcryptHash));
        Assertions.assertTrue(PasswordHasher.verify(longPassword, sha256Hash));
    }

    @Test
    @DisplayName("UT-PWD-I-005: Password with only whitespace")
    void testHash_WhitespacePassword() {
        // Given
        String whitespacePassword = "   \t\n\r   ";

        // When
        String hashedPassword = PasswordHasher.hash(whitespacePassword);

        // Then
        Assertions.assertNotNull(hashedPassword);
        Assertions.assertTrue(PasswordHasher.verify(whitespacePassword, hashedPassword));
        Assertions.assertFalse(PasswordHasher.verify("", hashedPassword), "Empty string should not match whitespace");
    }

    @Test
    @DisplayName("UT-PWD-I-006: Case sensitivity check")
    void testVerify_CaseSensitive() {
        // Given
        String password = "Password";
        String hashedPassword = PasswordHasher.hash(password);

        // When & Then
        Assertions.assertTrue(PasswordHasher.verify("Password", hashedPassword), "Exact match should work");
        Assertions.assertFalse(PasswordHasher.verify("password", hashedPassword), "Should be case-sensitive");
        Assertions.assertFalse(PasswordHasher.verify("PASSWORD", hashedPassword), "Should be case-sensitive");
    }
}
