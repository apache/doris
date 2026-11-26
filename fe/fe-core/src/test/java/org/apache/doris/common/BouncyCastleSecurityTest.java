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

package org.apache.doris.common;

import org.bouncycastle.asn1.DEROctetString;
import org.bouncycastle.asn1.DERSet;
import org.bouncycastle.asn1.cms.CMSObjectIdentifiers;
import org.bouncycastle.asn1.cms.ContentInfo;
import org.bouncycastle.asn1.cms.SignedData;
import org.bouncycastle.asn1.x509.GeneralName;
import org.bouncycastle.asn1.x509.GeneralSubtree;
import org.bouncycastle.cms.CMSException;
import org.bouncycastle.cms.CMSProcessableByteArray;
import org.bouncycastle.cms.CMSSignedData;
import org.bouncycastle.cms.CMSSignedDataGenerator;
import org.bouncycastle.cms.jcajce.JcaSignerInfoGeneratorBuilder;
import org.bouncycastle.cms.jcajce.JcaSimpleSignerInfoVerifierBuilder;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.jce.provider.PKIXNameConstraintValidator;
import org.bouncycastle.jce.provider.PKIXNameConstraintValidatorException;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.bouncycastle.operator.jcajce.JcaDigestCalculatorProviderBuilder;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Provider;
import java.security.Signature;

class BouncyCastleSecurityTest {
    @ParameterizedTest
    @CsvSource({
            "1,user@example.com.,true",
            "6,https://example.com./,true",
            "1,user@example.com,true",
            "6,https://example.com/,true",
            "1,user@safe.example,false",
            "6,https://safe.example/,false"
    })
    void enforcesExcludedHostsWithAndWithoutTrailingDots(int tag, String candidate, boolean excluded) {
        PKIXNameConstraintValidator validator = new PKIXNameConstraintValidator();
        validator.addExcludedSubtree(new GeneralSubtree(new GeneralName(tag, "example.com")));
        GeneralName name = new GeneralName(tag, candidate);
        // A DNS root-label dot must not let an excluded certificate identity escape its constraint.
        if (excluded) {
            Assertions.assertThrows(PKIXNameConstraintValidatorException.class, () -> validator.checkExcluded(name));
        } else {
            Assertions.assertDoesNotThrow(() -> validator.checkExcluded(name));
        }
    }

    @Test
    void rejectsCmsWithNoSigners() throws Exception {
        SignedData unsigned = new SignedData(new DERSet(),
                new ContentInfo(CMSObjectIdentifiers.data, new DEROctetString(new byte[] {1, 2, 3})),
                null, null, new DERSet());
        CMSSignedData data = new CMSSignedData(new ContentInfo(CMSObjectIdentifiers.signedData, unsigned));
        // An empty signer set must not turn signature verification into a vacuous success.
        Assertions.assertThrows(CMSException.class, () -> data.verifySignatures(id -> {
            throw new AssertionError("An unsigned message has no signer to resolve");
        }));
    }

    @Test
    void verifiesSignedCmsAndRejectsTamperedContent() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keys = generator.generateKeyPair();
        CMSSignedDataGenerator generatorCms = new CMSSignedDataGenerator();
        generatorCms.addSignerInfoGenerator(new JcaSignerInfoGeneratorBuilder(
                new JcaDigestCalculatorProviderBuilder().build()).build(
                    new JcaContentSignerBuilder("SHA256withRSA").build(keys.getPrivate()), new byte[] {1, 2, 3}));
        CMSSignedData signed = generatorCms.generate(new CMSProcessableByteArray(new byte[] {4, 5, 6}), false);
        CMSSignedData valid = new CMSSignedData(new CMSProcessableByteArray(new byte[] {4, 5, 6}), signed.getEncoded());
        Assertions.assertTrue(valid.verifySignatures(
                id -> new JcaSimpleSignerInfoVerifierBuilder().build(keys.getPublic())));
        CMSSignedData tampered = new CMSSignedData(
                new CMSProcessableByteArray(new byte[] {4, 5, 7}), signed.getEncoded());
        Assertions.assertThrows(CMSException.class, () -> tampered.verifySignatures(
                id -> new JcaSimpleSignerInfoVerifierBuilder().build(keys.getPublic())));
    }

    @Test
    void initializesProviderAndPreservesRsaSignatureVerification() throws Exception {
        Provider provider = new BouncyCastleProvider();
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair keys = generator.generateKeyPair();
        Signature signature = Signature.getInstance("SHA256withRSA", provider);
        byte[] message = "fixture-signed-content".getBytes(StandardCharsets.UTF_8);
        signature.initSign(keys.getPrivate());
        signature.update(message);
        byte[] signed = signature.sign();
        signature.initVerify(keys.getPublic());
        signature.update(message);
        Assertions.assertTrue(signature.verify(signed));
        signature.initVerify(keys.getPublic());
        signature.update("fixture-tampered-content".getBytes(StandardCharsets.UTF_8));
        Assertions.assertFalse(signature.verify(signed));
    }
}
