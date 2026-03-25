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

package org.apache.doris.authentication.plugin.oidc;

import org.apache.doris.authentication.spi.AuthenticationPlugin;
import org.apache.doris.authentication.spi.AuthenticationPluginFactory;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.gen.RSAKeyGenerator;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;

import java.text.ParseException;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class TestOidcAuthenticationPluginFactory implements AuthenticationPluginFactory {

    private final RSAKey rsaKey;

    public TestOidcAuthenticationPluginFactory(RSAKey rsaKey) {
        this.rsaKey = rsaKey;
    }

    @Override
    public String name() {
        return OidcAuthenticationPlugin.PLUGIN_NAME;
    }

    @Override
    public AuthenticationPlugin create() {
        return new TestOidcAuthenticationPlugin(rsaKey);
    }

    public static RSAKey newRsaJwk(String keyId) throws JOSEException {
        return new RSAKeyGenerator(2048)
                .keyID(keyId)
                .generate();
    }

    public static String createSignedToken(
            RSAKey signingKey,
            String issuer,
            List<String> audience,
            String subject,
            String username,
            Object groups,
            Map<String, Object> extraClaims
    ) throws JOSEException, ParseException {
        JWTClaimsSet.Builder claimsBuilder = new JWTClaimsSet.Builder()
                .issuer(issuer)
                .audience(audience)
                .subject(subject)
                .claim("preferred_username", username)
                .issueTime(Date.from(Instant.now()))
                .notBeforeTime(Date.from(Instant.now().minusSeconds(30)))
                .expirationTime(Date.from(Instant.now().plusSeconds(300)));
        if (groups != null) {
            claimsBuilder.claim("groups", groups);
        }
        for (Map.Entry<String, Object> entry : extraClaims.entrySet()) {
            claimsBuilder.claim(entry.getKey(), entry.getValue());
        }

        SignedJWT signedJwt = new SignedJWT(
                new JWSHeader.Builder(JWSAlgorithm.RS256).keyID(signingKey.getKeyID()).build(),
                claimsBuilder.build()
        );
        signedJwt.sign(new RSASSASigner(signingKey.toPrivateKey()));
        return signedJwt.serialize();
    }

    private static final class TestOidcAuthenticationPlugin extends OidcAuthenticationPlugin {
        private final RSAKey rsaKey;

        private TestOidcAuthenticationPlugin(RSAKey rsaKey) {
            this.rsaKey = rsaKey;
        }

        @Override
        protected OidcTokenValidator createValidator(OidcIntegrationConfig config) {
            JWKSource<SecurityContext> jwkSource = new ImmutableJWKSet<>(new JWKSet(rsaKey.toPublicJWK()));
            return new OidcTokenValidator(config, jwkSource);
        }
    }
}
