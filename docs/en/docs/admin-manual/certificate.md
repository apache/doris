---
{
    "title": "TLS certificate",
    "language": "en"
}
---

<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements. See the NOTICE file
distributed with this work for additional information
regarding copyright ownership. The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License. You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied. See the License for the
specific language governing permissions and limitations
under the License.
-->

# Key Certificate Configuration

Doris needs a key certificate file to verify the SSL encrypted connection. The default key certificate file is located at `Doris/fe/mysql_ssl_default_certificate/certificate.p12`, and the default password is `doris`. You can modify the FE configuration file `conf/fe. conf`, add `mysql_ssl_default_certificate = /path/to/your/certificate` to modify the key certificate file, and you can also add the password corresponding to your custom key book file through `mysql_ssl_default_certificate_password = your_password`.

## Custom key certificate file

In addition to the Doris default certificate file, you can also generate a custom certificate file through `openssl`. Proceed as follows:

1. Run the following OpenSSL command to generate your private key and public certificate. Answer the questions and enter the Common Name when prompted.
```bash
openssl req -newkey rsa:2048 -nodes -keyout key.pem -x509 -days 365 -out certificate.pem
```

2. Review the created certificate.
```bash
openssl x509 -text -noout -in certificate.pem
```

3. Combine your key and certificate in a PKCS#12 (P12) bundle.
```bash
openssl pkcs12 -inkey key.pem -in certificate.pem -export -out certificate.p12
```

4. Validate your P2 file.
```bash
openssl pkcs12 -in certificate.p12 -noout -info
```

After completing these operations, you can get the certificate.p12 file.

>[reference documents](https://www.ibm.com/docs/en/api-connect/2018.x?topic=overview-generating-self-signed-certificate-using-openssl)
