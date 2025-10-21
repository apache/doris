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

#include "common/certificate_manager.h"

#include <gtest/gtest.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>
#include <thread>

namespace doris {

std::string normal_ca = R"(-----BEGIN CERTIFICATE-----
MIIDyTCCArGgAwIBAgIUbv4yzPMf2GwvKou+I3xGbAxVb68wDQYJKoZIhvcNAQEL
BQAwdDELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxDjAMBgNVBAoMBU15T3JnMR4wHAYDVQQLDBVDZXJ0aWZpY2F0ZSBBdXRo
b3JpdHkxETAPBgNVBAMMCE15Um9vdENBMB4XDTI1MTAxMTA2MjUwOFoXDTM1MTAw
OTA2MjUwOFowdDELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNV
BAcMB0JlaWppbmcxDjAMBgNVBAoMBU15T3JnMR4wHAYDVQQLDBVDZXJ0aWZpY2F0
ZSBBdXRob3JpdHkxETAPBgNVBAMMCE15Um9vdENBMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAx14IZYIic4IgrdytWq243XHrigQws11lU+oq+WIBN65O
WU72MB1SQgsmId+FbbK/lYj1/b2f91BOjjisjkPcPrsxuQjGFPJ7MVhRblNYNnCU
Qd/wvaQabfJEL/YfOzulJc2OsjUxvgAMJo6zBJZD6j9rQRT43Uanlef24GECD0wa
XSbAL2jhvzs6/o/AnpQYi5xZqAgAf56s0sb9xjdOCaPWWgB3Ly6NDPmwQ7Lzgwcn
dIedU5/z0pWo1+LN6lMBkvdbpBwxv4nVLbn0voM5jU+sUKlpyEX7Sd8MXaLzlYxA
yxVkJ0XdR4ehYmLkmZr1OX8z98N+6iTRDKJ2Muy65QIDAQABo1MwUTAdBgNVHQ4E
FgQUp3jT9umZ59M5r6vmVQOUqMNcSEcwHwYDVR0jBBgwFoAUp3jT9umZ59M5r6vm
VQOUqMNcSEcwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAmITP
uulp8/e/fKISz+l8FnvglEpiYXjOt/mggRodm14br40cYv2ZDc8pxREMXXp/NRjR
P2NjoyfDZ/TraMgAwv4vDj0T1roc/qL+h4VQCwpZfVFHkDa8kPJmo9IHHPn8ztD7
nA9LoKAsFbMmcPUwNqS1WyMOczHYb//51gnRUrjneU+CBm+tj7Km7DCTc2iFwMg1
XhzL/800upoIbikHKbfuFj6xpBaWugMb8WcKnShi447BAyAEfAochkQ0rzlcDDjy
+kPN8/fMR3QRpdXFouWzb8p+3JzCQtvBPpoMGAK9JC5L4+PB6Yxi8aDTXtQd2ICa
cAf9Y3wKUWrfyGyjAQ==
-----END CERTIFICATE-----)";

std::string normal_key = R"(-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDxENLeRIAOH4r/
tTrbCAzDpwUSMkXNq0xTH9lCSHZ9ii+bNiouKdlGaxnjwnvjtLXNs7Oqt9zav0qS
yYAnyrsDg0iqWsHQ2IkesMrgZwu/DYXHg8Gz6W1nzU9HmEB9bq4E7NqgAtXaQJbR
elOBeHqX0ACC9AoHdErK7IyakdtlmkuKgi0vx6BY0dupR5uIavxwXPQfa+XHGfDB
CkI23awuI2tmYnkQuDbcDno718bbB8Aj0ktQ3dfB3NHTqvjfSmAIVEdGLHkIBKdv
b/eIraWwaQs+vXdDcl/D3Lnr3gdNcmzRQjj47WpruNwi1x+tkEotjpJc66nU+jfM
ZW1/h2zfAgMBAAECggEAHN7/8kDP+WELu65P2zWumawiLli5BXXTkU0KLycQkd5/
7x3glWDLteRT2HRNdCsLbxRrmzGkEMrCOqxJXfqoxTXu+QAfoEyet39C/Pc6b+pF
sGx2QX7ebIUpTpDMgHlF/C8FzA4q4JXFulblM2djf1UZCpYBsXzbDEyiVkzLsvI5
P/3YgsVi7RkzOr1wxxfp2TvDx02jyfY8RQmBSzHn8tQaxlF8yo1zB1eFeYOYVcXp
ki51vKq4hSf6rsPVLUBrTkqPCX07Fm8M/7fFp3VHi81ftDftrADeLvlXas3KcTSr
zebQjpi5DfJWn20/kUo3IZpvEk/vIYwzst9YSfLfCQKBgQD9GaeEtjWg+urKwOor
7DPqvKSsg5lJO3XL9RUxzp/6hosXTgDXq2dmghPjvH/4rtrjgM726qGbERyACWzl
t59UiFbuejAFB2tfMe3DbIkF8LT+2ldKBPfuHpmn7rDNLLR+UtPNhmIUeZGHMaMq
b0FRfE6S0k5BXgijvcZT5hWW0wKBgQDz0989kskYXf8PeXuOURQsHasBONIWLp7i
k/8InzEZgu7vl7yf3IzwKJSz4gkb2dhopw5r8ZZABXAOxfP5Wm5k8QHWTNd7F0MY
/N82DzJjPoP022SVB3iDdXaGY65Hb2g3qiYQNUhz1SVCKcuWk0d9HiKW6zjfTNvQ
0U19joNiRQKBgQCk+3QXLi8HIIisYdRDjVTKTu2JBr+E3R2MNdX7AZWG2O0R4+bo
rvJX/7K3YMiKcnB8nBpNGeT+D8lkLMCvfWJ+1+DS0xM6M/vpscIrATTQindxKSJ9
PX/f2FKRBSZ6mAmPzq4B6vdEIXqbhd+2aY9Kbp1JV41rTsCS+8GsLu8jwQKBgErA
XeSGp9gsmRGpcNPEz6ZmwhJx1Rav3E4iiUGfbHIhzhbuMgngl+TYzB4J4jkDpHER
Jj65phKimCQvVAjSTJ3ttV552GYIT63NeLEeH5iFhfb/e+qki7HhxSCWVsvXv9+w
7lJxw3Cfm/iYz62uIXCeWIRkQN9UtN9kC5m+o1DxAoGBAO81ZFTtAbaJLRG6WaUE
PiPW0qinbSLcBd9vUV1Eik3pVEAAnV01ThqHb+pZD6gteFBmhH5vw3KTIe4H7Tze
LlOknYS9xV7DGPwuRI2JW8mmv1fVvqQklm5qvTmOs8LhTxTXZnmypiPzxTeJ7o1D
YCuUWm+v3E9/EbQALbyaSkNf
-----END PRIVATE KEY-----)";

std::string normal_cert = R"(-----BEGIN CERTIFICATE-----
MIIDjDCCAnSgAwIBAgIUJEdtjoGLIBE1M0UVLlXbmjxU3FMwDQYJKoZIhvcNAQEL
BQAwdDELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxDjAMBgNVBAoMBU15T3JnMR4wHAYDVQQLDBVDZXJ0aWZpY2F0ZSBBdXRo
b3JpdHkxETAPBgNVBAMMCE15Um9vdENBMB4XDTI1MTAxMTA2MjUxMFoXDTM1MTAw
OTA2MjUxMFowZjELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNV
BAcMB0JlaWppbmcxDjAMBgNVBAoMBU15T3JnMQ8wDQYDVQQLDAZTZXJ2ZXIxEjAQ
BgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
APEQ0t5EgA4fiv+1OtsIDMOnBRIyRc2rTFMf2UJIdn2KL5s2Ki4p2UZrGePCe+O0
tc2zs6q33Nq/SpLJgCfKuwODSKpawdDYiR6wyuBnC78NhceDwbPpbWfNT0eYQH1u
rgTs2qAC1dpAltF6U4F4epfQAIL0Cgd0SsrsjJqR22WaS4qCLS/HoFjR26lHm4hq
/HBc9B9r5ccZ8MEKQjbdrC4ja2ZieRC4NtwOejvXxtsHwCPSS1Dd18Hc0dOq+N9K
YAhUR0YseQgEp29v94itpbBpCz69d0NyX8PcueveB01ybNFCOPjtamu43CLXH62Q
Si2OklzrqdT6N8xlbX+HbN8CAwEAAaMkMCIwIAYDVR0RBBkwF4cEChAKB4cEfwAA
AYIJbG9jYWxob3N0MA0GCSqGSIb3DQEBCwUAA4IBAQCH3yfht34eZYWQYLxRLgv0
8dji3Q2qIxUlPCek8zb3PC+bB9jrUVmf+cEtcrl9H8QwKseDZKhojaaogaVlH8wP
ZuD00OCtYp7dfCtpJjeLAN11rPvyCiPW29gR+bCcMhjHx2F+nyqXgA9NbLA3XQlK
tcZgMUJ3fpPwPvoR0lmw/GP4EVc95UbvAGDlBWUnBQr4/3o+obDsiLCndC53znHD
xnBNXKd/Q4wBRKd0ctLCs5gLDnARLDI9KYEabMfrsq+KHVBDDx8o5jO/plSIm2f+
AdE4iy5dwYCJtNycJtK1FCwW0B4DEQH7Ak/zw1ZBSYt30n7oEVN9hVvAGVMG5Pc1
-----END CERTIFICATE-----)";

// password is 123456
std::string encrypted_key = R"(-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFLTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQITgr8KXdHtrcCAggA
MAwGCCqGSIb3DQIJBQAwHQYJYIZIAWUDBAEqBBAVP3/y4CchldZkLa44j+QBBIIE
0DzFCKKOArrOALwJvNA733WlDvVvOQ2p57oscTVwVWsQhL2mAhZC3uufqdU9lLMv
11JoNlcxUoB8+di/3uHEi0kTX+FT7BC6pKug0Q/wkUMwrrUaDEqWuEFITwlB1xGc
HNx1eULddnIYfhmWgBt6O0xstkDTTK7HrH+CbfZgRnFpRyAutKldjLxltLYslGRe
ppL84RxgjGXq3JOsncGnJFXrdjAv369m+vaBrgdqt7t+J96VPJtPfwoKhcgyEgct
afnc6jgcOq9ySvr+vDvBTBtblKNadvW0ICY20kEw4BzZXvkFk53qTZWTQaFFCOFw
BaPREuxCL5lCha1fdJa8jyJK+FXQccpULkqFrg90HzIet/QAPcm4TmXesS+198S5
7F1JeZmy8YyLVoHVMJvfHUFSgcGPWWKcbL2FlkoWEnYKyGWOZIea+JUXjJn6rOHu
GDswfT6A1fHhkZtZw9xZCdUk3x8FMHlz/vgh3ou6aZWT0G0eoQjaXKvHx//xWMYN
styBsBlV8hfOwfwlEazudIP79VLvlJOjZrBNCRExO0NgIfyvImd0txhEk7Cta7zN
qI+Zu9WGywgGmxfAdxbRxOMv0K5i7ZqCKE+OJ2VgDh7Zw6S3zua2iZCmrMocz13l
9pForJVrd6aImLa2Gz8L0+8xh2omFNJHXdmC9w4uYeo+Ogofp/4psdxAzELiZ1vT
IZNXRN3f5xZlY+5JVUWoHEZ/AC0HtulTd+s5Fi0mGjcTCDkcbTor5MlQqQJv30jW
dbdJcXeGiQiHv7dcxrqd9x/JCb8cpXnsClbKFaNtj9xjvSBCXhk/VUNPAwTJHWuP
GMKgmglrDFlAjfVJeBP8ZaJeEInp3rb2qUYYxvdISa4qn+ZUU6oqNWKDTBckVXcj
FbTx9QAxPsf70ZWQ/FP/8NlG8t9MVHdCN4LwKqvDTxkBUc8ob4TjbKsLGwBEYeVr
5mEIgg9/r5bgVRBcxOxaTA4gCUeOOg7b/YKr+4iUueTrH5D0VxWKpaji/P+eTyNF
K/hRMRdJ0ZudbgUNJMJoqdYtaHsT6c46KmgYGY2WC8dk4Pus/dtJkc+f8Ef3isEo
I1YDdxOq2h5pI/aybH0mCK20fAQxYfz82VKU8gncd0Rpb2WapZRnAthmzoVEZzHs
yIh0HImiS6bidfAbn0Sa85po1o6d2s4BwtKk15N9X5Z270gWE4NbveIliW15PYoY
jg2FydZVfuHpUmsx3uYqcaPs6DMx68ORh5PvxnLOttfW5lFEZC8MIuZ/3PyVz+Zx
/u7cVyEbGOu1Vasw1HkoB/d5o5YOWT3B1ANxV2mJmDcR1RdnUrybPXHezFO8VJsl
OBgJiOP7gmhhu/WlwGcadwRRnPAPAqgfIBPOeChJv1Uoxay89MOn5sYmqCQqVTKj
xNhve58/L+fyw5Bp/WmHkDFO25C3Pj4TBsGqhxh9sL1DC09y485iWGnPPM4b180/
qZ4eCe6HDayPJchA+isQKEag3a5g+OggIICV8HCrDX4AVCsq5mRFAP7R0HK/DFbT
qIXkqeILmp9bTSPW69ODtJrou9bCqta/Y556kXSMptWbQysv8Nh++o57DJEb7ZiD
m8ENR4MHq0aAPV1eI0Q4vyJoyEx3/JuGsLG5YPHYi9Kg
-----END ENCRYPTED PRIVATE KEY-----)";

// delete some data from normal_ca
std::string incomplete_ca = R"(-----BEGIN CERTIFICATE-----
MIIDyTCCArGgAwIBAgIUbv4yzPMf2GwvKou+I3xGbAxVb68wDQYJKoZIhvcNAQEL
BQAwdDELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxDjAMBgNVBAoMBU15T3JnMR4wHAYDVQQLDBVDZXJ0aWZpY2F0ZSBBdXRo
b3JpdHkxETAPBgNVBAMMCE15Um9vdENBMB4XDTI1MTAxMTA2MjUwOFoXDTM1MTAw
OTA2MjUwOFowdDELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNV
BAcMB0JlaWppbmcxDjAMBgNVBAoMBU15T3JnMR4wHAYDVQQLDBVDZXJ0aWZpY2F0
ZSBBdXRob3JpdHkxETAPBgNVBAMMCE15Um9vdENBMIIBIjANBgkqhkiG9w0BAQEF
AAOCAQ8AMIIBCgKCAQEAx14IZYIic4IgrdytWq243XHrigQws11lU+oq+WIBN65O
WU72MB1SQgsmId+FbbK/lYj1/b2f91BOjjisjkPcPrsxuQjGFPJ7MVhRblNYNnCU
Qd/wvaQabfJEL/YfOzulJc2OsjUxvgAMJo6zBJZD6j9rQRT43Uanlef24GECD0wa
XSbAL2jhvzs6/o/AnpQYi5xZqAgAf56s0sb9xjdOCaPWWgB3Ly6NDPmwQ7Lzgwcn
dIedU5/z0pWo1+LN6lMBkvdbpBwxv4nVLbn0voM5jU+sUKlpyEX7Sd8MXaLzlYxA
yxVkJ0XdR4ehYmLkmZr1OX8z98N+6iTRDKJ2Muy65QIDAQABo1MwUTAdBgNVHQ4E
FgQUp3jT9umZ59M5r6vmVQOUqMNcSEcwHwYDVR0jBBgwFoAUp3jT9umZ59M5r6vm
VQOUqMNcSEcwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAmITP)";

// delete some data from normal_key
std::string incomplete_key = R"(-----BEGIN PRIVATE KEY-----
MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQDxENLeRIAOH4r/
tTrbCAzDpwUSMkXNq0xTH9lCSHZ9ii+bNiouKdlGaxnjwnvjtLXNs7Oqt9zav0qS
yYAnyrsDg0iqWsHQ2IkesMrgZwu/DYXHg8Gz6W1nzU9HmEB9bq4E7NqgAtXaQJbR
elOBeHqX0ACC9AoHdErK7IyakdtlmkuKgi0vx6BY0dupR5uIavxwXPQfa+XHGfDB
CkI23awuI2tmYnkQuDbcDno718bbB8Aj0ktQ3dfB3NHTqvjfSmAIVEdGLHkIBKdv
b/eIraWwaQs+vXdDcl/D3Lnr3gdNcmzRQjj47WpruNwi1x+tkEotjpJc66nU+jfM
ZW1/h2zfAgMBAAECggEAHN7/8kDP+WELu65P2zWumawiLli5BXXTkU0KLycQkd5/
7x3glWDLteRT2HRNdCsLbxRrmzGkEMrCOqxJXfqoxTXu+QAfoEyet39C/Pc6b+pF
sGx2QX7ebIUpTpDMgHlF/C8FzA4q4JXFulblM2djf1UZCpYBsXzbDEyiVkzLsvI5
P/3YgsVi7RkzOr1wxxfp2TvDx02jyfY8RQmBSzHn8tQaxlF8yo1zB1eFeYOYVcXp
ki51vKq4hSf6rsPVLUBrTkqPCX07Fm8M/7fFp3VHi81ftDftrADeLvlXas3KcTSr
zebQjpi5DfJWn20/kUo3IZpvEk/vIYwzst9YSfLfCQKBgQD9GaeEtjWg+urKwOor
7DPqvKSsg5lJO3XL9RUxzp/6hosXTgDXq2dmghPjvH/4rtrjgM726qGbERyACWzl
t59UiFbuejAFB2tfMe3DbIkF8LT+2ldKBPfuHpmn7rDNLLR+UtPNhmIUeZGHMaMq
b0FRfE6S0k5BXgijvcZT5hWW0wKBgQDz0989kskYXf8PeXuOURQsHasBONIWLp7i
k/8InzEZgu7vl7yf3IzwKJSz4gkb2dhopw5r8ZZABXAOxfP5Wm5k8QHWTNd7F0MY)";

// delete some data from normal_cert
std::string incomplete_cert = R"(-----BEGIN CERTIFICATE-----
MIIDjDCCAnSgAwIBAgIUJEdtjoGLIBE1M0UVLlXbmjxU3FMwDQYJKoZIhvcNAQEL
BQAwdDELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNVBAcMB0Jl
aWppbmcxDjAMBgNVBAoMBU15T3JnMR4wHAYDVQQLDBVDZXJ0aWZpY2F0ZSBBdXRo
b3JpdHkxETAPBgNVBAMMCE15Um9vdENBMB4XDTI1MTAxMTA2MjUxMFoXDTM1MTAw
OTA2MjUxMFowZjELMAkGA1UEBhMCQ04xEDAOBgNVBAgMB0JlaWppbmcxEDAOBgNV
BAcMB0JlaWppbmcxDjAMBgNVBAoMBU15T3JnMQ8wDQYDVQQLDAZTZXJ2ZXIxEjAQ
BgNVBAMMCWxvY2FsaG9zdDCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEB
APEQ0t5EgA4fiv+1OtsIDMOnBRIyRc2rTFMf2UJIdn2KL5s2Ki4p2UZrGePCe+O0
tc2zs6q33Nq/SpLJgCfKuwODSKpawdDYiR6wyuBnC78NhceDwbPpbWfNT0eYQH1u
rgTs2qAC1dpAltF6U4F4epfQAIL0Cgd0SsrsjJqR22WaS4qCLS/HoFjR26lHm4hq
/HBc9B9r5ccZ8MEKQjbdrC4ja2ZieRC4NtwOejvXxtsHwCPSS1Dd18Hc0dOq+N9K
YAhUR0YseQgEp29v94itpbBpCz69d0NyX8PcueveB01ybNFCOPjtamu43CLXH62Q
Si2OklzrqdT6N8xlbX+HbN8CAwEAAaMkMCIwIAYDVR0RBBkwF4cEChAKB4cEfwAA
AYIJbG9jYWxob3N0MA0GCSqGSIb3DQEBCwUAA4IBAQCH3yfht34eZYWQYLxRLgv0
8dji3Q2qIxUlPCek8zb3PC+bB9jrUVmf+cEtcrl9H8QwKseDZKhojaaogaVlH8wP)";

// delete some data from encrypted_key
std::string incomplete_encrypted_key = R"(-----BEGIN ENCRYPTED PRIVATE KEY-----
MIIFLTBXBgkqhkiG9w0BBQ0wSjApBgkqhkiG9w0BBQwwHAQITgr8KXdHtrcCAggA
MAwGCCqGSIb3DQIJBQAwHQYJYIZIAWUDBAEqBBAVP3/y4CchldZkLa44j+QBBIIE
0DzFCKKOArrOALwJvNA733WlDvVvOQ2p57oscTVwVWsQhL2mAhZC3uufqdU9lLMv
11JoNlcxUoB8+di/3uHEi0kTX+FT7BC6pKug0Q/wkUMwrrUaDEqWuEFITwlB1xGc
HNx1eULddnIYfhmWgBt6O0xstkDTTK7HrH+CbfZgRnFpRyAutKldjLxltLYslGRe
ppL84RxgjGXq3JOsncGnJFXrdjAv369m+vaBrgdqt7t+J96VPJtPfwoKhcgyEgct
afnc6jgcOq9ySvr+vDvBTBtblKNadvW0ICY20kEw4BzZXvkFk53qTZWTQaFFCOFw
BaPREuxCL5lCha1fdJa8jyJK+FXQccpULkqFrg90HzIet/QAPcm4TmXesS+198S5
7F1JeZmy8YyLVoHVMJvfHUFSgcGPWWKcbL2FlkoWEnYKyGWOZIea+JUXjJn6rOHu
GDswfT6A1fHhkZtZw9xZCdUk3x8FMHlz/vgh3ou6aZWT0G0eoQjaXKvHx//xWMYN
styBsBlV8hfOwfwlEazudIP79VLvlJOjZrBNCRExO0NgIfyvImd0txhEk7Cta7zN
qI+Zu9WGywgGmxfAdxbRxOMv0K5i7ZqCKE+OJ2VgDh7Zw6S3zua2iZCmrMocz13l
9pForJVrd6aImLa2Gz8L0+8xh2omFNJHXdmC9w4uYeo+Ogofp/4psdxAzELiZ1vT
IZNXRN3f5xZlY+5JVUWoHEZ/AC0HtulTd+s5Fi0mGjcTCDkcbTor5MlQqQJv30jW
dbdJcXeGiQiHv7dcxrqd9x/JCb8cpXnsClbKFaNtj9xjvSBCXhk/VUNPAwTJHWuP
GMKgmglrDFlAjfVJeBP8ZaJeEInp3rb2qUYYxvdISa4qn+ZUU6oqNWKDTBckVXcj
FbTx9QAxPsf70ZWQ/FP/8NlG8t9MVHdCN4LwKqvDTxkBUc8ob4TjbKsLGwBEYeVr)";

namespace fs = std::filesystem;

// Helper to write content to a temporary file and return its path
fs::path write_temp_file(const fs::path& dir, const std::string& name, const std::string& content) {
    fs::path path = dir / name;
    std::ofstream ofs(path, std::ios::out | std::ios::trunc);
    ofs << content;
    return path;
}

class CertificateManagerTest : public ::testing::Test {
protected:
    void SetUp() override {
        auto timestamp = std::chrono::steady_clock::now().time_since_epoch().count();
        temp_dir = fs::temp_directory_path() /
                   fs::path("certificate_manager_ut_" + std::to_string(timestamp));
        fs::create_directories(temp_dir);
    }

    void TearDown() override {
        std::error_code ec;
        fs::remove_all(temp_dir, ec);
    }

    fs::path temp_dir;
};

// Verify loading of complete CA/cert/key files through CertificateManager APIs.
TEST_F(CertificateManagerTest, LoadValidCertificatesAndKeys) {
    auto ca_path = write_temp_file(temp_dir, "ca.pem", normal_ca);
    auto cert_path = write_temp_file(temp_dir, "cert.pem", normal_cert);
    auto key_path = write_temp_file(temp_dir, "key.pem", normal_key);

    std::unique_ptr<X509, decltype(&X509_free)> ca(CertificateManager::load_ca(ca_path), X509_free);
    ASSERT_NE(ca, nullptr);

    std::unique_ptr<X509, decltype(&X509_free)> cert(CertificateManager::load_cert(cert_path),
                                                     X509_free);
    ASSERT_NE(cert, nullptr);

    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
            CertificateManager::load_key(key_path, /*passwd=*/""), EVP_PKEY_free);
    ASSERT_NE(key, nullptr);

    std::string key_string = CertificateManager::load_key_string(key_path, /*passwd=*/"");
    ASSERT_FALSE(key_string.empty());
    ASSERT_NE(key_string.find("PRIVATE KEY"), std::string::npos);
}

// Verify that an encrypted private key can be decrypted with the provided password.
TEST_F(CertificateManagerTest, LoadEncryptedPrivateKey) {
    auto encrypted_path = write_temp_file(temp_dir, "encrypted_key.pem", encrypted_key);
    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
            CertificateManager::load_key(encrypted_path, "123456"), EVP_PKEY_free);
    ASSERT_NE(key, nullptr);

    std::string key_string = CertificateManager::load_key_string(encrypted_path, "123456");
    ASSERT_FALSE(key_string.empty());
    ASSERT_NE(key_string.find("PRIVATE KEY"), std::string::npos);
}

// Invalid/truncated files should result in nullptr without crashing.
TEST_F(CertificateManagerTest, InvalidFilesReturnNull) {
    auto ca_path = write_temp_file(temp_dir, "bad_ca.pem", incomplete_ca);
    auto cert_path = write_temp_file(temp_dir, "bad_cert.pem", incomplete_cert);
    auto key_path = write_temp_file(temp_dir, "bad_key.pem", incomplete_key);
    auto encrypted_path = write_temp_file(temp_dir, "bad_encrypted.pem", incomplete_encrypted_key);

    std::unique_ptr<X509, decltype(&X509_free)> ca(CertificateManager::load_ca(ca_path), X509_free);
    EXPECT_EQ(ca, nullptr);

    std::unique_ptr<X509, decltype(&X509_free)> cert(CertificateManager::load_cert(cert_path),
                                                     X509_free);
    EXPECT_EQ(cert, nullptr);

    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> key(
            CertificateManager::load_key(key_path, /*passwd=*/""), EVP_PKEY_free);
    EXPECT_EQ(key, nullptr);

    std::unique_ptr<EVP_PKEY, decltype(&EVP_PKEY_free)> encrypted_key_res(
            CertificateManager::load_key(encrypted_path, "123456"), EVP_PKEY_free);
    EXPECT_EQ(encrypted_key_res, nullptr);
}

// Ensure watcher detects first load, ignores unchanged file, and triggers reload on later modification.
TEST_F(CertificateManagerTest, DetectsInitialAndSubsequentChanges) {
    const fs::path file_path = temp_dir / "cert.pem";
    {
        std::ofstream ofs(file_path);
        ofs << "initial";
        ofs.close();
    }

    CertificateManager::CertFileMonitorState state;
    auto no_stop = []() { return false; };

    ASSERT_TRUE(CertificateManager::check_certificate_file(file_path.string(), &state,
                                                           "certificate", no_stop));
    ASSERT_TRUE(state.has_value);

    EXPECT_FALSE(CertificateManager::check_certificate_file(file_path.string(), &state,
                                                            "certificate", no_stop));

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    {
        std::ofstream ofs(file_path, std::ios::app);
        ofs << "updated";
        ofs.close();
    }

    EXPECT_TRUE(CertificateManager::check_certificate_file(file_path.string(), &state,
                                                           "certificate", no_stop));
}

// Ensure watcher can detect recreation after deletion.
TEST_F(CertificateManagerTest, FileRecreatedAfterDeletion) {
    const fs::path file_path = temp_dir / "recreated.pem";
    CertificateManager::CertFileMonitorState state;
    auto no_stop = []() { return false; };

    // Initial detection should fail because file doesn't exist.
    EXPECT_FALSE(CertificateManager::check_certificate_file(file_path.string(), &state,
                                                            "certificate", no_stop));
    EXPECT_FALSE(state.has_value);

    // Create file, expect detection.
    {
        std::ofstream ofs(file_path);
        ofs << "content";
    }
    EXPECT_TRUE(CertificateManager::check_certificate_file(file_path.string(), &state,
                                                           "certificate", no_stop));
    EXPECT_TRUE(state.has_value);

    // Delete file and ensure state resets.
    fs::remove(file_path);
    EXPECT_FALSE(CertificateManager::check_certificate_file(file_path.string(), &state,
                                                            "certificate", no_stop));
    EXPECT_FALSE(state.has_value);

    // Recreate file and ensure detection happens again.
    {
        std::ofstream ofs(file_path);
        ofs << "new content";
    }
    EXPECT_TRUE(CertificateManager::check_certificate_file(file_path.string(), &state,
                                                           "certificate", no_stop));
    EXPECT_TRUE(state.has_value);
}

// Ensure missing file clears watcher state so subsequent recreations can be detected.
TEST_F(CertificateManagerTest, MissingFileDoesNotBlockAndResetsState) {
    const fs::path missing_path = temp_dir / "missing.pem";

    CertificateManager::CertFileMonitorState state;
    state.has_value = true;
    state.write_time = fs::file_time_type::clock::now();

    auto stop_immediately = []() { return false; };

    EXPECT_FALSE(CertificateManager::check_certificate_file(missing_path.string(), &state,
                                                            "certificate", stop_immediately));
    EXPECT_FALSE(state.has_value);
}

} // namespace doris
