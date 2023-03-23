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

package org.apache.doris.deploy;

import io.fabric8.kubernetes.client.Config;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import org.junit.Test;

import java.io.File;

public class K8sDeployManagerTest {
    /**
     * The purpose of this test case is to verify whether there is a problem with the referenced Jackson version
     * when initializing the DefaultKubernetesClient.
     * The prerequisite for executing this test case is that the system has a file named:~/.kube/config.
     * The file demo is as follows:
     *
     * apiVersion: v1
     * clusters:
     * - cluster:
     *     certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUN5RENDQWJDZ0F3SUJBZ0lCQURBTkJna3Foa2lHOXcwQkFRc0ZBREFWTVJNd0VRWURWUVFERXdwcmRXSmwKY201bGRHVnpNQjRYRFRJek1ESXdOakE1TkRFek5sb1hEVE16TURJd016QTVOREV6Tmxvd0ZURVRNQkVHQTFVRQpBeE1LYTNWaVpYSnVaWFJsY3pDQ0FTSXdEUVlKS29aSWh2Y05BUUVCQlFBRGdnRVBBRENDQVFvQ2dnRUJBTUpICjIwTzBKeFQweUZuVnJjcENRYlpsWUxNRFM5VWU5OHZVMFA5L1M3M3RjRU42TEVxbXRZUEtHRUNjcU5MQlFzUnMKYXZqUUhqSnVUb3N3QktycFJ3allJYUFvcmFmOWhkcW5ReTJINU5SbGQ2aXdJNmdLM0lIMVBmRzhUVGJzOVM5NApUcWlyVjVlVlJOSG5QYnNqT3F2ZEZTdnFUS2xYZGswSDFoVDZpdHpQSnAwbjZSZHlMcG1MM3JKZVpybzlBZjV0Cmlhd2gyMC9qUjE0VUovRE1COFhQTmxRbFpia2lSS3RLeHYwK0JhZklXU3ZwbS9KWFhvVy9ONGRvYm1raFFaMlMKb3A4eHZLZXN0TlV1RUNJY0hiSG1iTnVYRy9EWjZ3Qndwa3c3aGprRFR2Z2NYYTluSFBKS0FUVFRabStxK1NVUwppdDVsUURxZmdkNzF6dTNkcmVzQ0F3RUFBYU1qTUNFd0RnWURWUjBQQVFIL0JBUURBZ0tVTUE4R0ExVWRFd0VCCi93UUZNQU1CQWY4d0RRWUpLb1pJaHZjTkFRRUxCUUFEZ2dFQkFDVmlxOGhMYTB4aksrcXBoR01VcHVIMlZYOCsKOGJUb1NzUkJDQ2xrODhrNlN4a1IrZlV6eFJScW9TTnR1N2Qzd0lNcTJoUnphbG5JMGRPbUtINlpHOGhUL1ZoNgp4QUMzTEdQejZYZGxPM1BJNXc5M1V3UFlQeWhEdnNNWWZWMUlVV1I3eEdUSSs2ZTI1YXYyOUVuc0ZrLzM4UWphCkJvQ2l5WTU3WUtsRUNOZFJHN1lWTGtNYU92dWp5YkRZTjRxWThqWkN5TitrY0ZJakFLbFYveHd4NitnM3ZCNDAKQ0RXcm51VmlvZU05dWwxTkIyK2ZhSjlLanMwNm9CZFM3b0ZnWGRmUXVwRGdsbUkwbFI2RGxOWGtVUDhYbmJVUwpSYWRpcnkrVVRkYmpvQlRkblMzaE14THFheHNvanU4VkdYMGdZT3R4VWpKMUpsaWZqVzgyQitjQWluaz0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
     *     server: https://172.22.0.16
     *   name: cls-m5vzryyb
     * contexts:
     * - context:
     *     cluster: cls-m5vzryyb
     *     user: "100029465455"
     *   name: cls-m5vzryyb-100029465455-context-default
     * current-context: cls-m5vzryyb-100029465455-context-default
     * kind: Config
     * preferences: {}
     * users:
     * - name: "100029465455"
     *   user:
     *     client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURERENDQWZTZ0F3SUJBZ0lJRGR2VTcyT250TWt3RFFZSktvWklodmNOQVFFTEJRQXdGVEVUTUJFR0ExVUUKQXhNS2EzVmlaWEp1WlhSbGN6QWVGdzB5TXpBeU1EWXhNREExTURKYUZ3MDBNekF5TURZeE1EQTFNREphTURZeApFakFRQmdOVkJBb1RDWFJyWlRwMWMyVnljekVnTUI0R0ExVUVBeE1YTVRBd01ESTVORFkxTkRVMUxURTJOelUyCk56YzVNREl3Z2dFaU1BMEdDU3FHU0liM0RRRUJBUVVBQTRJQkR3QXdnZ0VLQW9JQkFRQ2o3OGtjMWdQb2h2VlEKdzVFZDBoa3ZyZzVPYllkWDZPTjIvdGlVRWtjbEpNcFA4NFNIeUhiSm1jRjJtM0huRTRuWWpEcXE5a0FBUlB3MwpQVDhQOXZsSUxuQ1BucUhZcE9lY2hoWGFjejVYRDBqaXNUeE5ETnV2NjVUcnRpa294VVA5MExGbkoycGVNclNkCjRHWGRpbDN0Rk1qdVplRjI1NzFwb2EydUxKZHhsN0UxUEoxOXhGRi9NRVhKV0prS0dSRjNWSjVtRmJsVWlpUEoKMHVHdWVEUWoxelF2ekRHejNZcnJQaUdxZkVFbktDaEExMkVTS2N2N2tsL1ZGNS9Lbzh2dVBLOUxZUUFXbXpIYgpqMjJpSENWUUFSc1BuRGtOaDdzRzVtSXUvbDdLWndHajMwZWZNcER1TTlsUDBPMW9aTU05ZnRGY3JXQ0cxS0pYCnA1R3Rha1poQWdNQkFBR2pQekE5TUE0R0ExVWREd0VCL3dRRUF3SUNoREFkQmdOVkhTVUVGakFVQmdnckJnRUYKQlFjREFnWUlLd1lCQlFVSEF3RXdEQVlEVlIwVEFRSC9CQUl3QURBTkJna3Foa2lHOXcwQkFRc0ZBQU9DQVFFQQpmM3FPQ2hmWEY2d05qNUo5bXBvNXhQVVdPZ0JneWhQbWJGQVJaWnNDNUdEQ3FWcm03RG5rUFR3Q3BFS1pkZThyCmFXblRRMTVqWHBoem5MNVFUeXVIQVNnSEN2TTNKR0pOeUtidTVjWjFmVnRSa3JLbjFMMWM0U3Q2Y09RL3JGVHUKZjY2UkFSVXQ4b1F0ZjBZbjRlZHlhQllDV3R0K2h0SXhvK0tpbzV2QjY4UVZPL2hmYVl1b3NMOWZlR21PYnd1cApqZHVFd1hGY3k3RlNlM0Q2dWdUZWpSMFo1bVFtcWFRZ25MQXg5akFxc1FJbUdkUC9ER0xhclYyUmhJb0tReC9hCnFVWTdZdGtISmtodG1RMXhpREhkSHBIa1ZoV1NHUitqbVlOckIwWm14emZ6ZTZURmthL3ViSE91cW9rM2IvL3oKTlhCMGF1U3ZMa1FmWm5oUDNyejUxZz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
     *     client-key-data: LS0tLS1CRUdJTiBSU0EgUFJJVkFURSBLRVktLS0tLQpNSUlFcEFJQkFBS0NBUUVBbysvSkhOWUQ2SWIxVU1PUkhkSVpMNjRPVG0ySFYrampkdjdZbEJKSEpTVEtUL09FCmg4aDJ5Wm5CZHB0eDV4T0oySXc2cXZaQUFFVDhOejAvRC9iNVNDNXdqNTZoMktUbm5JWVYybk0rVnc5STRyRTgKVFF6YnIrdVU2N1lwS01WRC9kQ3haeWRxWGpLMG5lQmwzWXBkN1JUSTdtWGhkdWU5YWFHdHJpeVhjWmV4TlR5ZApmY1JSZnpCRnlWaVpDaGtSZDFTZVpoVzVWSW9qeWRMaHJuZzBJOWMwTDh3eHM5Mks2ejRocW54Qkp5Z29RTmRoCkVpbkwrNUpmMVJlZnlxUEw3anl2UzJFQUZwc3gyNDl0b2h3bFVBRWJENXc1RFllN0J1WmlMdjVleW1jQm85OUgKbnpLUTdqUFpUOUR0YUdURFBYN1JYSzFnaHRTaVY2ZVJyV3BHWVFJREFRQUJBb0lCQVFDVVhEY1hmNTk5MmxVOApTQ0NXYWtNYzRWcFZJODV1Z25jSWc4NGhBd0diM2RXeDBXSkpOK1E0d290UStaQjFYVHRNM0d0anVRTm11d2UrClBoRktNMnFhSmx6N3ZNWWZ0KzFidkFVZTgxaWhsVldTd204ZUU5cmxORUJMcVVsS2VtdnowZUFheUpMOHVNcTQKYmhJQ0VteTZXQ3NYaE9Bc3FjRERUZXhPU2xDc0hQY0w3bkdDdDE4MnhnTm1ndHZoYmhxSUJaSS83SjhiMnhNTwpGajVtRUw3Y25wUVRucDNjSTFaby9oVllJVHN4a2wrdFJ3TzdNOWE0Q2RQeW9CME1SNUNxemNrNlk5aVlOVDFTCkhqeE0yOEVnVEdxRXBZZFVxYVcxRno3bmVNa3RkbWFHV3lLSWIxZWRwQXduWFdEdDMxaWhzeDkrOHJ5Q2NQRkQKcmtQYnBTSzVBb0dCQU5BNXI5VEhURk50T0RkcUEyRExZbWxrTm9HaEdFZFY4T21WSTlubFlPNEp4R2Q3MU9aeQpMWEtmUzRGS3RpL1lxampNYjUvQklISjQ2UFRsKzJyZFBncnlxZkdrMVZDOWFCMldZOFhFeFJaTjhZSXIwYWJjCm1IeGFCVFdVYmExUlNXVVJ4SHowOTF3YlpoOUloWUdyQWh1MktNaE5UbHJHdVZ3YnlqQ3lOdDNIQW9HQkFNbU0Kd3VEY252eDdMWXNFTUU0VFdodEVoWWVrczBjeEpucVMreEIzOU91eC9oUVBMTlh0ZEUvTmpBYkY4Q25DWXZZeQp3b3h1cUNmTHRXUWxseDZHSmhuOTc4UURGU05DRzZiK0p1ZnVYUVBqbHZ6T3d4WXhDSzc3ZStlMlk3S0NDUFF1CkJ6NWNiUlhZYk1aVkhwM0pSNmgzRUk0VVlJa1hLUDRxaUZMZ1B0cVhBb0dBY0trMXBIZXNxVnJuMXJ1cVZqM1UKNGxjUlVyUFowZ2NDMFM4YmRiS3c3am9rcFNVUC9SdDcwWSswcDZESDBEMTNaRUhnaDF4VDlQdk1kMnpUeW04bwpPWDR3U3drM1RYY2RsUnlnb1FtazVUdUkxemhrTjljVlBmcnE1S0dKY2dRUUpQb25DTUQxOFVUMXpTbE02dXFlCnVtV2x6VEplbWFqNTg0Y3ByNDFOT0ZNQ2dZRUFtTk94V1l3d3ljWWxMSXBLam9sQ0EvY2JRVlZ6MDRIRGFhSlYKMlpEOEdGUnBFcERITUpmVFlFZTk2OHpmWk9yTzIxeDJCTUpMbzVGbHc4QjFMR0lRTmhsRlcxM3pBejgzZEpLMgpzWnNlMExvY1hTbnk1N3JhbU1SOG1hREZUREFwMWUybzlISmxEUEdFMllibHBrTmZvTEJYejBSSVJ1dFczQk1vCk41OXVTWlVDZ1lBNmlHU0FEVGpoeE5kYk4vS2d2RytMSUFlRVZVcEdyRWxYZ2FFa2R2ekhxOTc4S1BZcFYwNkQKTWxITkdCT1pzeDlYd2RTWWFmeHpFM0JZUkowYmdVUzJqR3dPV3N2bDRHaGtnYVQ5Zi82aktPNndaVWJmdHhWZgoyaVB5Z0JJRkdqMW95S2MveTFUTjJpYXlDRjEvaWZGNm9XNFVFMldpK1FjWHIxTDdpM2VUM1E9PQotLS0tLUVORCBSU0EgUFJJVkFURSBLRVktLS0tLQo=
     *
     * When the test case fails,the error message is as follows:
     *
     * java.lang.NoClassDefFoundError: org/yaml/snakeyaml/LoaderOptions
     * detailed information: https://github.com/apache/doris/pull/18046
     */
    @Test
    public void testClient() {
        File kubeConfigFile = new File(Config.getKubeconfigFilename());
        if (!kubeConfigFile.isFile()) {
            System.out.println("Did not find Kubernetes config at: [" + kubeConfigFile.getPath() + "]. Ignoring.");
            return;
        }
        new DefaultKubernetesClient();
    }
}
