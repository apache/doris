package org.apache.doris.regression.action

import groovy.transform.stc.ClosureParams
import groovy.transform.stc.FromString
import groovy.util.logging.Slf4j
import org.apache.doris.regression.suite.SuiteContext
import org.apache.http.client.methods.HttpGet
import org.apache.http.entity.StringEntity
import org.apache.http.entity.ContentType
import org.apache.http.impl.client.HttpClients
import org.apache.http.util.EntityUtils
import org.apache.http.client.methods.HttpPost
import org.apache.tools.ant.taskdefs.condition.Http;
import org.junit.Assert

@Slf4j
class HttpCliAction implements SuiteAction {
    private String endpoint
    private String uri
    private String body
    private String result
    private Closure check
    SuiteContext context

    HttpCliAction(SuiteContext context) {
        this.context = context
    }

    void endpoint(Closure<String> endpointSupplier) {
        this.endpoint = endpointSupplier.call()
    }

    void endpoint(String endpoint) {
        this.endpoint = endpoint
    }

    void uri(Closure<String> uriSupplier) {
        this.uri = uriSupplier.call()
    }

    void uri(String uri) {
        this.uri = uri
    }

    void body(Closure<String> bodySupplier) {
        this.body = bodySupplier.call()
    }

    void body(String body) {
        this.body = body
    }

    @Override
    void run() {
        try {
            def result = HttpClients.createDefault().withCloseable { client ->
                uri = "http://$endpoint" + uri
                log.info("url : " + uri)
                log.info("body: " + body)
                HttpPost httpPost = new HttpPost(uri)
                StringEntity requestEntity = new StringEntity(
                        body,
                        ContentType.APPLICATION_JSON);
                httpPost.setEntity(requestEntity)
                client.execute(httpPost).withCloseable { resp ->
                    resp.withCloseable {
                        String respJson = EntityUtils.toString(resp.getEntity())
                        def respCode = resp.getStatusLine().getStatusCode()
                        return new ActionResult(respCode, respJson)
                    }
                }
            }
            if (check != null) {
                check.call(result.respCode, result.body)
            } else {
                if (this.result != null) {
                    Assert.assertEquals(this.result, result.body)
                }
            }
        } catch (Throwable t) {
            throw new IllegalStateException("HttpCliAction failed, uri:${uri}", t)
        }
    }

    class ActionResult {
        String body
        int respCode

        ActionResult(int respCode, String body) {
            this.body = body
            this.respCode = respCode
        }
    }

    void check(@ClosureParams(value = FromString, options = ["int, String"]) Closure check) {
        this.check = check
    }
}
