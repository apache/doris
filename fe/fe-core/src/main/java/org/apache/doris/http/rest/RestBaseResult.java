package org.apache.doris.http.rest;

import com.google.gson.Gson;

// Base restful result
public class RestBaseResult {
    private static final RestBaseResult OK = new RestBaseResult();
    public ActionStatus status;
    public String msg;

    public RestBaseResult() {
        status = ActionStatus.OK;
        msg = "Success";
    }

    public RestBaseResult(String msg) {
        status = ActionStatus.FAILED;
        this.msg = msg;
    }

    public static RestBaseResult getOk() {
        return OK;
    }

    public String toJson() {
        Gson gson = new Gson();
        return gson.toJson(this);
    }
}
