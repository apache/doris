package org.apache.doris.http.entity;

import java.util.HashMap;
import java.util.Map;

/**
 * http response entity
 * @author zhangjiafeng
 */
public final class ResponseEntity<T> {

    public ResponseEntity() {

    }

    public ResponseEntity(HttpStatus status) {
        this.code = status.value();
        this.msg = status.getReasonPhrase();
    }

    public ResponseEntity(HttpStatus status, T data) {
        this.code = status.value();
        this.msg = status.getReasonPhrase();
        this.data = data;
    }

    public ResponseEntity(HttpStatus status, Map<String, String> headers, T body) {
        this.code = status.value();
        this.msg = status.getReasonPhrase();
        this.data = body;
    }

    public ResponseEntity(HttpStatus status, Map<String, String> headers, T body, String mimetype) {
        this.code = status.value();
        this.msg = status.getReasonPhrase();
        this.data = body;
    }

    public ResponseEntity(HttpStatus status, Map<String, String> headers, T body, String mimetype, String fileName) {
        this.msg = status.getReasonPhrase();
        this.code = status.value();
        this.data = body;
    }


    private String msg;

    private int code;

    private T data;



    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }


    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    /**
     * 根据状态码创建ResponseBuilder
     *
     * @param status
     * @return
     */
    public static ResponseBuilder status(HttpStatus status) {
        return new ResponseBuilder(status);
    }

    /**
     * 创建Http OK ResponseBuilder
     *
     * @return
     */
    public static ResponseBuilder ok() {
        return status(HttpStatus.OK);
    }

    /**
     * 创建Http OK ResponseEntity
     *
     * @param data
     * @return
     */
    public static <T> ResponseEntity<T> ok(T data) {
        ResponseBuilder builder = ok();
        return builder.build(data);
    }

    /**
     * 创建Http OK ResponseEntity
     *
     * @param data
     * @param mimetype
     * @return
     */
    public static <T> ResponseEntity<T> ok(T data, String mimetype) {
        ResponseBuilder builder = ok();
        return builder.build(data, mimetype);
    }

    /**
     * 创建Http OK ResponseEntity
     *
     * @param body
     * @param mimetype
     * @param fileName
     * @return
     */
    public static <T> ResponseEntity<T> ok(T body, String mimetype, String fileName) {
        ResponseBuilder builder = ok();
        return builder.build(body, mimetype, fileName);
    }

    /**
     * 创建Http Created ResponseBuilder
     *
     * @return
     */
    public static ResponseBuilder created() {
        return status(HttpStatus.CREATED);
    }

    /**
     * 创建Http Created ResponseEntity
     *
     * @param body
     * @return
     */
    public static <T> ResponseEntity<T> created(T body) {
        ResponseBuilder builder = created();
        return builder.build(body);
    }

    /**
     * 创建Http No Content ResponseBuilder
     *
     * @return
     */
    public static ResponseBuilder noContent() {
        return status(HttpStatus.NO_CONTENT);
    }

    /**
     * 创建Http No Content ResponseEntity
     *
     * @param body
     * @return
     */
    public static <T> ResponseEntity<T> noContent(T body) {
        ResponseBuilder builder = noContent();
        return builder.build(body);
    }

    /**
     * 创建Http Not Found ResponseBuilder
     *
     * @return
     */
    public static ResponseBuilder notFound() {
        return status(HttpStatus.NOT_FOUND);
    }

    /**
     * 创建Http Not Found ResponseEntity
     *
     * @param body
     * @return
     */
    public static <T> ResponseEntity<T> notFound(T body) {
        ResponseBuilder builder = notFound();
        return builder.build(body);
    }

    /**
     * 创建Http Internal Server Error ResponseEntity
     *
     * @return
     */
    public static ResponseBuilder internalServerError() {
        return status(HttpStatus.INTERNAL_SERVER_ERROR);
    }

    /**
     * 创建Http Internal Server Error ResponseEntity
     *
     * @param data
     * @return
     */
    public static <T> ResponseEntity<T> internalServerError(T data) {
        ResponseBuilder builder = internalServerError();
        return builder.build(data);
    }

    /**
     * 设置headers
     *
     * @param headers
     * @return
     */
    public ResponseEntity<T> headers(Map<String, String> headers) {
        return this;
    }

    /**
     * Http Response 构建器
     *
     */
    public static class ResponseBuilder {

        private HttpStatus status;

        private Map<String, String> headers;

        public ResponseBuilder(HttpStatus status) {
            this.status = status;
        }

        public ResponseBuilder headers(Map<String, String> headers) {
            this.headers = headers;
            return this;
        }

        public <T> ResponseEntity<T> build() {
            return build(null);
        }

        public <T> ResponseEntity<T> build(T data) {
            return new ResponseEntity<>(this.status, this.headers, data);
        }

        public <T> ResponseEntity<T> build(T data, String mimetype) {
            return new ResponseEntity<>(this.status, this.headers, data, mimetype);
        }

        public <T> ResponseEntity<T> build(T data, String mimetype, String fileName) {
            return new ResponseEntity<>(this.status, this.headers, data, mimetype, fileName);
        }
    }

}
