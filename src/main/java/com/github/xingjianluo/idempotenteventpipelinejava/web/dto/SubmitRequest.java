package com.github.xingjianluo.idempotenteventpipelinejava.web.dto;

/**
 * 事件提交请求。
 */
public record SubmitRequest(String idempotencyKey, String orderKey, String payload) {
}
