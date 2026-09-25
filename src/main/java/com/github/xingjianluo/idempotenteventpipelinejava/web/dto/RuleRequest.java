package com.github.xingjianluo.idempotenteventpipelinejava.web.dto;

/**
 * 管理接口：为某个幂等键配置前 failTimes 次投递失败。
 */
public record RuleRequest(String idempotencyKey, Integer failTimes, Boolean permanent) {
}
