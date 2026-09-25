package com.github.xingjianluo.idempotenteventpipelinejava.web.dto;

/**
 * 提交响应：明确区分首次接受、并发拒绝、顺序重复与终态复用。
 */
public record SubmitResponse(String outcome, EventView event) {
}
