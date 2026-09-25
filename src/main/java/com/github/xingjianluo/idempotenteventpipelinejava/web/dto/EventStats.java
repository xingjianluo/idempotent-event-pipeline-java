package com.github.xingjianluo.idempotenteventpipelinejava.web.dto;

/**
 * 整体投递统计。
 */
public record EventStats(long total, long pending, long processing, long delivered, long deadLetter) {
}
