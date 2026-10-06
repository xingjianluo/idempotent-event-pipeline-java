package com.github.xingjianluo.idempotenteventpipelinejava.model;

import java.util.List;

/**
 * 批量重投结果：逐条记录每条死信的重投结局，便于区分生效与拒绝。
 */
public record BatchReplayResult(int limit, int replayed, int rejected, List<ReplayResult> results) {
}
