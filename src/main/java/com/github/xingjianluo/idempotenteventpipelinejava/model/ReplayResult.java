package com.github.xingjianluo.idempotenteventpipelinejava.model;

/**
 * 单条死信重投结果：调用方可区分“生效”与各类拒绝原因。
 */
public record ReplayResult(ReplayOutcome outcome, EventRecord event, String reason) {

    public boolean replayed() {
        return outcome == ReplayOutcome.REPLAYED;
    }
}
