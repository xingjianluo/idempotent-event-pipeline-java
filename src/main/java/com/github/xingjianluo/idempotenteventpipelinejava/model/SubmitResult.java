package com.github.xingjianluo.idempotenteventpipelinejava.model;

/**
 * 幂等提交结果：调用方可区分“受理”与“重复复用”。
 */
public record SubmitResult(SubmitOutcome outcome, EventRecord event) {

    public boolean accepted() {
        return outcome == SubmitOutcome.ACCEPTED;
    }
}
