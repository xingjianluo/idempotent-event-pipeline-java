package com.github.xingjianluo.idempotenteventpipelinejava.model;

/**
 * 幂等提交的结果类别。
 */
public enum SubmitOutcome {
    /** 首次提交，已受理并生成新事件。 */
    ACCEPTED,
    /** 幂等键已存在，复用已有事件，不产生新投递。 */
    DUPLICATE
}
