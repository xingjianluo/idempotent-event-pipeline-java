package com.github.xingjianluo.idempotenteventpipelinejava.handler;

/**
 * 不可恢复的投递失败。语义上用于区分失败原因：仍会按同一套重试/死信规则处理，
 * 但失败原因会保留该类型名，方便排查。
 */
public class PermanentDeliveryException extends RuntimeException {

    public PermanentDeliveryException(String message) {
        super(message);
    }

    public PermanentDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
