package com.github.xingjianluo.idempotenteventpipelinejava.handler;

/**
 * 可重试的临时性投递失败。
 */
public class TransientDeliveryException extends RuntimeException {

    public TransientDeliveryException(String message) {
        super(message);
    }

    public TransientDeliveryException(String message, Throwable cause) {
        super(message, cause);
    }
}
