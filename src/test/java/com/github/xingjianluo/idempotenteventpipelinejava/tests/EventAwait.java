package com.github.xingjianluo.idempotenteventpipelinejava.tests;

import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventStatus;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;

import java.time.Duration;

/** 测试轮询工具。 */
final class EventAwait {

    private EventAwait() {
    }

    static EventRecord await(EventStore store, String key, EventStatus expected, Duration timeout) {
        long deadline = System.currentTimeMillis() + timeout.toMillis();
        while (System.currentTimeMillis() < deadline) {
            EventRecord e = store.findByKey(key);
            if (e != null && e.getStatus() == expected) {
                return e;
            }
            try {
                Thread.sleep(25);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ex);
            }
        }
        EventRecord e = store.findByKey(key);
        throw new AssertionError("idempotencyKey=" + key + " 未在 " + timeout + " 内达到 "
                + expected + "，当前=" + (e == null ? "不存在" : e.getStatus()));
    }
}
