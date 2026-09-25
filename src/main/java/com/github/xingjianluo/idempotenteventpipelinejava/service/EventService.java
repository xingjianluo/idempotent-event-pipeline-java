package com.github.xingjianluo.idempotenteventpipelinejava.service;

import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.domain.SubmitOutcome;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import org.springframework.stereotype.Service;

/**
 * 事件接收与查询服务。
 */
@Service
public class EventService {

    private final EventStore store;

    public EventService(EventStore store) {
        this.store = store;
    }

    /** 幂等提交，返回判定结论与事件记录。 */
    public EventStore.SubmitResult submit(String idempotencyKey, String orderKey, String payload) {
        return store.submit(idempotencyKey, orderKey, payload, java.time.Instant.now());
    }

    public EventRecord get(String idempotencyKey) {
        return store.findByKey(idempotencyKey);
    }
}
