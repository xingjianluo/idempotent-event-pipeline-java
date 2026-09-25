package com.github.xingjianluo.idempotenteventpipelinejava.service;

import com.github.xingjianluo.idempotenteventpipelinejava.domain.EventRecord;
import com.github.xingjianluo.idempotenteventpipelinejava.store.EventStore;
import com.github.xingjianluo.idempotenteventpipelinejava.web.dto.EventStats;
import com.github.xingjianluo.idempotenteventpipelinejava.web.dto.EventView;
import org.springframework.stereotype.Service;

/**
 * 状态查询与整体投递统计。
 */
@Service
public class ObservabilityService {

    private final EventStore store;

    public ObservabilityService(EventStore store) {
        this.store = store;
    }

    public EventView getEvent(String idempotencyKey) {
        EventRecord e = store.findByKey(idempotencyKey);
        return e == null ? null : EventView.from(e);
    }

    public EventStats stats() {
        long pending = 0, processing = 0, delivered = 0, dead = 0;
        for (EventRecord e : store.findAll()) {
            switch (e.getStatus()) {
                case PENDING -> pending++;
                case PROCESSING -> processing++;
                case DELIVERED -> delivered++;
                case DEAD_LETTER -> dead++;
            }
        }
        return new EventStats(pending + processing + delivered + dead,
                pending, processing, delivered, dead);
    }
}
