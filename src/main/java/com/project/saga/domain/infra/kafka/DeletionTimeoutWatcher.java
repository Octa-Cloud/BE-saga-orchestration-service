package com.project.saga.domain.infra.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.saga.domain.domain.entity.OutboxEvent;
import com.project.saga.domain.domain.entity.UserDeletionSaga;
import com.project.saga.domain.domain.repository.OutboxRepository;
import com.project.saga.domain.domain.repository.UserDeletionSagaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.util.List;

/**
 * 사가 타임아웃 감시자:
 * - IN_PROGRESS 상태가 너무 오래 머물러 있으면 SLEEP_COMPENSATE 를 발행하여 복구.
 * - 보상 자체도 Outbox 로 적재 → 퍼블리셔가 카프카로 전송.
 */
@Service @RequiredArgsConstructor
public class DeletionTimeoutWatcher {

    private final UserDeletionSagaRepository sagaRepo;
    private final OutboxRepository outboxRepo;
    private final ObjectMapper om;

    /** IN_PROGRESS 허용 시간(초). 초과 시 보상 전환 */
    @Value("${saga.delete.pending-ttl-sec:180}")
    private long pendingTtlSec;

    @Scheduled(fixedDelayString = "${saga.delete.timeout-scan-ms:30000}")
    @Transactional
    public void scanAndCompensate(){
        Instant cutoff = Instant.now().minusSeconds(pendingTtlSec);

        // 오래된 진행중 사가 상위 200건만 처리(폭주 방지)
        var stuck = sagaRepo.findTop200ByStatusAndUpdatedAtBeforeOrderByIdAsc(
                UserDeletionSaga.SagaStatus.IN_PROGRESS, cutoff);

        for (var s : stuck){
            s.setStatus(UserDeletionSaga.SagaStatus.COMPENSATING);
            s.touch();
            outboxRepo.save(OutboxEvent.of(
                    s.getSagaId(), "User", s.getUserNo(), "SLEEP_COMPENSATE",
                    json(om.createObjectNode().put("eventId", s.getSagaId()).put("userNo", s.getUserNo())),
                    String.valueOf(s.getUserNo())
            ));
        }
    }

    private String json(Object o){
        try { return om.writeValueAsString(o); } catch (Exception e){ throw new IllegalStateException(e); }
    }
}