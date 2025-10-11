package com.project.saga.domain.infra.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.saga.domain.domain.entity.OutboxEvent;
import com.project.saga.domain.domain.entity.UserDeletionSaga;
import com.project.saga.domain.domain.repository.OutboxRepository;
import com.project.saga.domain.domain.repository.UserDeletionSagaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 슬립 서비스의 삭제/보상 결과 회신(user.delete.reply) 처리.
 *
 * - SUCCESS/DELETE → sleepOk=true, 조건 충족 시 USER_FINAL_DELETE outbox 적재
 * - FAIL          → COMPENSATING 으로 전환하고 SLEEP_COMPENSATE outbox 적재
 * - SUCCESS/COMPENSATE → COMPENSATED 로 표기
 *
 * 주의:
 * - 과거(시작 실패 등)로 인해 사가 레코드가 없을 수 있는데,
 *   현재 코드는 orElseThrow 로 DLT 로 보냄. 운영에서 reply-만 도착하는
 *   희귀 케이스를 “무해하게 스킵”하려면 Optional 가드로 바꾸는 것을 권장(TODO).
 */
@Slf4j
@Component @RequiredArgsConstructor
public class SleepReplyListener {

    private final ObjectMapper om;
    private final UserDeletionSagaRepository sagaRepo;
    private final OutboxRepository outboxRepo;

    @RetryableTopic(
            attempts = "5",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            autoCreateTopics = "true", dltTopicSuffix = ".dlt"
    )
    @KafkaListener(topics = "user.delete.reply", groupId = "orchestrator")
    @Transactional
    public void onSleepReply(String payload) throws Exception {
        var n = om.readTree(payload);
        String status = n.path("status").asText();                 // SUCCESS / FAIL ...
        String type   = n.path("type").asText("DELETE").toUpperCase(); // DELETE / COMPENSATE
        String sagaId = n.path("eventId").asText();
        long userNo   = n.path("userNo").asLong();

        var saga = sagaRepo.findBySagaId(sagaId).orElseThrow();    // TODO: Optional 가드로 변경 고려

        if (!"SUCCESS".equalsIgnoreCase(status)) {
            // 실패 → 보상 트리거 (이미 COMPENSATING이면 중복 방지)
            if (saga.getStatus() != UserDeletionSaga.SagaStatus.COMPENSATING) {
                saga.setStatus(UserDeletionSaga.SagaStatus.COMPENSATING);
                saga.touch();
                outboxRepo.save(OutboxEvent.of(
                        sagaId, "User", userNo, "SLEEP_COMPENSATE",
                        om.writeValueAsString(om.createObjectNode()
                                .put("eventId", sagaId).put("userNo", userNo)),
                        String.valueOf(userNo)
                ));
            }
            return;
        }

        if ("COMPENSATE".equals(type)) {
            // 보상 성공 마킹
            saga.setStatus(UserDeletionSaga.SagaStatus.COMPENSATED);
            saga.touch();
            return;
        }

        if (!"DELETE".equals(type)) return; // 관심 없는 타입은 무시

        // 슬립 삭제 성공(멱등 보정)
        if (!saga.isSleepOk()) {
            saga.setSleepOk(true);
            saga.touch();
        }

        // 모든 선행조건 충족 시 유저 최종 삭제 커맨드 발행(중복 방지 체크)
        if (saga.isTokenOk() && saga.isSleepOk() && !saga.isUserOk()
                && !outboxRepo.existsByEventIdAndEventType(sagaId, "USER_FINAL_DELETE")) {
            outboxRepo.save(OutboxEvent.of(
                    sagaId, "User", userNo, "USER_FINAL_DELETE",
                    om.writeValueAsString(om.createObjectNode()
                            .put("eventId", sagaId).put("userNo", userNo)),
                    String.valueOf(userNo)
            ));
        }
    }

    /** reply 의 DLT 모니터링(운영 파악용 로그) */
    @KafkaListener(topics="user.delete.reply.dlt", groupId="orchestrator")
    public void onSleepReplyDlt(String payload){
        log.error("[DLT][sleep.reply] {}", payload);
    }
}