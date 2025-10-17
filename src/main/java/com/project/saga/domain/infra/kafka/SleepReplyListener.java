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
 * 슬립 서비스의 삭제/보상 결과 회신(sleep.user-delete.reply) 처리.
 *
 * 규칙
 * - SUCCESS / type=DELETE      → sleepOk=true, 조건 충족 시 USER_FINAL_DELETE 발행
 * - SUCCESS / type=COMPENSATE  → COMPENSATED 로 표기(보상 성공)
 * - FAIL    / type=DELETE      → (최초 1회) COMPENSATING 전환 + SLEEP_COMPENSATE 발행
 * - FAIL    / type=COMPENSATE  → FAILED 로 종결(보상 자체가 실패 → 더 이상 진행 불가)
 *
 * 주의
 * - 사가 레코드가 없으면 운영에서는 무해 스킵하는 편이 안전하지만,
 *   현재는 orElseThrow 로 두어 데이터/플로우 문제를 조기에 드러내도록 함.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SleepReplyListener {

    private final ObjectMapper om;
    private final UserDeletionSagaRepository sagaRepo;
    private final OutboxRepository outboxRepo;

    @RetryableTopic(
            attempts = "3",
            backoff = @Backoff(delay = 1000, multiplier = 2.0),
            autoCreateTopics = "false",
            dltTopicSuffix = ".dlt"
    )
    @KafkaListener(topics = "sleep.user-delete.reply", groupId = "orchestrator")
    @Transactional
    public void onSleepReply(String payload) throws Exception {
        var n = om.readTree(payload);
        String status = n.path("status").asText();                    // SUCCESS | FAIL
        String type   = n.path("type").asText("DELETE").toUpperCase();// DELETE | COMPENSATE
        String sagaId = n.path("eventId").asText();
        long userNo   = n.path("userNo").asLong();

        var saga = sagaRepo.findBySagaId(sagaId).orElseThrow();

        // ---- FAIL 경로 ----
        if (!"SUCCESS".equalsIgnoreCase(status)) {
            if ("COMPENSATE".equals(type)) {
                // 보상도 실패 → 더 이상 할 게 없음. 사가를 FAILED 로 종결
                saga.setStatus(UserDeletionSaga.SagaStatus.FAILED);
                saga.touch();
                log.warn("[orchestrator] sleep FAIL/COMPENSATE -> mark FAILED. sagaId={}, userNo={}", sagaId, userNo);
                return;
            }

            // DELETE 실패: 최초 1회만 보상 트리거
            if (saga.getStatus() != UserDeletionSaga.SagaStatus.COMPENSATING) {
                saga.setStatus(UserDeletionSaga.SagaStatus.COMPENSATING);
                saga.touch();
                if (!outboxRepo.existsByEventIdAndEventType(sagaId, "SLEEP_COMPENSATE")) {
                    outboxRepo.save(OutboxEvent.of(
                            sagaId, "User", userNo, "SLEEP_COMPENSATE",
                            om.writeValueAsString(om.createObjectNode()
                                    .put("eventId", sagaId).put("userNo", userNo)),
                            String.valueOf(userNo)
                    ));
                }
                log.warn("[orchestrator] sleep FAIL/DELETE -> trigger COMPENSATE. sagaId={}, userNo={}", sagaId, userNo);
            }
            return;
        }

        // ---- SUCCESS 경로 ----
        if ("COMPENSATE".equals(type)) {
            // 보상 성공
            saga.setStatus(UserDeletionSaga.SagaStatus.COMPENSATED);
            saga.touch();
            log.info("[orchestrator] sleep SUCCESS/COMPENSATE -> COMPENSATED. sagaId={}, userNo={}", sagaId, userNo);
            return;
        }

        if (!"DELETE".equals(type)) {
            // 우리가 관심 없는 타입이면 무해 스킵
            log.debug("[orchestrator] ignore sleep reply type={}, sagaId={}", type, sagaId);
            return;
        }

        // DELETE 성공: 멱등 보정
        if (!saga.isSleepOk()) {
            saga.setSleepOk(true);
            saga.touch();
        }

        // 토큰/슬립 완료 & 아직 유저 최종 미발행이면 USER_FINAL_DELETE 커맨드 발행
        if (saga.isTokenOk() && saga.isSleepOk() && !saga.isUserOk()
                && !outboxRepo.existsByEventIdAndEventType(sagaId, "USER_FINAL_DELETE")) {

            outboxRepo.save(OutboxEvent.of(
                    sagaId, "User", userNo, "USER_FINAL_DELETE",
                    om.writeValueAsString(om.createObjectNode()
                            .put("eventId", sagaId).put("userNo", userNo)),
                    String.valueOf(userNo)
            ));
            log.info("[orchestrator] emit USER_FINAL_DELETE. sagaId={}, userNo={}", sagaId, userNo);
        }
    }

}