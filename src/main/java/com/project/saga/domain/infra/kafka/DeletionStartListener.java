package com.project.saga.domain.infra.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.saga.domain.domain.service.UserDeletionOrchestrator;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * user.deletion.start 컨슈머.
 *
 * - API(유저 서비스)에서 발행한 사가 시작 이벤트를 수신하여 오케스트레이션을 트리거.
 * - 파싱 실패/DB 제약 등 예외는 @RetryableTopic 으로 재시도→DLT 로 안전하게 흘린다.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DeletionStartListener {

    private final ObjectMapper om;
    private final UserDeletionOrchestrator orchestrator;

    @RetryableTopic(
            attempts = "5",
            backoff = @Backoff(delay = 1000, multiplier = 2.0), // 1s→2s→4s→8s→16s
            autoCreateTopics = "true",
            dltTopicSuffix = ".dlt"
    )
    @KafkaListener(topics = "user.deletion.start", groupId = "orchestrator")
    @Transactional
    public void onStart(
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            @Payload String payload
    ) {
        log.info("[orchestrator] start key={}, payload={}", key, payload);
        try {
            var n = om.readTree(payload);
            String sagaId = n.path("sagaId").asText();
            long   userNo = n.path("userNo").asLong();
            String token  = n.path("accessToken").asText(null);
            long   ttlSec = n.path("accessTokenTtlSec").asLong(0);

            orchestrator.startWithSagaId(sagaId, userNo, token, ttlSec);
        } catch (Exception e) {
            // 예외를 리스루 → @RetryableTopic 가 재시도/보류/최종 DLT 로 처리
            log.error("[orchestrator] onStart failed. payload={}", payload, e);
            throw new RuntimeException(e);
        }
    }

    /** 시작 이벤트의 DLT 모니터링(운영 파악용 로그) */
    @KafkaListener(topics = "user.deletion.start.dlt", groupId = "orchestrator")
    public void onStartDlt(
            @Payload String payload,
            @Header(name = KafkaHeaders.RECEIVED_KEY, required = false) String key,
            // 표준 DLT 헤더
            @Header(name = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false) String exFqcn,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exMsg,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_STACKTRACE, required = false) String exStack,
            // 구버전/대체 헤더
            @Header(name = "kafka_exception-fqcn", required = false) String exFqcnRaw,
            @Header(name = "kafka_exception-message", required = false) String exMsgRaw,
            @Header(name = "kafka_exception-stacktrace", required = false) String exStackRaw
    ) {
        String msg = exMsg != null ? exMsg : exMsgRaw;
        String fqcn = exFqcn != null ? exFqcn : exFqcnRaw;
        String stack = exStack != null ? exStack : exStackRaw;

        log.error("[DLT][user.deletion.start] key={}, exFqcn={}, exMsg={}\npayload={}\nstack={}",
                key, fqcn, msg, payload, stack);
    }
}