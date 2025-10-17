package com.project.saga.domain.infra.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.saga.domain.domain.entity.UserDeletionSaga;
import com.project.saga.domain.domain.repository.UserDeletionSagaRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * 유저 서비스의 최종 삭제 결과 회신(user.final-delete.reply) 처리.
 *
 * - SUCCESS → userOk=true, (sleepOk도 true면) COMPLETED 로 사가 종료
 * - FAIL    → FAILED 로 표기(타임아웃 워처/운영 수동 개입 고려)
 */
@Slf4j
@Component @RequiredArgsConstructor
public class UserFinalReplyListener {

    private final ObjectMapper om;
    private final UserDeletionSagaRepository sagaRepo;

    @RetryableTopic(
            attempts="3", backoff=@Backoff(delay=1000, multiplier=2.0),
            autoCreateTopics="false", dltTopicSuffix=".dlt"
    )
    @KafkaListener(topics="user.final-delete.reply", groupId="orchestrator")
    @Transactional
    public void onUserFinalReply(String payload) throws Exception {
        var n = om.readTree(payload);
        String status = n.path("status").asText();
        String sagaId = n.path("eventId").asText();

        var saga = sagaRepo.findBySagaId(sagaId).orElseThrow(); // (필요 시 Optional 가드 적용 가능)

        if ("SUCCESS".equalsIgnoreCase(status)) {
            if (!saga.isUserOk()) saga.setUserOk(true);
            // 슬립이 이미 OK면 사가 종료
            if (saga.isSleepOk()) saga.setStatus(UserDeletionSaga.SagaStatus.COMPLETED);
            saga.touch();
        } else {
            saga.setStatus(UserDeletionSaga.SagaStatus.FAILED);
            saga.touch();
        }
    }


}