package com.project.saga.domain.domain.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.project.saga.domain.domain.entity.OutboxEvent;
import com.project.saga.domain.domain.entity.UserDeletionSaga;
import com.project.saga.domain.domain.repository.OutboxRepository;
import com.project.saga.domain.domain.repository.UserDeletionSagaRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

/**
 * 유저 삭제 사가 오케스트레이터.
 *
 * - 시작 이벤트(user.start-delete.command)를 받아 사가 레코드를 생성하고
 *   AUTH_REVOKE → SLEEP_DELETE 두 outbox 이벤트를 차례로 적재한다.
 * - 실제 브로커 전송은 OutboxPublisher 가 담당(Outbox 패턴).
 * - 동일 sagaId 반복 수신 시 멱등 처리로 무시한다.
 */
@Service @RequiredArgsConstructor
public class UserDeletionOrchestrator {

    private final UserDeletionSagaRepository sagaRepo;
    private final OutboxRepository outboxRepo;
    private final ObjectMapper om;

    @Transactional
    public void startWithSagaId(String sagaId, Long userNo, String accessToken, long blacklistSeconds){
        // ✅ 멱등성: 동일 sagaId가 이미 있으면 무시(중복 시작 방지)
        if (sagaRepo.findBySagaId(sagaId).isPresent()) return;

        // 사가 생성(IN_PROGRESS/타임스탬프/버전은 엔티티 어노테이션으로 자동 세팅)
        var saga = UserDeletionSaga.builder()
                .sagaId(sagaId)
                .userNo(userNo)
                .build();
        sagaRepo.save(saga);

        // 1) 토큰 폐기(회신 없음 → 즉시 tokenOk=true)
        outboxRepo.save(OutboxEvent.of(
                sagaId, "User", userNo, "AUTH_REVOKE",
                json(new AuthCmd(userNo, accessToken, blacklistSeconds)),
                String.valueOf(userNo)
        ));
        saga.setTokenOk(true); saga.touch();

        // 2) 슬립 삭제 커맨드 (reply 필요)
        outboxRepo.save(OutboxEvent.of(
                sagaId, "User", userNo, "SLEEP_DELETE",
                json(new SleepCmd(userNo, sagaId)),
                String.valueOf(userNo)
        ));
    }

    /** 객체 → JSON 직렬화 헬퍼(실패 시 런타임 예외로 승격) */
    private String json(Object o){
        try { return om.writeValueAsString(o); } catch (Exception e){ throw new IllegalStateException(e); }
    }

    /** AUTH_REVOKE 페이로드 스키마 */
    public record AuthCmd(Long userNo, String accessToken, long blacklistSeconds) {}

    /** SLEEP_DELETE / SLEEP_COMPENSATE 페이로드 스키마(이벤트 상관을 위해 eventId 포함) */
    public record SleepCmd(Long userNo, String eventId) {}
}