package com.project.saga.domain.domain.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.*;

/**
 * Outbox 패턴용 이벤트 레코드.
 *
 * - 오케스트레이션 서비스는 도메인 트랜잭션 안에서 이 엔티티를 INSERT만 한다.
 * - 별도 배경 스케줄러(OutboxPublisher)가 READY 상태의 레코드를 잠금으로 집어와
 *   Kafka 로 전송 후 SENT 로 마킹한다.
 * - 전송 실패 시 backoff 를 적용하여 일정 시간 뒤 재시도한다.
 *
 * 운영 인덱스 권장:
 *   CREATE INDEX idx_outbox_ready_next ON outbox_event(status, next_attempt_at, id);
 *   CREATE UNIQUE INDEX ux_outbox_event ON outbox_event(event_id, event_type);
 */
@Entity
@Table(name = "outbox_event")
@Getter @NoArgsConstructor
public class OutboxEvent {

    public enum Status { READY, SENT, FAILED } // FAILED 는(필요 시) 수동 조사를 위해 유지

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 사가의 상관키(correlation) → 이번 설계에서는 sagaId 와 동일 */
    @Column(nullable=false, length=36)
    private String eventId;

    /** 이벤트가 속한 애그리게이트 타입(모니터링/검색용) */
    @Column(nullable=false, length=64)
    private String aggregateType;     // "User"

    /** 애그리게이트 식별자(파티셔닝 키와도 동일 케이스가 많음) */
    @Column(nullable=false)
    private Long aggregateId;         // userNo

    /** 전송할 이벤트 종류(→ 카프카 토픽 매핑에 사용) */
    @Column(nullable=false, length=64)
    private String eventType;         // AUTH_REVOKE | SLEEP_DELETE | SLEEP_COMPENSATE | USER_FINAL_DELETE

    /** 브로커로 흘려보낼 JSON 페이로드 (RDB에서는 단순 문자열로 보관) */
    @Column(nullable=false, columnDefinition="json")
    private String payload;

    /** 카프카 파티셔닝 키(동일 유저 이벤트 순서 보장을 위해 userNo 문자열 사용) */
    @Column(nullable=false, length=128)
    private String partitionKey;      // userNo 문자열

    /** 준비됨 → 전송됨 → (실패 시) 재시도 대기/실패 마킹 */
    @Enumerated(EnumType.STRING)
    @Column(nullable=false, length=16)
    private Status status = Status.READY;

    /** 누적 전송 시도 횟수(지수 백오프 계산에 사용) */
    @Column(nullable=false)
    private Integer attempts = 0;

    /** 생성 시간(UTC, 하이버네이트가 자동 세팅) */
    @org.hibernate.annotations.CreationTimestamp
    @Column(nullable=false, updatable=false)
    private Instant createdAt;

    /** 다음 재시도 시각(UTC). 전송 성공 시엔 의미 없음 */
    @Column(nullable=false)
    private Instant nextAttemptAt = Instant.now();

    /** 실제 브로커 전송 성공 시간(모니터링용) */
    private Instant processedAt;

    /** 팩토리 메서드: 필요한 필드만 받아 간결하게 생성 */
    public static OutboxEvent of(String eventId, String aggType, Long aggId,
                                 String eventType, String payload, String partitionKey){
        OutboxEvent e = new OutboxEvent();
        e.eventId = eventId; e.aggregateType = aggType; e.aggregateId = aggId;
        e.eventType = eventType; e.payload = payload; e.partitionKey = partitionKey;
        return e;
    }

    /** 전송 성공 마킹(상태 전이 + 타임스탬프 기록) */
    public void markSent(){ this.status = Status.SENT; this.processedAt = Instant.now(); }

    /**
     * 전송 실패 시 backoff 적용:
     * - attempts 증가
     * - nextAttemptAt 을 현재+지연 으로 갱신
     * - 상태는 READY 로 유지 → 퍼블리셔가 다음 턴에 다시 집어가게 함
     */
    public void failAndBackoff(Duration d){
        this.attempts = this.attempts + 1;
        this.nextAttemptAt = Instant.now().plus(d);
        this.status = Status.READY; // 재시도 대기
    }
}