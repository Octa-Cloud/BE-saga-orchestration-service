package com.project.saga.domain.domain.entity;

import jakarta.persistence.*;
import lombok.*;
import java.time.Instant;

/**
 * 유저 삭제 사가의 진행 상태를 추적하는 엔티티.
 *
 * - 상태 머신: IN_PROGRESS → (COMPENSATING | COMPLETED | FAILED | COMPENSATED)
 * - 각 하위 단계의 완료 여부(tokenOk/sleepOk/userOk)를 플래그로 기록
 * - @CreationTimestamp/@UpdateTimestamp 로 UTC 타임스탬프 자동 관리
 * - 낙관적 락(@Version)으로 동시 업데이트 충돌을 방지
 */
@Entity
@Table(name = "user_deletion_saga")
@Getter @Setter @NoArgsConstructor @AllArgsConstructor @Builder
public class UserDeletionSaga {

    public enum SagaStatus { IN_PROGRESS, COMPENSATING, COMPLETED, COMPENSATED, FAILED }

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    /** 삭제 대상 사용자 */
    @Column(nullable=false)
    private Long userNo;

    /** 이 사가 인스턴스를 식별하는 correlationId(= 각 커맨드 eventId) */
    @Column(nullable=false, length=36, unique=true)
    private String sagaId;

    /** 사가 상태(빌더 기본값 보존을 위해 @Builder.Default 적용) */
    @Builder.Default
    @Enumerated(EnumType.STRING)
    @Column(nullable=false, length=16)
    private SagaStatus status = SagaStatus.IN_PROGRESS;

    /** 하위 스텝 진행 여부(멱등/재처리에 대비하여 별도 기록) */
    @Builder.Default @Column(nullable=false) private boolean tokenOk = false;  // 토큰 폐기는 보상 없음
    @Builder.Default @Column(nullable=false) private boolean sleepOk = false;  // 슬립 삭제 성공 여부
    @Builder.Default @Column(nullable=false) private boolean userOk  = false;  // 유저 최종 삭제 성공 여부

    /** 사가 시작/갱신 시간(UTC, 하이버네이트가 자동 세팅) */
    @org.hibernate.annotations.CreationTimestamp
    @Column(nullable=false, updatable=false)
    private Instant startedAt;

    @org.hibernate.annotations.UpdateTimestamp
    @Column(nullable=false)
    private Instant updatedAt;

    /** 낙관적 락 버전 필드 → 동시 업데이트 충돌 시 예외로 감지 */
    @Version
    private Long version;

    /** 외부에서 강제로 갱신 타임스탬프를 새기고 싶을 때 사용 */
    public void touch() { this.updatedAt = Instant.now(); }
}