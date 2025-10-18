package com.project.saga.domain.domain.repository;

import com.project.saga.domain.domain.entity.OutboxEvent;
import org.springframework.data.jpa.repository.*;
import org.springframework.data.repository.query.Param;
import java.util.List;

/**
 * Outbox 퍼블리셔가 READY 이벤트를 락으로 안전하게 "가져가기" 위한 쿼리 제공.
 *
 * - FOR UPDATE SKIP LOCKED: 다중 퍼블리셔(스케줄러 컨커런시 등)에서도
 *   서로 다른 레코드를 집어가도록 보장.
 * - next_attempt_at <= 현재(UTC) 인 것만 집음 → backoff 대기 중 이벤트는 건너뜀.
 * - 정렬: next_attempt_at, id 순으로 가장 오래 대기한 이벤트부터 전송.
 */
public interface OutboxRepository extends JpaRepository<OutboxEvent, Long> {

    @Query(value = """
      SELECT * FROM outbox_event
      WHERE status='READY' AND next_attempt_at <= UTC_TIMESTAMP(3)
      ORDER BY next_attempt_at, id
      LIMIT :batch
      FOR UPDATE SKIP LOCKED
      """, nativeQuery = true)
    List<OutboxEvent> lockBatchForSend(@Param("batch") int batch);

    /** 동일 (eventId, eventType) 중복 전송 방지(멱등 체크)용 존재 여부 */
    boolean existsByEventIdAndEventType(String eventId, String eventType);
}

// 운영 ddl 인덱스
//CREATE INDEX idx_outbox_ready_next ON outbox_event(status, next_attempt_at, id);
//CREATE UNIQUE INDEX ux_outbox_event ON outbox_event(event_id, event_type);
//CREATE INDEX idx_saga_status_updated ON user_deletion_saga(status, updated_at, id);