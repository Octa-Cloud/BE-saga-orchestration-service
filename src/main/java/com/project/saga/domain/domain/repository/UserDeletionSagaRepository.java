package com.project.saga.domain.domain.repository;

import com.project.saga.domain.domain.entity.UserDeletionSaga;
import org.springframework.data.jpa.repository.JpaRepository;
import java.time.Instant;
import java.util.*;

/** 사가 조회 및 타임아웃 스캔용 쿼리 제공 */
public interface UserDeletionSagaRepository extends JpaRepository<UserDeletionSaga, Long> {

    /** correlationId 기반 단건 조회(멱등 체크 포함 다수 지점에서 사용) */
    Optional<UserDeletionSaga> findBySagaId(String sagaId);

    /**
     * IN_PROGRESS 상태인데 updated_at 이 오래된(=응답이 안 온) 사가 상위 N건.
     * 타임아웃 보상 워커(DeletionTimeoutWatcher)가 주기적으로 조회.
     */
    List<UserDeletionSaga> findTop200ByStatusAndUpdatedAtBeforeOrderByIdAsc(
            UserDeletionSaga.SagaStatus status, Instant before);
}