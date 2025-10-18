package com.project.saga.global.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import jakarta.annotation.PostConstruct;

/**
 * 부트 시점에 운영 인덱스를 보증한다.
 *
 * 대상/효과
 * - outbox_event:
 *    1) idx_outbox_ready_next(status, next_attempt_at, id) : 폴링/정렬 쿼리 최적화
 *    2) ux_outbox_event(event_id, event_type) UNIQUE      : 동일 이벤트 중복 방지
 * - user_deletion_saga:
 *    3) idx_saga_status_updated(status, updated_at, id)   : 타임아웃 스캐너 쿼리 최적화
 *    4) ux_saga_saga_id(saga_id) UNIQUE                   : 사가Id 단일성 보증(코드에도 unique)
 *
 * 주의
 * - 앱 DB 계정에 CREATE INDEX 권한 필요.
 * - 운영에선 Flyway/Liquibase 권장. 필요 시 해당 마이그레이션으로 대체 가능.
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SqlIndexInitializerConfig {

    private final JdbcTemplate jdbc;

    @PostConstruct
    public void ensureIndexes() {
        // 현재 스키마명 획득
        String schema = jdbc.queryForObject("SELECT DATABASE()", String.class);

        // ---- outbox_event ----
        ensure(schema, "outbox_event", "idx_outbox_ready_next",
                "CREATE INDEX idx_outbox_ready_next " +
                        "ON `outbox_event` (`status`, `next_attempt_at`, `id`)");

        ensure(schema, "outbox_event", "ux_outbox_event",
                "CREATE UNIQUE INDEX ux_outbox_event " +
                        "ON `outbox_event` (`event_id`, `event_type`)");

        // ---- user_deletion_saga ----
        ensure(schema, "user_deletion_saga", "idx_saga_status_updated",
                "CREATE INDEX idx_saga_status_updated " +
                        "ON `user_deletion_saga` (`status`, `updated_at`, `id`)");

        // 엔티티에 unique가 잡혀 있어도 스키마에 없을 수 있으니 보증
        ensure(schema, "user_deletion_saga", "ux_saga_saga_id",
                "CREATE UNIQUE INDEX ux_saga_saga_id " +
                        "ON `user_deletion_saga` (`saga_id`)");
    }

    /** index 존재 여부 확인 후 없으면 생성 */
    private void ensure(String schema, String table, String indexName, String createDdl) {
        Integer exists = jdbc.queryForObject(
                """
                SELECT COUNT(*) 
                  FROM information_schema.statistics 
                 WHERE table_schema = ? 
                   AND table_name   = ? 
                   AND index_name   = ?
                """,
                Integer.class, schema, table, indexName
        );
        if (exists != null && exists > 0) {
            log.info("[index] exists: {}.{} -> skip", table, indexName);
            return;
        }
        jdbc.execute(createDdl);
        log.info("[index] created: {}.{}", table, indexName);
    }
}