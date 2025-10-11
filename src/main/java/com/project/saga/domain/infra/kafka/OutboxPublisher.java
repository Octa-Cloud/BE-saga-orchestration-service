package com.project.saga.domain.infra.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

/**
 * Outbox → Kafka 퍼블리셔(폴링형).
 *
 * - 1초 간격으로 READY + next_attempt_at 만료 레코드를 가져와 브로커 전송.
 * - 너무 많은 레코드를 한 턴에서 처리하면 다른 스케줄러가 굶을 수 있어
 *   MAX_PER_TICK 로 상한을 둔다(스레드풀/스케줄러 공정성).
 */
@Service @RequiredArgsConstructor
public class OutboxPublisher {
    private static final int BATCH = 100;         // DB에서 한 번에 집어올 수
    private static final int MAX_PER_TICK = 5000; // 한 턴 최대 처리량(굶김 방지)
    private final OutboxPublisherTx tx;

    @Scheduled(fixedDelayString = "${outbox.poll.delay-ms:1000}")
    public void publishOutbox() {
        int total = 0;
        while (total < MAX_PER_TICK) {
            int n = tx.publishOnceTx(BATCH);
            if (n == 0) break; // 더 없으면 조기 종료
            total += n;
        }
        if (total > 0) {
            // 필요 시 metrics/logging
        }
    }
}