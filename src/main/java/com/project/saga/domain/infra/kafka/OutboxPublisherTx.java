package com.project.saga.domain.infra.kafka;

import com.project.saga.domain.domain.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import jakarta.transaction.Transactional;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 퍼블리셔의 트랜잭션 경계:
 * - lockBatchForSend 로 락을 잡아 배치를 가져오고, 각 이벤트를 브로커로 동기 전송.
 * - 성공하면 SENT 로 마킹, 실패하면 지수 백오프 + 지터로 nextAttemptAt 조정.
 */
@Service @RequiredArgsConstructor
public class OutboxPublisherTx {
    private final OutboxRepository repo;
    private final ResilientSender sender;

    @Transactional
    public int publishOnceTx(int batchSize){
        var batch = repo.lockBatchForSend(batchSize);
        for (var e : batch){
            try {
                // eventType → 카프카 토픽 매핑(변경 시 여기를 수정)
                String topic = switch (e.getEventType()){
                    case "AUTH_REVOKE"       -> "auth.token.revoke";
                    case "SLEEP_DELETE"      -> "user.delete.command";
                    case "SLEEP_COMPENSATE"  -> "user.delete.compensate";
                    case "USER_FINAL_DELETE" -> "user.final.delete";
                    default -> throw new IllegalArgumentException("unknown eventType=" + e.getEventType());
                };
                sender.sendSync(topic, e.getPartitionKey(), e.getPayload());
                e.markSent(); // 성공
            } catch (Exception ex){
                e.failAndBackoff(nextBackoff(e.getAttempts())); // 실패 → backoff
            }
        }
        return batch.size();
    }

    /** 1,2,4,8,16,32,60s + 250~750ms 지터(최대 60s 캡) */
    private Duration nextBackoff(int attempts){
        long sec = Math.min(60, 1L << Math.min(attempts, 6));
        long jitterMs = ThreadLocalRandom.current().nextLong(250, 750);
        return Duration.ofSeconds(sec).plusMillis(jitterMs);
    }
}