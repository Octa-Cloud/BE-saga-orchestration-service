package com.project.saga.domain.infra.kafka;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import io.github.resilience4j.retry.annotation.Retry;
import io.github.resilience4j.circuitbreaker.annotation.CircuitBreaker;

/**
 * 카프카 전송 래퍼:
 * - .get() 호출로 브로커 acks=all 까지 동기 확인(실패 시 예외 발생)
 * - Resilience4j @Retry/@CircuitBreaker 로 일시 장애 완충
 *   (Fallback 은 재던져서 Outbox 백오프로 연결)
 */
@Component @RequiredArgsConstructor
public class ResilientSender {
    private final KafkaTemplate<String,String> kafka;

    @Retry(name="kafkaSend")
    @CircuitBreaker(name="kafkaSend", fallbackMethod = "sendFallback")
    public void sendSync(String topic, String key, String payload) throws Exception {
        kafka.send(topic, key, payload).get(); // 동기 전송
    }
    @SuppressWarnings("unused")
    public void sendFallback(String topic, String key, String payload, Throwable t) throws Exception {
        throw (t instanceof Exception) ? (Exception)t : new RuntimeException(t);
    }
}