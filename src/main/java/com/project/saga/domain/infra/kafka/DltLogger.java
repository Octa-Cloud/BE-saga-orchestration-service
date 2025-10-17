package com.project.saga.domain.infra.kafka;

// 공통 유틸(선택): 헤더를 예쁘게 로깅하고, 카운터 증가
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class DltLogger {

    private final MeterRegistry meter; // micrometer

    public void logAndCount(
            String dltTopic,
            ConsumerRecord<String, String> rec,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false)   String oTopic,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_PARTITION, required = false) Integer oPart,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false)   Long oOffset,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false)    String exClass,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exMsg
    ) {
        // 메트릭: dlt 카운트
        meter.counter("kafka.dlt.count",
                        "dltTopic", dltTopic,
                        "origTopic", String.valueOf(oTopic))
                .increment();

        log.error("[DLT] topic={}, origTopic={}, origPartition={}, origOffset={}, key={}, ts={}, exClass={}, exMsg={}, payload={}",
                dltTopic,
                oTopic, oPart, oOffset,
                rec.key(),
                rec.timestamp(),
                exClass, exMsg,
                rec.value()
        );
    }
}
