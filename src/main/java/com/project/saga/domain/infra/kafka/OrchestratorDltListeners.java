package com.project.saga.domain.infra.kafka;
import lombok.RequiredArgsConstructor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class OrchestratorDltListeners {

    private final DltLogger dlt;

    @KafkaListener(topics = "sleep.user-delete.reply.dlt", groupId = "orchestrator")
    public void onSleepReplyDlt(
            ConsumerRecord<String,String> rec,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false)   String oTopic,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_PARTITION, required = false) Integer oPart,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false)   Long oOffset,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false)    String exClass,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exMsg
    ){
        dlt.logAndCount("sleep.user-delete.reply.dlt", rec, oTopic, oPart, oOffset, exClass, exMsg);
    }

    @KafkaListener(topics = "user.final-delete.reply.dlt", groupId = "orchestrator")
    public void onUserFinalReplyDlt(
            ConsumerRecord<String,String> rec,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false)   String oTopic,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_PARTITION, required = false) Integer oPart,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false)   Long oOffset,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false)    String exClass,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exMsg
    ){
        dlt.logAndCount("user.final-delete.reply.dlt", rec, oTopic, oPart, oOffset, exClass, exMsg);
    }

    // (선택) 사가 시작 커맨드 DLT도 오케스트레이터가 본다면 여기에 추가
    @KafkaListener(topics = "user.start-delete.command.dlt", groupId = "orchestrator")
    public void onStartDeleteCmdDlt(
            ConsumerRecord<String,String> rec,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_TOPIC, required = false)   String oTopic,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_PARTITION, required = false) Integer oPart,
            @Header(name = KafkaHeaders.DLT_ORIGINAL_OFFSET, required = false)   Long oOffset,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_FQCN, required = false)    String exClass,
            @Header(name = KafkaHeaders.DLT_EXCEPTION_MESSAGE, required = false) String exMsg
    ){
        dlt.logAndCount("user.start-delete.command.dlt", rec, oTopic, oPart, oOffset, exClass, exMsg);
    }
}