package com.project.saga.global.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.*;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.*;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.*;

@Configuration
@EnableKafka
public class KafkaConfig {

    @Bean
    public ProducerFactory<String,String> producerFactory(KafkaProperties props){
        Map<String,Object> cfg = new HashMap<>(props.buildProducerProperties());
        cfg.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        cfg.put(ProducerConfig.ACKS_CONFIG, "all");
        cfg.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        cfg.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return new DefaultKafkaProducerFactory<>(cfg);
    }
    @Bean
    public KafkaTemplate<String,String> kafkaTemplate(ProducerFactory<String,String> pf){
        return new KafkaTemplate<>(pf);
    }

    @Bean
    public ConsumerFactory<String,String> consumerFactory(KafkaProperties props){
        Map<String,Object> cfg = new HashMap<>(props.buildConsumerProperties());
        cfg.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        cfg.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        cfg.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        return new DefaultKafkaConsumerFactory<>(cfg);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String,String> kafkaListenerContainerFactory(
            ConsumerFactory<String,String> cf){
        var f = new ConcurrentKafkaListenerContainerFactory<String,String>();
        f.setConsumerFactory(cf);
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        return f;
    }
    @Bean(name="kafkaManualAckFactory")
    public ConcurrentKafkaListenerContainerFactory<String,String> kafkaManualAckFactory(
            ConsumerFactory<String,String> cf){
        var f = new ConcurrentKafkaListenerContainerFactory<String,String>();
        f.setConsumerFactory(cf);
        f.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        f.setConcurrency(3);
        return f;
    }

    // @RetryableTopic이 요구하는 스케줄러
    @Bean
    public org.springframework.scheduling.TaskScheduler taskScheduler(){
        var t = new ThreadPoolTaskScheduler();
        t.setPoolSize(2);
        t.setThreadNamePrefix("orch-sched-");
        t.initialize();
        return t;
    }
}