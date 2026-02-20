package com.kafka.consumer.services.consumer;

import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

import com.kafka.consumer.repositories.KafkaOffsetRepository;
import com.kafka.consumer.repositories.StoreProcessedRecordsRepository;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@AllArgsConstructor
@Slf4j
public class KafkaMessaageConsumer {

    private final KafkaOffsetRepository kafkaOffsetRepository;
    private final StoreProcessedRecordsRepository storeProcessedRecordsRepository;

    @KafkaListener(
        id = "clusterAListener",
        groupId = "distributed_message_consumers", 
        concurrency = "5",
        clientIdPrefix = "distributed-consumer-primary-",
        containerFactory = "kafkaListenerContainerFactory",
        properties = {
            "bootstrap.servers=${kafka.clusters.primary.bootstrap-servers}"
        },
        topicPattern = "${kafka.clusters.primary.topic.name}",
        autoStartup = "false"
    )
    public void consumeMessageFromClusterA(Acknowledgment acknowledgment, ConsumerRecord<String, String> consumerRecord) {
        processMessage("distributed-consumer-primary", acknowledgment, consumerRecord);
    }
    
    @KafkaListener(
        id = "clusterBListener",
        groupId = "distributed_message_consumers", 
        concurrency = "5",
        clientIdPrefix = "distributed-consumer-secondary-",
        containerFactory = "kafkaListenerContainerFactory",
        properties = {
            "bootstrap.servers=${kafka.clusters.secondary.bootstrap-servers}"
        },
        topicPattern = "${kafka.clusters.secondary.topic.name}",
        autoStartup = "false"
    )
    public void consumeMessageFromClusterB(ConsumerRecord<String, String> consumerRecord, Acknowledgment acknowledgment) {
        processMessage("distributed-consumer-secondary", acknowledgment, consumerRecord);
    }

    private void processMessage(String clientIdPrefix, Acknowledgment acknowledgment, ConsumerRecord<String, String> consumerRecord) {
        String topic = consumerRecord.topic();
        consumerRecord.headers().headers(KafkaHeaders.GROUP_ID).forEach(data -> log.info(data.key(), data.value()));
        String msgToPrint = String.format("[%s] - [%s] - Topic:%s - Message: %s", clientIdPrefix, Thread.currentThread().getName(), topic, consumerRecord.value());
        log.info(msgToPrint);
        log.info("topic: {} partition: {} with offset: {}", topic, consumerRecord.partition(), consumerRecord.offset()+1);
        // consumer logic goes here
        kafkaOffsetRepository.storeOffset("distributed_message_consumers", topic, consumerRecord.partition(), consumerRecord.offset() + 1);

        // save processed records in DB to cross check, duplicate messages are not processed when cluster was switched
        storeProcessedRecordsRepository.upcertProcessedRecords(UUID.randomUUID(), "distributed_message_consumers", topic, consumerRecord.partition(), 1L, Instant.now());
        acknowledgment.acknowledge();
    }
}
