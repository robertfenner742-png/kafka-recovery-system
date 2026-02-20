package com.kafka.consumer.config;

import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import com.kafka.consumer.repositories.KafkaPartitionLockRepository;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@AllArgsConstructor
@Slf4j
public class LockHeatbeat {
    
    private final KafkaPartitionLockRepository kafkaPartitionLockRepository;

    @Scheduled(fixedDelay = 60_000)
    public void renewLock() {
        kafkaPartitionLockRepository.findAll().forEach(lock -> {
            kafkaPartitionLockRepository.renewLock(lock.getConsumerGroup(), lock.getTopic(), lock.getPartitionId());
        });
        log.info("Renewed all partition locks");
    }
}
