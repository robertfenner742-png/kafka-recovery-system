package com.kafka.consumer.repositories;

import java.time.Instant;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.kafka.consumer.entity.KafkaOffsetEntity;
import com.kafka.consumer.entity.KafkaOffsetId;

@Repository
public interface KafkaOffsetRepository extends JpaRepository<KafkaOffsetEntity, KafkaOffsetId> {
    
    default void storeOffset(String consumerGroup, String topic, int partitionId, long offset) {
        KafkaOffsetEntity entity = new KafkaOffsetEntity(consumerGroup, topic, partitionId, offset, Instant.now());
        save(entity);
    }
}
