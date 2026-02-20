package com.kafka.consumer.repositories;

import java.time.Instant;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.kafka.consumer.entity.KafkaPartitionLockEntity;
import com.kafka.consumer.entity.KafkaPartitionLockId;

@Repository
public interface KafkaPartitionLockRepository extends JpaRepository<KafkaPartitionLockEntity, KafkaPartitionLockId> {
    
    default boolean acquireLock(String consumerGroup, String topic, int partitionId, String podId) {
        save(new KafkaPartitionLockEntity(consumerGroup, topic, partitionId, podId, Instant.now()));
        return true;
    }

    default void releaseLock(String consumerGroup, String topic, int partitionId, String podId) {
        deleteById(new KafkaPartitionLockId(consumerGroup, topic, partitionId));
    }

    default void renewLock(String consumerGroup, String topic, int partitionId) {
        findById(new KafkaPartitionLockId(consumerGroup, topic, partitionId)).ifPresentOrElse(lock -> {
            lock.setLockedAt(Instant.now());
            save(lock);
        }, () -> {
            throw new IllegalStateException(String.format("Lock not found for consumerGroup=%s, topic=%s, partitionId=%d", consumerGroup, topic, partitionId));
        });
    }
}
