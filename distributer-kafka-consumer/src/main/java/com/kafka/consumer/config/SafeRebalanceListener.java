package com.kafka.consumer.config;

import java.util.Collection;
import java.util.Objects;
import java.util.Set;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.listener.ConsumerAwareRebalanceListener;
import org.springframework.stereotype.Component;

import com.kafka.consumer.entity.KafkaOffsetId;
import com.kafka.consumer.repositories.KafkaOffsetRepository;
import com.kafka.consumer.repositories.KafkaPartitionLockRepository;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Component
@RequiredArgsConstructor
@Slf4j
public class SafeRebalanceListener implements ConsumerAwareRebalanceListener {
    
    private final KafkaOffsetRepository kafkaOffsetRepository;
    private final KafkaPartitionLockRepository kafkaPartitionLockRepository;

    private final String podId = System.getenv().getOrDefault("POD_NAME", "unknown-pod");
    private final String consumerGroup = "distributed_message_consumers";

    @Override
    public void onPartitionsAssigned(Consumer<?, ?> consumer, Collection<TopicPartition> partitions) {
        log.info("Partitions assigned: {}", partitions);
        for (TopicPartition partition : partitions) {
            // Acquire lock for the partition

            String topic = partition.topic();
            int partitionName = partition.partition();
            boolean lockAcquired = kafkaPartitionLockRepository.acquireLock(consumerGroup, topic, partitionName, podId);
            if (lockAcquired) {
                // Fetch the last committed offset from the database
                KafkaOffsetId id = new KafkaOffsetId(consumerGroup, topic, partitionName);
                kafkaOffsetRepository.findById(id)
                    .ifPresentOrElse(
                        e -> {
                            long nextOffset = e.getOffset();
                            consumer.seek(partition, nextOffset);
                        },
                        () -> {
                            OffsetAndMetadata offsetAndMetadata = consumer.committed(Set.of(partition)).get(partition);
                            long offset = 0;
                            Objects.requireNonNull(offsetAndMetadata, () -> String.format("Committed offset cannot be null for partition %s", partition));
                            kafkaOffsetRepository.storeOffset(consumerGroup, topic, partitionName, offset);

                            consumer.seek(partition, offset);
                            log.warn("Bootstrapping offset for partition {} from Kafka committed offset {}", partition, offset);
                        });
            } else {
                log.warn("Failed to acquire lock for partition {}, another instance may be processing it", partition);
            }
        }
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        partitions.forEach(partition -> {
            // Release lock for the partition
            kafkaPartitionLockRepository.releaseLock(consumerGroup, partition.topic(), partition.partition(), podId);
            log.info("Released lock for partition {}", partition);
        });
    }
}
