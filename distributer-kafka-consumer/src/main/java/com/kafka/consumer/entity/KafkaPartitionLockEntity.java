package com.kafka.consumer.entity;

import java.time.Instant;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "kafka_partition_locks")
@IdClass(KafkaPartitionLockId.class)
@Data
@AllArgsConstructor
@NoArgsConstructor
@Setter
public class KafkaPartitionLockEntity {
    
    @Id
    private String consumerGroup;

    @Id
    private String topic;

    @Id
    private Integer partitionId;

    @Column(name = "owner_id")
    private String ownerId;

    @Column(name = "locked_at")
    private Instant lockedAt;
}
