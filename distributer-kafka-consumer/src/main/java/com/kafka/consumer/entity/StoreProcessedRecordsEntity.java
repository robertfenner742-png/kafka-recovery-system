package com.kafka.consumer.entity;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "processed_records",
    uniqueConstraints = {
        @UniqueConstraint(
            name="processed_records_consumer_group_topic_partition_id_key",
            columnNames = {"consumer_group", "topic", "partition_id"}
        )
    }
)
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder(toBuilder = true)
public class StoreProcessedRecordsEntity {
    
    @Id
    @GeneratedValue
    @Column(name = "id", nullable = false, updatable = false)
    private UUID id;

    @Column(name = "consumer_group")
    private String consumerGroup;

    @Column(name = "topic")
    private String topic;

    @Column(name = "partition_id")
    private Integer partitionId;

    @Column(name = "consumer_offset")
    private Long msgCount;

    @Column(name = "created_at")
    private Instant createdAt;
}
