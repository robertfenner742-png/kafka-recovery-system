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

@Entity
@Table(name = "kafka_offsets")
@IdClass(KafkaOffsetId.class)
@Data
@AllArgsConstructor
@NoArgsConstructor
public class KafkaOffsetEntity {
    
    @Id
    private String consumerGroup;

    @Id
    private String topic;

    @Id
    private Integer partitionId;

    @Column(name = "consumer_offset")
    private Long offset;

    @Column(name = "updated_at")
    private Instant updatedAt;
}
