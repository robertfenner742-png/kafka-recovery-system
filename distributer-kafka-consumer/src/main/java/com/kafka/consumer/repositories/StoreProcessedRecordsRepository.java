package com.kafka.consumer.repositories;

import java.time.Instant;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import com.kafka.consumer.entity.StoreProcessedRecordsEntity;

@Repository
public interface StoreProcessedRecordsRepository extends JpaRepository<StoreProcessedRecordsEntity, UUID> {
    
    @Modifying
    @Transactional
    @Query(value = """
        INSERT INTO processed_records (
            id,
            consumer_group,
            topic,
            partition_id,
            consumer_offset,
            created_at
        )
            VALUES (
                :id,
                :consumerGroup,
                :topic,
                :partitionId,
                :offset,
                :createdAt
            )
            ON CONFLICT (consumer_group, topic, partition_id)
            DO UPDATE SET
                consumer_offset = processed_records.consumer_offset + 1,
                created_at = EXCLUDED.created_at
        """,
        nativeQuery = true
    )
    void upcertProcessedRecords(
        @Param("id") UUID id,
        @Param("consumerGroup") String consumerGroup,
        @Param("topic") String topic,
        @Param("partitionId") Integer partitionId,
        @Param("offset") Long offset,
        @Param("createdAt") Instant createdAt
    );
}
