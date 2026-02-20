-- Offsets table
CREATE TABLE IF NOT EXISTS kafka_offsets (
    consumer_group VARCHAR(100) NOT NULL,
    topic          VARCHAR(255) NOT NULL,
    partition_id   INT NOT NULL,
    consumer_offset         BIGINT NOT NULL,
    updated_at     TIMESTAMP DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition_id)
);

-- Partition locks table
CREATE TABLE IF NOT EXISTS kafka_partition_locks (
    consumer_group VARCHAR(100) NOT NULL,
    topic          VARCHAR(255) NOT NULL,
    partition_id   INT NOT NULL,
    owner_id       VARCHAR(100) NOT NULL,
    locked_at      TIMESTAMP DEFAULT now(),
    PRIMARY KEY (consumer_group, topic, partition_id)
);

-- Index for cleanup / monitoring
CREATE INDEX IF NOT EXISTS idx_kafka_partition_locks_locked_at
ON kafka_partition_locks (locked_at);
