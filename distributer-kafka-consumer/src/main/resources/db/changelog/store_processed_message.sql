CREATE TABLE IF NOT EXISTS processed_records (
    id             UUID PRIMARY KEY,
    consumer_group VARCHAR(100) NOT NULL,
    topic          VARCHAR(255) NOT NULL,
    partition_id   INT NOT NULL,
    consumer_offset         BIGINT NOT NULL,
    created_at     TIMESTAMP DEFAULT now(),
    UNIQUE (consumer_group, topic, partition_id)
) 