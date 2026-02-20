package com.kafka.consumer.config;

import java.util.Collection;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.jspecify.annotations.Nullable;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.HealthIndicator;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class KafkaClusterHealth implements HealthIndicator {
    
    private AdminClient adminClient;
    private final String bootstrap;

    public KafkaClusterHealth(@Value("${kafka.clusters.primary.bootstrap-servers}") String bootstrap) {
        this.bootstrap = bootstrap;
        initializeAdminClient();
    }

    public boolean isClusterHealthy() {
        try {
            initializeAdminClient();
            Objects.requireNonNull(adminClient, "adminClient must not be null");
            Collection<TopicListing> topics = adminClient.listTopics().listings().get(2, TimeUnit.SECONDS);
            log.info("List of topics are {}", topics);
            return !topics.isEmpty();
        } catch (Exception e) {
            log.error("Failed to retrieve cluster information. Primary cluster is unhealty.");
            return false;
        }
    }

    public synchronized void initializeAdminClient() {
        if (Objects.isNull(adminClient)) {
            try {
                Properties config = new Properties();
                config.put("bootstrap.servers", bootstrap);

                this.adminClient = AdminClient.create(config);
            } catch (Exception e) {
                log.error("Failed to connect to the broker: ", e.getMessage());
                this.adminClient = null;
            }
        }
    }

    @Override
    public @Nullable Health health() {
        return isClusterHealthy() ? Health.up().withDetail("satus", "Reachable").build() : Health.down().withDetail("satus", "Unreachable").build();
    }
}
