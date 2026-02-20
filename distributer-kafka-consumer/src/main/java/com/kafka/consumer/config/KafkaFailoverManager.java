package com.kafka.consumer.config;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Component
@EnableScheduling
@Slf4j
public class KafkaFailoverManager {
    
    private final KafkaClusterHealth clusterHealth;
    private final KafkaListenerEndpointRegistry listenerRegistry;
    private final AtomicBoolean isFailoverActive = new AtomicBoolean(false);
    private boolean serviceStarted = false;

    public KafkaFailoverManager(KafkaClusterHealth clusterHealth, KafkaListenerEndpointRegistry listenerRegistry) {
        this.clusterHealth = clusterHealth;
        this.listenerRegistry = listenerRegistry;
    }

    @Scheduled(fixedDelay = 5000)
    public void monitorAndFailover() {
        try {
            boolean isHealthy = clusterHealth.isClusterHealthy();
        
            MessageListenerContainer listenerAContainer = listenerRegistry.getListenerContainer("clusterAListener");
            MessageListenerContainer listenerBContainer = listenerRegistry.getListenerContainer("clusterBListener");

            Objects.requireNonNull(listenerAContainer, "Listener container for clusterAListener not found");
            Objects.requireNonNull(listenerBContainer, "Listener container for clusterBListener not found");

            // FAILOVER LOGIC
            if (!isHealthy && isFailoverActive.compareAndSet(false, true) && serviceStarted) {
                log.warn("Primary cluster is down. Activating failover to secondary cluster.");
                hardStop(listenerAContainer);
                safelyStart(listenerBContainer);
                return;
            }
            
            // FALLBACK LOGIC
            if (isHealthy && isFailoverActive.compareAndSet(true, false) && serviceStarted) {
                log.warn("Primary cluster is back online. Reverting to primary cluster.");
                hardStop(listenerBContainer);
                safelyStart(listenerAContainer);
                return;
            }

            // FIRST time consumer start
            if (isHealthy && isFailoverActive.get() == false && serviceStarted == false) {
                log.warn("Starting consumer with the default/primary cluster");
                safelyStart(listenerAContainer);
                serviceStarted = true;
                return;
            }
        } catch (Exception e) {
            log.error("There was some problem while switching failover to another cluser", e.getMessage());
        }

        // log.info("None of the condition satisfied");
    }

    private void hardStop(MessageListenerContainer container) {
        if (container != null && container.isRunning()) {
            container.stop();

            while (container.isRunning()) {
                sleepSilently(200);
            }
            log.info("Stopped listener: {}", container.getListenerId());
        }
    }

    private void safelyStart(MessageListenerContainer container) {
        if (container != null && !container.isRunning()) {
            container.start();

            log.info("Started listener: {}", container.getListenerId());
        }
    }

    private void sleepSilently(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
