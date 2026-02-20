package com.kafka.simulation.services.producer;

public interface IMessageProducer {
    void sendMessage(String topic, String message);
}
