package com.kafka.simulation.controller;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.kafka.simulation.services.producer.IMessageProducer;

import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/kafka")
@Slf4j
public class ProduceMessageToKafkaController {

    private final ExecutorService executorService;
    private final IMessageProducer messageProducer;


    public ProduceMessageToKafkaController(IMessageProducer messageProducer) {
        this.executorService = Executors.newVirtualThreadPerTaskExecutor();
        this.messageProducer = messageProducer;
    }
    
    @PostMapping("/bulk/{messageCount}")
    public void sendBulkMessages(@RequestBody String message, @PathVariable(name = "messageCount") Integer messageCount) {
        for (int i = 0; i < messageCount; i++) {
            final int messageId = i;
            executorService.submit(() -> {
                // log.info("Sending message ID " + messageId + ": " + message);
                messageProducer.sendMessage("topic-1", messageId+"*****"+message);
            });
        }
    }
}
