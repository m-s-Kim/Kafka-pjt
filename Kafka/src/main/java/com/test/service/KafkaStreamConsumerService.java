package com.test.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class KafkaStreamConsumerService {
	private final Logger log = LoggerFactory.getLogger(this.getClass());

    @KafkaListener(id = "test-streams-to-id", topics = "test-streams-to")
    public void listen(String message) {
        log.info("[Consumer] KAFKASTREAM message = {}", message);
    }
}
