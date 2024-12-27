package com.test;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.core.KafkaTemplate;

@SpringBootApplication
public class KafkaApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}
	
	
//	@Bean
//	public ApplicationRunner runner(KafkaTemplate<String, String> kafkaTemplate) {
//		return args -> {
//			int i =0;
//			while (i<5) {
//				kafkaTemplate.send("test-streams", "test-message!zzzzzz!..");
//				Thread.sleep(1000);
//				i++;
//			}
//		};
//	}

}
