//package com.test.config;
//
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.common.serialization.StringSerializer;
//import org.springframework.context.annotation.Bean;
//import org.springframework.context.annotation.Configuration;
//import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
//import org.springframework.kafka.core.ConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaProducerFactory;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.core.ProducerFactory;
//import org.springframework.kafka.listener.ContainerProperties;
//import org.springframework.kafka.listener.ContainerProperties.AckMode;
//import org.springframework.kafka.support.serializer.JsonSerializer;
//
//import lombok.AllArgsConstructor;
//
//@Configuration
//@AllArgsConstructor
//public class KafkaConfig {
//	
//	final ConsumerFactory<String, String> consumerFactory;
//	
//	@Bean
//	public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory() {
//		ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
//		
//		factory.setConsumerFactory(consumerFactory);
//		factory.setConcurrency(2);
//		ContainerProperties properties = factory.getContainerProperties();
//		properties.setAckMode(AckMode.MANUAL);
//		
//		return factory;
//	}
	
//	
//	@Bean
//    public ProducerFactory<String, Object> producerFactory() {
//        Map<String, Object> configProps = new HashMap<>();
//        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:10000");
//        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
//        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class); // JSON 직렬화 사용
//
//        return new DefaultKafkaProducerFactory<>(configProps);
//    }
//	
//	 @Bean
//	 public KafkaTemplate<String, Object> kafkaTemplate() {
//	        return new KafkaTemplate<>(producerFactory());
//	 }
//
//}
