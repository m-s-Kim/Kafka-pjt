package com.test.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

import org.apache.kafka.common.PartitionInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import com.test.entity.KafkaSendData;
import com.test.repository.KafkaRecvRepository;
import com.test.repository.KafkaSendRepository;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class KafkaPartitionOrKeyService {
	
	private final KafkaTemplate<String, Object> kafkaTemplate;
	
	private final KafkaSendRepository kafkaSendRepository;
	private final KafkaRecvRepository kafkaRecvRepository;
	
	@Value("${server.port}")
	private String port;
	
	private LocalDate today = LocalDate.now();
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
	private String formattedDate = today.format(formatter);
	
	@Autowired
    public KafkaPartitionOrKeyService(KafkaTemplate<String, Object> kafkaTemplate, KafkaSendRepository kafkaSendRepository, KafkaRecvRepository kafkaRecvRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaSendRepository = kafkaSendRepository;
        this.kafkaRecvRepository = kafkaRecvRepository;
	}
	
	/**
	 * 
	 * @param topic
	 * @param key
	 * @param message
	 */
    public void sendMessageWithKey(String topic, String key, String message) {
        kafkaTemplate.send(topic, key, message);
        log.info("Sent message [%s] with key [%s] to topic [%s]%n", message, key, topic);
        
        KafkaSendData kafkaSendData  = new KafkaSendData();
        kafkaSendData.setSendData(message); 
        kafkaSendData.setBaseDt(formattedDate);
        kafkaSendData.setTopic(topic); 
        kafkaSendData.setKey(key);
        
        kafkaSendRepository.save(kafkaSendData); 
    }
    
    
    /**
     * 
     * @param topic
     * @param partition
     * @param message
     */
    public void sendMessageToPartition(String topic, int partition, String message) {
    	List<PartitionInfo> partitions = kafkaTemplate.partitionsFor(topic);
//    	int numPartitions = partitions.size();
//    	int partition = Math.abs(key.hashCode()) % numPartitions;	
        	
    	kafkaTemplate.send(topic, partition, null, message);
    	log.info("Sent message [%s] to topic [%s], partition [%d]%n", message, topic, partition);
    	
    	 KafkaSendData kafkaSendData  = new KafkaSendData();
         kafkaSendData.setSendData(message); 
         kafkaSendData.setBaseDt(formattedDate);
         kafkaSendData.setTopic(topic);
         kafkaSendData.setPartition(String.valueOf(partition));
         kafkaSendRepository.save(kafkaSendData); 
    	
    }
	
	
	
	

}
