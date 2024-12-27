package com.test.service;

import java.io.FileReader;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.opencsv.CSVReader;
import com.test.dto.KafkaMessageRequest;
import com.test.entity.KafkaRecvData;
import com.test.entity.KafkaSendData;
import com.test.repository.KafkaRecvRepository;
import com.test.repository.KafkaSendRepository;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service
public class KafkaService {
	private final KafkaTemplate<String, String> kafkaTemplate;
	
	private final KafkaSendRepository kafkaSendRepository;
	private final KafkaRecvRepository kafkaRecvRepository;
	
	@Value("${server.port}")
	private String port;
	
	private LocalDate today = LocalDate.now();
	private DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
	private String formattedDate = today.format(formatter);
	
	
	public KafkaService(KafkaTemplate<String, String> kafkaTemplate, KafkaSendRepository kafkaSendRepository, KafkaRecvRepository kafkaRecvRepository) {
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaSendRepository = kafkaSendRepository;
        this.kafkaRecvRepository = kafkaRecvRepository;
    }

	/**
	 * 
	 * @param topic
	 * @param message
	 */
	@Transactional
    public void sendMessage(String topic, String message) {
        kafkaTemplate.send(topic, message);
        
        KafkaSendData kafkaSendData = new KafkaSendData();
        kafkaSendData.setSendData(message);
        kafkaSendData.setBaseDt(formattedDate);
        kafkaSendData.setTopic(topic);
        // 필요한 필드 설정 후 데이터베이스에 저장
        kafkaSendRepository.save(kafkaSendData);
    }
    
    /**
	 * 
	 * @param topic
	 * @param message
	 */
    @Transactional
    public void sendMultiMessage(KafkaMessageRequest request) {
    	
    	kafkaTemplate.send(request.getTopic(), request.getText());
         // 데이터베이스에 저장
         LocalDate today = LocalDate.now();
         DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMdd");
         String formattedDate = today.format(formatter);

         KafkaSendData kafkaSendData = new KafkaSendData();
         kafkaSendData.setSendData(request.getText().toString()); // text 객체를 String으로 변환하여 저장
         kafkaSendData.setBaseDt(formattedDate);
         kafkaSendData.setTopic(request.getTopic()); 

         kafkaSendRepository.save(kafkaSendData); 
    }
    
    
    /**
     * 
     * @param record
     */
    @KafkaListener(topics = "test-topic", groupId = "my-group2")
    public void listen(ConsumerRecord<String, String> record) {
        insertKafkaMsg(record);
    }
    
    @KafkaListener(topics = "test-topic", groupId = "my-group")
    public void listentest(ConsumerRecord<String, String> record) {
    	insertKafkaMsg(record);
    }
    
    /**
     * pub , sub 모든 메시지 조회
     * @return
     */
    public List<Object> getAllMessages() {
        // 데이터베이스에서 모든 메시지 조회
    	List<Object> msg = new ArrayList<>();
    	
    	List<KafkaSendData> send = kafkaSendRepository.findAll();  
    	List<KafkaRecvData> recv = kafkaRecvRepository.findAll();
    	
    	msg.addAll(send);
    	msg.addAll(recv);
    	
        return msg;
    }
    
    
    @Transactional
    public void sendCsvToKafka(String topic) {
    	String filePath = "C:\\workspace\\Kafka\\data\\sample.csv";
    	String[] nextLine;
    	
        try (CSVReader csvReader = new CSVReader(new FileReader(filePath))) {
            
            KafkaSendData kafkaSendData = null;
            
            while ((nextLine = csvReader.readNext()) != null) {
            	
            	kafkaSendData = new KafkaSendData();
                String message = Arrays.toString(nextLine);
                kafkaTemplate.send(topic, message);
                
                log.info("Sent message to Kafka: " + message);
                
                kafkaSendData.setSendData(message); 
                kafkaSendData.setBaseDt(formattedDate);
                kafkaSendData.setTopic(topic); 
                
                kafkaSendRepository.save(kafkaSendData); // DB에 저장
                
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    
    @KafkaListener(topics = "topic1", groupId = "group1")
    public void listenToTopic(ConsumerRecord<String, Object> record) {
        String key = record.key();
        String value = record.value().toString();

        KafkaRecvData kafkaRecvData = new KafkaRecvData();
        kafkaRecvData.setSendData(record.value().toString());
        kafkaRecvData.setBaseDt(formattedDate);
        kafkaRecvData.setTopic(record.topic());
        kafkaRecvData.setTarget(port);
        kafkaRecvData.setKey(key);
        kafkaRecvRepository.save(kafkaRecvData);
        
        // 특정 키로 들어왔을시 처리로직
        if ("key".equals(key)) {
//            System.out.println("Handling specific key: " + key);
        } else {
//            System.out.println("Other Key: " + key);
        }
    }
    
    @KafkaListener(topics = "topic1", groupId = "group2")
    public void listenToTopic2(ConsumerRecord<String, Object> record) {
        String key = record.key();
        String value = record.value().toString();

        KafkaRecvData kafkaRecvData = new KafkaRecvData();
        kafkaRecvData.setSendData(record.value().toString());
        kafkaRecvData.setBaseDt(formattedDate);
        kafkaRecvData.setTopic(record.topic());
        kafkaRecvData.setTarget(port);
        kafkaRecvData.setKey(key);
        kafkaRecvRepository.save(kafkaRecvData);
        
        
        if ("key".equals(key)) {
//            System.out.println("Handling specific key: " + key);
        } else {
//            System.out.println("Other Key: " + key);
        }
    }
    
    /**
     * 메시지 전송이력 키 기준으로 조회
     * @param key
     * @return
     */
    public Optional<KafkaSendData> getByKey(String key) {
        return kafkaSendRepository.findByKey(key);
    }
    
    
    private void insertKafkaMsg(ConsumerRecord<String, String> record) {
    	
    	String message = record.value(); 
        String topic   = record.topic();
        String key     = record.key();
        
        log.info("topic : "+topic +" Received : " + message);
        
    	KafkaRecvData kafkaRecvData = new KafkaRecvData();
        kafkaRecvData.setSendData(message);
        kafkaRecvData.setBaseDt(formattedDate);
        kafkaRecvData.setTopic(topic);
        kafkaRecvData.setTarget(port);
        kafkaRecvData.setKey(key);
        kafkaRecvRepository.save(kafkaRecvData);
    }
    
    
    
}
