package com.test.controller;

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import com.test.dto.KafkaMessageRequest;
import com.test.dto.ResponseDto;
import com.test.service.KafkaPartitionOrKeyService;
import com.test.service.KafkaService;

@RestController
@RequestMapping("/kafka")
public class KafkaController {
	
	private final KafkaService kafkaService;
	private final KafkaPartitionOrKeyService kafkaPartitionService;
	
	@Autowired
	public KafkaController(KafkaService kafkaService, KafkaPartitionOrKeyService kafkaPartitionService) {
		this.kafkaService = kafkaService;
		this.kafkaPartitionService = kafkaPartitionService;
	}
	
	/**
	 * topic 에 데이터 전송하기
	 * @param request
	 * @return
	 */
	@PostMapping("/send")
	public ResponseEntity<ResponseDto> send(@RequestBody KafkaMessageRequest request){
		String topic = (request.getTopic() != null) ? request.getTopic() : "test-topic";
	    String text = request.getText();
	    
		kafkaService.sendMessage(topic, text);
		ResponseDto dto = new ResponseDto();
		dto.setData(text);
		return ResponseEntity.ok(dto);
	}
	
	/**
	 * 모든 메시지 조회
	 * @return
	 */
	@GetMapping("/messages")
    public ResponseEntity<List<Object>> getAllMessages() {
        List<Object> messages = kafkaService.getAllMessages();  
        return ResponseEntity.ok(messages);  
    }
	
	
	@GetMapping("/{id}/message")
	public ResponseEntity<?> writeArticle(@PathVariable String id){
		return ResponseEntity.ok(kafkaService.getByKey(id));
	}

	
	
	
    /**
     * csv 파일 읽어서 프로듀서에 보내기
     * @param topic
     */
	@PostMapping("/send-csv")
	public void sendCSV(@RequestParam String topic) {
		kafkaService.sendCsvToKafka(topic);
	}
	
	
	
	@PostMapping("/send-partition")
	public void sendPartitionMsg(
		    @RequestBody(required = false) KafkaMessageRequest jsonRequest,
		    @RequestParam(required = false) String text,
		    @RequestParam(required = false) String topic,
		    @RequestParam(required = false) String partition
		    ) {
		    
		
	    String resolvedText = (jsonRequest != null) ? jsonRequest.getText() : text;
	    String resolvedTopic = (jsonRequest != null) ? jsonRequest.getTopic() : topic;
	    String resolvedPartition = (jsonRequest != null) ? jsonRequest.getPartition() : partition;
	    if (resolvedText == null || resolvedTopic == null) {
	        throw new IllegalArgumentException("Invalid request data");
	    }

	    
	    int chgTypePartition = Integer.parseInt(resolvedPartition);
	    
	    kafkaPartitionService.sendMessageToPartition(resolvedTopic, chgTypePartition, resolvedText);
	}
	
	/**
	 * 특정 토픽 및 특정 키값에 메시지 보내기
	 * 키가 없을 경우 임의로 채번
	 * @param request
	 * @return
	 */
	@PostMapping("/send-with-key")
    public ResponseEntity<KafkaMessageRequest> sendWithKey(@RequestBody KafkaMessageRequest request) {
		
		long timestamp = System.currentTimeMillis();
		
		String uuid = UUID.randomUUID().toString();
		String msgKey = timestamp + "-" + uuid;
		
		if(request.getKey() != null  && !request.getKey().equals("")) {
			msgKey = request.getKey();
		}
		
		kafkaPartitionService.sendMessageWithKey(request.getTopic(), msgKey, request.getText());
		return ResponseEntity.ok(request);
    }
}
