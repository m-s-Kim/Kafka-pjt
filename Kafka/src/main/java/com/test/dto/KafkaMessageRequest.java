package com.test.dto;

import java.io.Serializable;
import java.util.Map;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class KafkaMessageRequest  {
	
	private String text;
    private String topic;
    private String partition;
    private String key;
    
    private Map<String, Object> msg; 
    
}
