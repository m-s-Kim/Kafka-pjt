package com.test.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;


@Configuration
@EnableKafkaStreams
public class KafkaStreamConfig {
	
//	streamsBuilder.stream으로 KStream을 생성할 때, 두 번째 인자로 Consumed를 넘겨줌으로써 Key, Value 직렬화, 역직렬화를 설정할 수 있다.
//	Kafka Streams를 사용하기 위해서는 ApplicationId가 필요한데, ConsumerGroupId를 대체안으로 사용한다.
//	Kafka Stream과 연결할 Topic은 없을 시 자동으로 생성하지 않기 때문에 이전에 만들어야 한다.
//	.peek()를 사용하면 return 없이 다른 Topic으로 데이터를 전달한다.
	
	@Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapAddress;

	@Bean
    public KStream<String, Object> kStream(StreamsBuilder streamsBuilder) {
        KStream<String, Object> stream = streamsBuilder.stream("test-streams");
        
        stream.peek((key, value) -> System.out.println("[Stream] Message = " + value))  // 값을 출력하는 peek 연산
              .map((key, value) -> KeyValue.pair(key, "change message."))  // Key와 새로운 value를 반환하는 map 연산
              .to("test-streams-to");  // 처리한 메시지를 다른 토픽에 전송
        
        return stream;
    }
	
	@Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    KafkaStreamsConfiguration defaultKafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        // Kafka Streams의 애플리케이션 ID 설정
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-streams-id");
        // Kafka 클러스터의 bootstrap server 설정
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapAddress);
        // Serde 설정: 기본적으로 String Serde 사용
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return new KafkaStreamsConfiguration(props);
    }
}
