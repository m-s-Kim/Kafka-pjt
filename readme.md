# Kafka

Kafka는 별도의 시작이나 끝이 없는 스트리밍 이벤트 데이터 또는 일반 데이터를 수집, 처리, 저장하는 데 널리 사용되는 이벤트 스트리밍 플랫폼입니다. Kafka는 차세대 분산 애플리케이션이 확장을 통해 스트리밍 이벤트를 분당 수십억 개까지 처리할 수 있도록 합니다.

Kafka와 Google Cloud Pub/Sub 같은 이벤트 스트리밍 시스템이 등장하기 전의 데이터 처리는 일반적으로 원시 데이터를 먼저 저장했다가 나중에 임의의 시간 간격으로 처리하는 주기적인 일괄 작업으로 다뤄져 왔습니다.     
일괄 처리의 한계 중 하나는 실시간이 아니라는 점입니다. 적시에 비즈니스 결정을 내리고 흥미로운 일이 발생할 경우 조치를 취하기 위해 데이터를 실시간으로 분석하고자 하는 조직은 점점 많아지고 있습니다. 예를 들어 앞서 언급한 통신 회사에서는 전반적인 고객 경험을 향상시킬 한 가지 방법으로 고객에게 실시간 요금을 알려주는 것이 도움이 될 수 있습니다.

이벤트가 생성되는 대로 이벤트의 무한 스트림을 지속적으로 처리하는 프로세스입니다. 이벤트 스트리밍의 예로는 고객 대면 웹 애플리케이션에서 생성되는 로그 파일을 지속적으로 분석하는 것, 사용자가 전자상거래 웹사이트를 탐색할 때 고객 행동을 모니터링하고 그에 대응하는 것, 소셜 네트워크에서 생성되는 클릭 스트림 데이터의 변화를 분석하여 고객 감정에 지속적으로 영향을 미치는 것, IoT 기기에서 생성되는 원격 분석 데이터를 수집하고 그에 대응하는 것 등이 포함됩니다.



## Kafka 도입 전 구조 

![image](https://github.com/user-attachments/assets/9ea4ea90-1443-47ab-a0e7-cfa1d357eca4)

Linked in 이 직면한 기존의 end-to-end 통신 방식의 문제점

- 시스템 복잡도(Complexity)의 증가
  1. 통합된 전송 영역이 없어 데이터 흐름을 파악하기 어렵고, 시스템 관리가 어려움
  2. 특정 부분에서 장애 발생 시 조치 시간 증가 (연결 되어있는 Application을 모두 확인해야 했기 때문)
  3. HW 교체 혹은 SW 업그레이드 시 관리 포인트가 늘어나고, 작업시간의 증가(연결되어 있는 Application에 Side Effect 가 없는지 확인 필요)

- 데이터 파이프라인 관리의 어려움
 1. 각 애플리케이션과 데이터 시스템 간 별도의 파이프라인이 존재하고, 파이프라인마다 데이터 포맷과 처리 방식이 다름
 2. 새로운 파이프라인 확장이 어려워지면서, 확장성 및 유연성이 떨어짐
 3. 데이터 불일치 가능성이 존재했기 때문에 신뢰도 감소


## Kafka 도입 후 구조

![image](https://github.com/user-attachments/assets/484ee9e6-c4b6-497f-ac1b-72742dea4567)

Kafka 도입 후 개선점
1. 기존 end-to-end 통신 방식에서 벗어나 통합/중앙화된 전송 영역을 설계.
2. 메세지를 생성하는 Producer 와 Consumer 을 분리.
3. 대용량 처리 및 빠른 처리 
4. 확장(Scale-out) 이 용이하게 설계

모든 시스템으로 데이터를 전송할 수 있고, 실시간 처리도 가능하며, 급속도로 성장하는 서비스를 위해 확장이 용이한 시스템을 구현 할수 있었습니다.

## Kafka 작동방식
카프카는 고성능 TCP 네트워크 프로토콜을 통해 통신하는 서버 와 클라이언트 로 구성된 분산 시스템입니다. 
온프레미스와 클라우드 환경의 베어 메탈 하드웨어, 가상 머신 및 컨테이너에 배포할 수 있습니다.

서버 : Kafka는 여러 데이터 센터 또는 클라우드 지역에 걸쳐 있는 하나 이상의 서버 클러스터로 실행됩니다. 이러한 서버 중 일부는 브로커라고 하는 스토리지 계층을 형성합니다. 다른 서버는 Kafka Connect를 실행하여 데이터를 이벤트 스트림으로 지속적으로 가져오고 내보내 Kafka를 RDB 및 다른 Kafka 클러스터와 같은 기존 시스템과 통합합니다. 미션 크리티컬(정상적으로 작동되지 않거나 파괴되면 업무 수행 전체에 치명적인 영향을 미쳐 조직이나 사회에 재앙을 가져올 수 있는 특성 또는 상태) 사용 사례를 구현할 수 있도록 Kafka 클러스터는 확장성이 뛰어나고 내결함성이 있습니다. 서버 중 하나가 실패하면 다른 서버가 작업을 인계하여 데이터 손실 없이 지속적인 운영을 보장합니다.

클라이언트 : 네트워크 문제나 머신 장애가 발생하더라도 병렬로, 대규모로, 내결함성 방식으로 이벤트 스트림을 읽고, 쓰고, 처리하는 분산 애플리케이션과 마이크로서비스를 작성할 수 있습니다. Kafka에는 이러한 클라이언트가 일부 포함되어 있으며, Kafka 커뮤니티에서 제공하는 수십 개의 클라이언트 로 보강됩니다 . 클라이언트는 Java 및 Scala, 상위 수준 Kafka Streams 라이브러리, Go, Python, C/C++ 및 기타 여러 프로그래밍 언어와 REST API를 위해 제공됩니다.


## Kafka Architecture
![image](https://github.com/user-attachments/assets/d6e14dd4-1782-4f1b-86d6-102aeca95764)


Cluster : Kafka 클러스터는 여러 대의 서버(브로커)로 구성된 Kafka 시스템이다. 클러스터는 대량의 데이터를 처리하고, 여러 소비자(Consumer)와 생산자(Producer)에게 메시지 서비스를 제공한다.
클러스터는 메시지의 저장, 처리 및 전달을 담당한다. 클러스터는 고가용성과 확장성을 제공하며, 데이터를 여러 브로커에 분산시켜 저장한다.

Broker : 생산자와 소비자와의 중재자 역할을 하는 역할

Topic : 토픽은 생산자(Producer)가 데이터를 보내는 대상이며, 소비자(Consumer)가 데이터를 읽는 출처다.

Producer : 데이터를 Kafka 클러스터로 전송하는 역할을 하는 클라이언트 애플리케이션이다

Consumer : Kafka 클러스터에서 데이터를 읽는 역할을 하며 
컨슈머는 리더 파티션을 통해서만 데이터를 읽습니다. 팔로워 파티션은 복제용이며 고가용성을 위한것입니다.

Partition : 파티션은 큐를 나눠서 병렬 처리를 가능하게 하는 기본 단위이며, Kafka의 각 토픽(topic)은 하나 이상의 파티션으로 나눠져 있다.       
 - 데이터 분산 및 병렬 처리 : 각 파티션은 독립적으로 데이터를 저장하고, 여러 브로커에 걸쳐 분산될 수 있다. 이를 통해 Kafka는 데이터를 효율적으로 관리하고, 동시에 여러 
소비자에게 서비스할 수 있다.          
 - 순차적 데이터 관리 : 각 파티션 내에서 메시지는 순차적으로 저장되며, 이 순서는 파티션 내에서 유지된다. 이는 데이터의 일관성과 정확한 순서 보장에 중요하다.    
 - 스케일 아웃 : 시스템의 부하가 증가할 때, 더 많은 파티션을 추가하여 처리 능력을 확장할 수 있다.    

출처 : https://ggop-n.tistory.com/91


![image](https://github.com/user-attachments/assets/eac5db86-81ff-494f-88b1-c83e0157b4f4)

데이터 저장 단위 : 세그먼트   
Producer에 의해서 브로커로 전송된 메시지는 토픽의 파티션에 저장되며 
각 메시지들은 세그먼트라는 로그 파일의 형태로 브로커의 로컬 디스크에 저장됩니다.
각 세그먼트 파일은 일정 크기에 도달하거나 특정 시간이 경과되면 새로운 세그먼트 파일로 전환 된다.
  
출처 : https://medium.com/@greg.shiny82/apache-kafka-%EA%B0%84%EB%9E%B5%ED%95%98%EA%B2%8C-%EC%82%B4%ED%8E%B4%EB%B3%B4%EA%B8%B0-343ad84a959b



## Flow

![image](https://github.com/user-attachments/assets/37036228-fd03-4e26-beb0-249f073acd9a)

buffer.memory : kafka producer가 메시지를 보내기 전 메모리에 보관하는 전체 메시지 용량을 의미합니다. (Default : 32MB)
max.request.size : 단일 요청으로 보낼 수 있는 최대 메시지의 크기를 의미합니다. (default : 1MB)
batch.size : 단일 배치 요청으로 단일 파티션에 요청을 보낼 수 있는 메시지들의 크기의 최대 합을 의미합니다.
linger.ms : producer가 batch.size에 도달하기까지 기다릴 수 있는 시간입니다.

request가 만들어지는 절차

1. message가 발행된다.

2. message들의 합이 batch.size가 될 떄 까지 기다린다.

3. linger.ms보다 먼저 batch.size를 넘어 설 경우, batch.size 크기로 batch를 묶는다.

4. linger.ms에 먼저 도달할 경우, 도달 전까지의 데이터만 batch 단위로 묶는다.

5. max.request.size보다 message 크기가 작은 경우만 전송이 완료된다.


![image](https://github.com/user-attachments/assets/c27d0daf-788d-41d1-a0d6-3bd7f59cc42d)

Fetcher
poll이 실행될 때, 적절한 크기의 record를 client에 return 하기 위해 Kafka Cluster로부터 record를 요청하고, client가 받기 직전에 메모리에 미리 저장하는 역할을 합니다.

Fetcher 성능에 영향을 주는 Configuration
fetch.min.byte ~ fetch.max.byte : broker에서 가져올 최소/최대 데이터 크기를 결정합니다. max.partition.fetch.bytes : 하나의 partition에서 가져올 데이터의 최대 크기를 결정합니다. fetch.max.wait.ms : fetch.min.byte에 도달하기까지 기다릴 수 있는 시간을 의미합니다. 이 시간이 지나도 min.byte에 도달하지 못하면 도달하지 못한 데이터 크기를 보냅니다.

coordinator
Kafka Cluster 내부의 coordinator(zookeeper, kraft)와 통신하여 어떻게 데이터를 consume할지, offset commit, consumer group, heartbeat 기능 등을 수행합니다.

poll configuration
데이터를 poll 할 시, 아래와 같은 설정값에 의해 동작이 달라질 수 있습니다. max.poll.records(default : 500)에 의해 fetcher로부터 가져 올 record 수가 정해진다. max.poll.interval.ms : poll 요청을 받은 메시지를 처리할 때 까지 최대 기다릴 수 있는 시간. 이 시간이 초과한다면, 해당 consumer는 rebalancing 될 때 consumer group에서 제외대상이 된다.

참조 : 

https://kafka.apache.org/20/documentation.html#introduction

https://medium.com/mo-zza/kafka-kraft-%EB%AA%A8%EB%93%9C-with-docker-%EB%8F%99%EB%AC%BC%EC%9B%90%EC%9D%84-%ED%83%88%EC%B6%9C%ED%95%9C-kafka-8b5e7c7632fa

https://oliveyoung.tech/2024-10-16/oliveyoung-scm-oms-kafka/

https://devocean.sk.com/blog/techBoardDetail.do?ID=165711&boardType=techBlog#none

https://kafka.apache.org/intro

https://cloud.google.com/learn/what-is-apache-kafka?hl=ko
https://curiousjinan.tistory.com/entry/understanding-kafka-all-structure
