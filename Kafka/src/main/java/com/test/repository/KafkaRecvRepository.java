package com.test.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import com.test.entity.KafkaRecvData;

public interface KafkaRecvRepository extends JpaRepository<KafkaRecvData, Long> {
	
	
}