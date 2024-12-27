package com.test.entity;

import java.time.LocalDateTime;

import javax.annotation.Nullable;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.PrePersist;
import javax.persistence.PreUpdate;
import javax.persistence.Table;

import org.hibernate.annotations.GeneratorType;
import org.springframework.data.annotation.CreatedDate;
import org.springframework.data.annotation.LastModifiedDate;

import lombok.Getter;
import lombok.Setter;

@Entity
@Table(name = "KAF_SEND_HIS")
@Getter
@Setter
public class KafkaSendData {
	

	@Id
	@GeneratedValue(strategy = GenerationType.IDENTITY)
	private long ID;
	
	private String msgId;
	
	@Column(nullable = false)
	private String baseDt;
	
	private String topic;
	
	private String partition;
	
	private String key;
	
	@Column(name = "offset_val")
	private String offset;
	
	@Column(nullable = false)
	private String sendData;
	
    @CreatedDate
    @Column(insertable = true)
    private LocalDateTime createdDate;
    
    @LastModifiedDate
    private LocalDateTime updatedDate;
    
    @PrePersist
    protected void onCreate() {
    	LocalDateTime now = LocalDateTime.now();
        this.createdDate = now;
        this.updatedDate = now; 
    }

    @PreUpdate
    protected void onUpdate() {
        this.updatedDate = LocalDateTime.now();
    }
}
