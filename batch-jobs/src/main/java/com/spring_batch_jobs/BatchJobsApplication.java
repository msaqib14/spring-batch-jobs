package com.spring_batch_jobs;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@EnableBatchProcessing
public class BatchJobsApplication {

	public static void main(String[] args) {
		SpringApplication.run(BatchJobsApplication.class, args);
	}

}
