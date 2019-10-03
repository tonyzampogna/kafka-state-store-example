package com.tonyzampogna.streams;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;


@SpringBootApplication(
		exclude = {
		    	// Excluding the KafkaAutoConfiguration allows the KafkaListenerContainerFactory
				// Spring Bean to be renamed something different than "kafkaListenerContainerFactory".
				//
				// This can also be added as the property:
				//    spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration
				//
				KafkaAutoConfiguration.class
		}
)
public class KafkaStateStoreApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaStateStoreApplication.class, args);
	}

}
