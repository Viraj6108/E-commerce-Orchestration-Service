package com.webdev.ws.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

import com.webdev.ws.errors.NotRetryableException;
import com.webdev.ws.errors.RetryableException;

@Configuration
public class KafkaConfiguration {
	
	@Value("${spring.kafka.producer.properties.in.flight.requests.per.connection}")
	private int flightConnection;
	
	@Value("${spring.kafka.producer.properties.delivery.timeout.ms}")
	private int deliveryTimeout;
	
	@Value("${spring.kafka.producer.properties.linger.ms}")
	private int lingerMs;
	
	@Value("${spring.kafka.producer.properties.request.timeout.ms}")
	private int requestTimeOut;
	@Autowired
	Environment env;
	
	@Bean
	ProducerFactory<String, Object> producerFactory() {
		Map<String, Object> config = new HashMap<>();
		config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.producer.bootstrap-servers"));
		config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
		config.put(ProducerConfig.ACKS_CONFIG, env.getProperty("spring.kafka.producer.acks"));
		config.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,
				env.getProperty("spring.kafka.producer.properties.enable.idempotence"));
		config.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,
				env.getProperty("spring.kafka.producer.properties.in.flight.requests.per.connection"));
		config.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG,
				env.getProperty("spring.kafka.producer.properties.delivery.timeout.ms"));
		config.put(ProducerConfig.LINGER_MS_CONFIG, env.getProperty("spring.kafka.producer.properties.linger.ms"));
		config.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,
				env.getProperty("spring.kafka.producer.properties.request.timeout.ms"));
		return new DefaultKafkaProducerFactory<>(config);
	}

	@Bean
	KafkaTemplate<String, Object> kafkaTemplate() {
		return new KafkaTemplate<>(producerFactory());
	}
	
	@Bean
	ConsumerFactory<String,Object>consumerFactory()
	{
		
	
	
		Map<String,Object> config = new HashMap<>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, env.getProperty("spring.kafka.consumer.bootstrap-servers"));
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ErrorHandlingDeserializer.class);
		config.put(ErrorHandlingDeserializer.VALUE_DESERIALIZER_CLASS, JsonDeserializer.class);
		config.put(JsonDeserializer.TRUSTED_PACKAGES, env.getProperty("spring.kafka.consumer.properties.spring.json.trusted-packages"));
		config.put(ConsumerConfig.GROUP_ID_CONFIG,env.getProperty("consumer.group.id"));
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, env.getProperty("spring.kafka.consumer.auto-offset-reset"));
		config.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
		return new DefaultKafkaConsumerFactory<>(config);	
	
	}

	@Bean
	ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
			ConsumerFactory<String, Object> consumerFactory, KafkaTemplate<String, Object> kafkaTemplate) {
		DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate);
		DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer,
				new FixedBackOff(5000L,3));
		
		errorHandler.addNotRetryableExceptions(NotRetryableException.class);
		errorHandler.addRetryableExceptions(RetryableException.class);

		ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();
		factory.setConsumerFactory(consumerFactory);
		factory.setCommonErrorHandler(errorHandler);
		
		return factory;
	}
	
	@Bean
	NewTopic orderCommand()
	{
		return TopicBuilder
				.name("order-command")
				.replicas(2)
				.partitions(3)
				.build();
		}
}
