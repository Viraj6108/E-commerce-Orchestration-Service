package com.webdev.ws.service;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.webdev.ws.commands.PaymentProceedCommand;
import com.webdev.ws.commands.ProductReserveCommand;
import com.webdev.ws.errors.RetryableException;
import com.webdev.ws.events.OrderCreatedEvent;
import com.webdev.ws.events.PaymentProcessEvent;

@KafkaListener(topics = {"order-event","product-event"} ,groupId = "orchestration-group")
@Component
public class HandlerkafkaEvents {

    

	Logger logger = LoggerFactory.getLogger(HandlerkafkaEvents.class);
	@Value("${product.reserve.command}")
	private String TOPIC_NAME;
	
	@Value("${payment.process.command}")
	private String PAYMENT_TOPIC;
	
	private KafkaTemplate<String,Object>kafkaTemplate;
	
	
	  public
	  HandlerkafkaEvents(KafkaTemplate<String,Object>kafkaTemplate)
	  { this.kafkaTemplate = kafkaTemplate; 
	  }
	 
	@KafkaHandler
	public void handle(@Payload OrderCreatedEvent event) throws InterruptedException, ExecutionException
	{
		
		  ProductReserveCommand reserve = new ProductReserveCommand();
		  reserve.setProductId(event.getProductId());
		  reserve.setQuantity(event.getQuantity());
		  reserve.setOrderId(event.getOrderId());
		 
		  ProducerRecord<String,Object> records = new ProducerRecord<String, Object>(TOPIC_NAME,event.getProductId().toString(), reserve);
		  SendResult<String, Object> result = kafkaTemplate.send(records).get();
		  logger.info("Topic partition" +result.getRecordMetadata().partition());
		  logger.info("TOpic Name"+result.getRecordMetadata().topic());
		  logger.info("Event sent"+reserve.getProductId().toString()+" "+
		  TOPIC_NAME+" "+reserve);
		 
	}
	
	/*
	 * @KafkaHandler public void handleProductReserve(@Payload ProductReserveCommand
	 * command) { logger.error("At product reserve handler");
	 * System.err.print("ProductReserveCommand" + command.getQuantity() + "" +
	 * command.getProductId()); }
	 */
	 
	//handler payment related commands
	@KafkaHandler()
	public void handleProductReservedEvent(@Payload PaymentProceedCommand payment ) throws InterruptedException, ExecutionException
	{
		try {
		PaymentProcessEvent paymentEvent = new PaymentProcessEvent(payment.getProductId(),payment.getOrderId()
				,payment.getQuantity(),payment.getPrice());
		ProducerRecord<String,Object> records = new ProducerRecord<String, Object>(PAYMENT_TOPIC, UUID.randomUUID().toString(),paymentEvent);
		SendResult<String, Object> result = kafkaTemplate.send(records).get();
		
		logger.info("Payment process event received at orchestration service");
		logger.info("topic name : "+result.getRecordMetadata().topic());
		logger.info("topic partition :"+ result.getRecordMetadata().partition());
		}catch(RetryableException e)
		{
			logger.error("Error occured while sending message to topic");
			throw new RetryableException("Retrying exception ");
			
		}
		
	}
}
