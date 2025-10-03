package com.webdev.ws.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.webdev.ws.commands.ProductReserveCommand;
import com.webdev.ws.errors.RetryableException;
import com.webdev.ws.events.OrderCreatedEvent;

@KafkaListener(topics = {"order-event"} ,groupId = "orchestration-group")
@Component
public class HandlerkafkaEvents {

	Logger logger = LoggerFactory.getLogger(HandlerkafkaEvents.class);
	@Value("${product.reserve.command}")
	private String TOPIC_NAME;
	
	private KafkaTemplate<String,Object>kafkaTemplate;
	
	
	  public
	  HandlerkafkaEvents(KafkaTemplate<String,Object>kafkaTemplate)
	  { this.kafkaTemplate = kafkaTemplate; }
	 
	@KafkaHandler
	public void handle(@Payload OrderCreatedEvent event)
	{
		
		  ProductReserveCommand reserve = new ProductReserveCommand();
		  reserve.setProductId(event.getProductId());
		  reserve.setQuantity(event.getQuantity());
		  kafkaTemplate.send(TOPIC_NAME,reserve.getProductId().toString(),reserve);
		  logger.info("Event sent"+reserve.getProductId().toString()+" "+
		  TOPIC_NAME+" "+reserve);
		 
	}
	/*
	 * @KafkaHandler public void handleProductReserve(@Payload ProductReserveCommand
	 * command) { logger.error("At product reserve handler");
	 * System.err.print("ProductReserveCommand"+command.getQuantity()+""+
	 * command.getProductId()); }
	 */
}
