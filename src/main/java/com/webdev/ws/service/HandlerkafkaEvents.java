package com.webdev.ws.service;

import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import com.webdev.ws.commands.OrderReservationFailedCommand;
import com.webdev.ws.commands.OrderStatusCommand;
import com.webdev.ws.commands.PaymentProceedCommand;
import com.webdev.ws.commands.ProductReserveCommand;
import com.webdev.ws.events.OrderCreatedEvent;
import com.webdev.ws.events.PaymentFailedEvent;
import com.webdev.ws.events.PaymentProcessEvent;
import com.webdev.ws.events.PaymentSuccessfulEvent;
import com.webdev.ws.events.ProductReservationFailedEvent;

@KafkaListener(topics = {"order-event","product-event","payment-event"} ,groupId = "orchestration-group")
@Component
public class HandlerkafkaEvents {

	Logger logger = LoggerFactory.getLogger(HandlerkafkaEvents.class);
	@Value("${product.reserve.command}")
	private String TOPIC_NAME;
	
	@Value("${payment.process.command}")
	String PAYMENT_COMMAND;
	
	@Value("${product.order-status.command}")
	private String ORDER_COMMAND;
	
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
		  reserve.setOrderId(event.getOrderId());
		  kafkaTemplate.send(TOPIC_NAME,UUID.randomUUID().toString(),reserve);
		  logger.info("Event sent"+reserve.getProductId().toString()+" "+
		  TOPIC_NAME+" "+reserve);
		 
	}
	
	@KafkaHandler
	public void handle(@Payload PaymentProceedCommand command)
	{
		logger.info("Received response from Product reservation successfully");
		PaymentProcessEvent event = new PaymentProcessEvent(command.getProductId(),command.getOrderId()
				,command.getQuantity(),command.getPrice());
		kafkaTemplate.send(PAYMENT_COMMAND,event);
		logger.info("Command sent to "+PAYMENT_COMMAND);
	}
	
	 @KafkaHandler 
	public void handler(@Payload PaymentSuccessfulEvent successEvent)
	{
		logger.info("Payment Successful . Sending command to change order status ");
		OrderStatusCommand command = new OrderStatusCommand(successEvent.getOrderId(),UUID.randomUUID());
		kafkaTemplate.send(ORDER_COMMAND,command);
		
	}

	 
/*-----------------------------------------failure Handlers------------------------------------------*/
/**
 * It will handle the product Reservation failed event. 
 * Order created -> product Reservation failed (product insufficient) 
 *  -> ProductReservationFailedEvent -> OrderReservationFailedCommand
 */
@KafkaHandler
	 public void handleFailure(@Payload ProductReservationFailedEvent event)
	 {
		 logger.info("Product reservation failed due to product un availability");
		 OrderReservationFailedCommand command = new OrderReservationFailedCommand(event.getOrderId());
		 kafkaTemplate.send(ORDER_COMMAND,command);
		 logger.info("To set order status to failed");
	 }
	 
	
	// this is to handle payment failed event 
	 public void handleFailure(@Payload PaymentFailedEvent event)
	 {
		 logger.info("payment is failed revert product quantity");
		 // Revert product quantity in product and after its reverted then send another 
		 //event handle it in orchestration service and send a command to set the order status to FAILED
		 
		 
	 }
	 
	 
}
