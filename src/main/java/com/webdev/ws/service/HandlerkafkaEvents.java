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

import com.webdev.ws.commands.OrderStatusFailedCommand;
import com.webdev.ws.commands.OrderStatusSuccessCommand;
import com.webdev.ws.commands.PaymentProceedCommand;
import com.webdev.ws.commands.ProductReserveCommand;
import com.webdev.ws.errors.RetryableException;
import com.webdev.ws.events.OrderCreatedEvent;
import com.webdev.ws.events.OrderFailedEvent;
import com.webdev.ws.events.PaymentProcessEvent;
import com.webdev.ws.events.ProductReservationFailedEvent;

@KafkaListener(topics = {"order-event","product-event","payment-event"} ,groupId = "orchestration-group")
@Component
public class HandlerkafkaEvents {

    

	Logger logger = LoggerFactory.getLogger(HandlerkafkaEvents.class);
	@Value("${product.reserve.command}")
	private String TOPIC_NAME;
	
	@Value("${payment.process.command}")
	private String PAYMENT_TOPIC;
	
	@Value("${order.failed.event}")
	private String Failed_Topic;
	
	private KafkaTemplate<String,Object>kafkaTemplate;
	
	
	  public
	  HandlerkafkaEvents(KafkaTemplate<String,Object>kafkaTemplate)
	  { this.kafkaTemplate = kafkaTemplate; 
	  }
	 //After order is created 
	@KafkaHandler
	public void handle(@Payload OrderCreatedEvent event) throws InterruptedException, ExecutionException
	{
		try {
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
		}catch(Exception e)
		{
			OrderFailedEvent failedEvent = new OrderFailedEvent(event.getOrderId(),event.getProductId());
			kafkaTemplate.send(Failed_Topic,failedEvent);
		}
		 
	}
	
	 
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
	
	//ProductReservations is failed due to unavailability of product quantity
		@KafkaHandler
		public void handleProductReservationFailed(@Payload ProductReservationFailedEvent event)throws Exception
		{
			try {
			OrderStatusFailedCommand command = new OrderStatusFailedCommand(event.getOrderId(),event.getProductId());
			ProducerRecord<String,Object>records=new ProducerRecord<String, Object>(Failed_Topic,null, command);
				SendResult<String, Object>result=	kafkaTemplate.send(records).get();
			logger.info("Sent to topic "+result.getRecordMetadata().topic());
			}catch(Exception e)
			{
				throw new Exception("Exception Occured"+e.getLocalizedMessage());
			}
		}
		
		//CHange state of order after successful payment 
		@KafkaHandler
		public void handleSuccessOrderStatus(@Payload OrderStatusSuccessCommand command)
		{
			OrderStatusSuccessCommand Ordercommand = new OrderStatusSuccessCommand(command.getOrderId());
			kafkaTemplate.send("order-command",null,Ordercommand);
		}
		
		
		@KafkaHandler(isDefault = true)
		public void defaultHandler(Object object)
		{
			System.err.print("Unknown Object"+object.getClass());
		}
		
		
	
}
