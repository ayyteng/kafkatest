package com.test.kafkademo;


import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.support.SendResult;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;


@SpringBootApplication
public class KafkaDemoApplication {

    public static void main(String[] args) throws Exception {

        ConfigurableApplicationContext context = SpringApplication.run(KafkaDemoApplication.class, args);

        MessageProducer producer = context.getBean(MessageProducer.class);
        MessageListener listener = context.getBean(MessageListener.class);
        
        //Sending a Hello World message to topic 'topic1' - defined in app properties. 
      
        producer.sendMessage("Hello, World!");
        listener.latch.await(10, TimeUnit.SECONDS);

        
        //Sending message to a topic with 5 partitions,
        //each message to a different partition. But as per
        //listener configuration, only the messages from
        //partition 0 and 3 will be consumed.
        
        for (int i = 0; i < 5; i++) {
            producer.sendMessageToPartition("Hello To Partitioned Topic!", i);
        }
        listener.partitionLatch.await(10, TimeUnit.SECONDS);

        //filtered topic
        producer.sendMessageToFiltered("Hello Test!");
        producer.sendMessageToFiltered("Hello World!");
        listener.filterLatch.await(10, TimeUnit.SECONDS);

		//needs testing
        // producer.sendGreetingMessage(new TestObject("Greetings", "World!"));
        // listener.testObjectLatch.await(10, TimeUnit.SECONDS);

        context.close();
    }

    @Bean
    public MessageProducer messageProducer() {
        return new MessageProducer();
    }

    @Bean
    public MessageListener messageListener() {
        return new MessageListener();
    }

    public static class MessageProducer {

        @Autowired
        private KafkaTemplate<String, String> kafkaTemplate;

        // @Autowired
        // private KafkaTemplate<String, TestObject> testObjectKafkaTemplate;

        @Value(value = "${message.topic.name}")
        private String topicName;

        @Value(value = "${partitioned.topic.name}")
        private String partitionedTopicName;

        @Value(value = "${filtered.topic.name}")
        private String filteredTopicName;

        // @Value(value = "${testObject.topic.name}")
        // private String testObjectTopicName;

        public void sendMessage(String message) {

            ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);

            future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

                @Override
                public void onSuccess(SendResult<String, String> result) {
                    System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata()
                        .offset() + "]");
                }

                @Override
                public void onFailure(Throwable ex) {
                    System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
                }
            });
        }

        public void sendMessageToPartition(String message, int partition) {
            kafkaTemplate.send(partitionedTopicName, partition, null, message);
        }

        public void sendMessageToFiltered(String message) {
            kafkaTemplate.send(filteredTopicName, message);
        }

        // public void sendGreetingMessage(TestObject testObject) {
        //     testObjectKafkaTemplate.send(testObjectTopicName, testObject);
        // }
    }

    public static class MessageListener {

        private CountDownLatch latch = new CountDownLatch(3);

        private CountDownLatch partitionLatch = new CountDownLatch(2);

        private CountDownLatch filterLatch = new CountDownLatch(2);

        // private CountDownLatch testObjectLatch = new CountDownLatch(1);

        @KafkaListener(topics = "${message.topic.name}", groupId = "group1", containerFactory = "group1KafkaListenerContainerFactory")
        public void listenGroup1(String message) {
            System.out.println("Received Message in group 'group1': " + message);
            latch.countDown();
        }

        @KafkaListener(topics = "${message.topic.name}", groupId = "group2", containerFactory = "group2KafkaListenerContainerFactory")
        public void listenGroup2(String message) {
            System.out.println("Received Message in group 'group2': " + message);
            latch.countDown();
        }

        @KafkaListener(topics = "${message.topic.name}", containerFactory = "headersKafkaListenerContainerFactory")
        public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
            System.out.println("Received Message: " + message + " from partition: " + partition);
            latch.countDown();
        }

        @KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = { "0", "3" }), containerFactory = "partitionsKafkaListenerContainerFactory")
        public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION_ID) int partition) {
            System.out.println("Received Message: " + message + " from partition: " + partition);
            this.partitionLatch.countDown();
        }

        @KafkaListener(topics = "${filtered.topic.name}", containerFactory = "filterKafkaListenerContainerFactory")
        public void listenWithFilter(String message) {
            System.out.println("Received Message in filtered listener: " + message);
            this.filterLatch.countDown();
        }

        // @KafkaListener(topics = "${testObject.topic.name}", containerFactory = "testObjectKafkaListenerContainerFactory")
        // public void testObjectListener(TestObject testObject) {
        //     System.out.println("Received testObject message: " + testObject);
        //     this.testObjectLatch.countDown();
        // }

    }

}