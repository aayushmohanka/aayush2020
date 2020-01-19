package consumer;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import producer.KafkaProducerExample;

import java.util.Collections;
import java.util.Properties;


public class KafkaConsumerExample  implements Runnable {
	
	
	private final static String TOPIC = "Topic1";
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";
	
	public static void bootStrapConsumer() throws Exception {
		
		while (KafkaProducerExample.flag)
		{
	      runConsumer();
		}
	  }
	/*
	 * public static void main(String[] args) throws InterruptedException {
	 * runConsumer();
	 * 
	 * }
	 */
	
	private static Consumer<Long, String> createConsumer() {
	      final Properties props = new Properties();
	      props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
	                                  BOOTSTRAP_SERVERS);
	      props.put(ConsumerConfig.GROUP_ID_CONFIG,
	                                  "KafkaExampleConsumer");
	      props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
	              LongDeserializer.class.getName());
	      props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
	              StringDeserializer.class.getName());
	      
	      // Create the consumer using props.
	      final Consumer<Long, String> consumer =
	                                  new KafkaConsumer<>(props);
	      // Subscribe to the topic.
	      consumer.subscribe(Collections.singletonList(TOPIC));
	      return consumer;
	  }
	 
	
	static void runConsumer() throws InterruptedException {
        final Consumer<Long, String> consumer = createConsumer();
        
        final int giveUp = 5;   int noRecordsCount = 0;
        while (true) {
        	
            final ConsumerRecords<Long, String> consumerRecords =
                    consumer.poll(1000);
            if (consumerRecords.count()==0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) 
                	{
                	System.out.println("NO message seen"); 
                	break;
                	}
                else continue;
            }
            
            System.out.println("Hey!! seems some record got produced");
            consumerRecords.forEach(record -> {
                System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                        record.key(), record.value(),
                        record.partition(), record.offset());
            });
            consumer.commitAsync();
        }
        consumer.close();
        
    }

	@Override
	public void run() {
		try {
			KafkaConsumerExample.bootStrapConsumer();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}

