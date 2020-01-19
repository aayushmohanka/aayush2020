package Utility;

import consumer.KafkaConsumerExample;
import producer.KafkaProducerExample;

public class RunApplication{

	public static void main(String[] args) {
		//Creating 2 thread to start both producer and consumer at a time.
		KafkaConsumerExample kce = new KafkaConsumerExample();
		Thread t1 = new Thread(kce);
		KafkaProducerExample kpe = new KafkaProducerExample();
		Thread t2 = new Thread(kpe);
		t1.start();
		t2.start();
		
		
	}

}
