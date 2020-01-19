package producer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.Scanner;

public class KafkaProducerExample implements Runnable {
	

	public static boolean flag = true;
	public static void bootStrap() throws Exception 
	{
		flag= true;//flag is also used in consumer to check and stop thread
		Scanner s = new Scanner(System.in);
		System.out.println("How many messages do you want to produce");
		int number = s.nextInt();
		for(int n =0 ;n < number;n++)
		{
		System.out.println("Enter Key");
		Long key = s.nextLong();
		System.out.println("Enter value");
		String value = s.next();
		runProducer(number,key,value);//method where flow starts
		}
		while(flag==true)
		{
		System.out.println("Do you want to produce more message(Y/N)");
		String msg= s.next();
		System.out.println(msg);
		if(msg.equalsIgnoreCase("Y"))
		{
			KafkaProducerExample.bootStrap();
		}
		else
		{
			flag=false;
			System.out.println("Thanks for producing");
		}
		}
	}


	private final static String TOPIC = "Topic1";//topic created with this name
	private final static String BOOTSTRAP_SERVERS = "localhost:9092";//broker /server 

	private static Producer<Long, String> createProducer() {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
				BOOTSTRAP_SERVERS);
		props.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaExampleProducer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
				LongSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
				StringSerializer.class.getName());
		return new KafkaProducer<>(props);
	}


	static void runProducer(int number,Long key,String value) throws Exception {
		final Producer<Long, String> producer = createProducer();//Create producer object by passing all needed configs.

		try {

			final ProducerRecord<Long,String> record =
					new ProducerRecord<>(TOPIC,0,key,value);//Many constructor available ,check source code.

			RecordMetadata metadata = producer.send(record).get();//send records to topic of broker with partition no 1
			/*
			 * System.out.println("recordKey-"+record.key()+"RecordValue -"+
			 * record.value()+"MetaData Partition-"+metadata.partition() +"offset -"+
			 * metadata.offset());
			 */


		} finally {
			producer.flush();
			producer.close();
		}
		
	}


	@Override
	public void run() {
		try {
			//bootStrap of this class will start producer
			KafkaProducerExample.bootStrap();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}


}
