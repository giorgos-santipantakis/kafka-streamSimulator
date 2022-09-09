package main;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class streamSimulator {

	public static String KAFKA_BROKERS ="";
    public static Integer MESSAGE_COUNT=1000;
    public static String CLIENT_ID="client1";
    public static String TOPIC_NAME="stLD"; //default value
    public static String GROUP_ID_CONFIG="consumerGroup1";
    public static Integer MAX_NO_MESSAGE_FOUND_COUNT=100;
    public static String OFFSET_RESET_LATEST="latest";
    public static String OFFSET_RESET_EARLIER="earliest";

    public static Integer MAX_POLL_RECORDS=1;
    
    public static String streamingFile = "";
    
	public static final String TEXT_GREEN = "\u001B[32m";
	public static final String TEXT_RED = "\u001B[31m";
	public static final String TEXT_RESET = "\u001B[0m";
	public static final String TEXT_ERASE_TO_END_OF_LINE = "\u001B[K";
	
	private static DecimalFormat df = new DecimalFormat("##.###");
    
	private static float sleepInterval = 0.25f;//10; //msec

	private static boolean realtime = false;
	
	private static String mode = "c";

	private static int timeIndex = 7;
	
	private static TreeMap<Long, HashSet<String>> data = new TreeMap<>();

    public static Producer<Long, String> createProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomPartitioner.class.getName());
        return new KafkaProducer<>(props);
    }
    
    public static Consumer<Long, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID_CONFIG);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OFFSET_RESET_EARLIER);

        Consumer<Long, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC_NAME));
        System.out.println("TOPIC: " + TOPIC_NAME);
        return consumer;
    }

	private static void showHelp(){
		System.out.println("usage: java -jar simulator.jar --broker <IP:PORT> --mode <mode> --i <inputFile> --topic <topic> --sleep <sleep> {--realtime} --timeIndex <index>");
		System.out.println("mode: \"s\" for producer, \"c\" for consumer");
		System.out.println("inputFile: path to file that will be streamed (applies on \"s\" mode). " +
				"If no input file is provided, stdin is used");
		System.out.println("--broker: set the broker's IP and port, e.g. 127.0.0.1:9092");
		System.out.println("--sleep: speed parameter, e.g. --sleep 0.25 forces the stream to play on 1/4 of the realtime stream");
		System.out.println("--realtime: enable realtime simulation, based on the timestamps of records ");
		System.out.println("--timeIndex: set the timestamp column index, for the given data (applies on mode \"s\" only)");
		System.out.println("Alternatively use --config <file> to provide the above settings in a list of <key>=<value> entries, e.g. broker=127.0.0.1:9092 to set the broker's IP and port");
	}

	public static void main(String[] args) {
		// option a: Start a topic and send messages
		// option b: start a consumer and write received messages on console
		if(args.length==0){
			showHelp();
			return;
		}
		if(args[0].equalsIgnoreCase("--help")){
			showHelp();
			return;
		}

		parseArgs(args);
		
		if(args.length>0) {
			if(mode.equalsIgnoreCase("s")) {
				runProducer();
			}else {
				runConsumer();
			}
		}else {
			runConsumer();
		}
		System.out.println("Finished");
	}
	
	private static void parseArgs(String[] args) {
		for(int i=0;i<args.length;i++) {
			if(args[i].equalsIgnoreCase("--config")){
				parseSettings(args[++i]);
				return;
			}else {
				parseArg(args[i], (i<args.length-1)?args[++i]:null);
			}
		}
	}

	private static void parseArg(String key, String value){
		if(key.trim().equalsIgnoreCase("--mode")) {
			mode = value.trim();
		} else if(key.equalsIgnoreCase("--topic")) {
			TOPIC_NAME = value.trim();
		} else if(key.equalsIgnoreCase("--i")) {
			streamingFile = value.trim();
		}else if(key.equalsIgnoreCase("--broker")){
			KAFKA_BROKERS = value.trim();
		}else if(key.equalsIgnoreCase("--sleep")) {
			sleepInterval = Float.valueOf(value.trim());
		}else if(key.equalsIgnoreCase("--realtime")) {
			realtime = true;
		}
	}

	private static void parseSettings(String configFile){
		try{
			Files.lines(Paths.get(configFile)).forEach(i->{
				if(!i.startsWith("#")){
					String[] arg = i.split("=",-1);
					if(arg.length==2){
						parseArg(arg[0],arg[1]);
					}
				}
			});
		}catch (Exception e){
			e.printStackTrace();
		}
	}
	
	private static int cntr = 0;

	private static void runProducer() {
		System.out.println("Parsing data");
		System.out.println("Starting producer");

		Producer kafkaProducer = createProducer();

		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));

		if(realtime){
			//prepare in-memory structure
			SimpleDateFormat s1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			s1.setTimeZone(TimeZone.getTimeZone("UTC"));
			try{

				Files.lines(Paths.get(streamingFile)).skip(1).forEach(i->{
					String[] tmp = i.split(";",-1);
					try {
						long unixtime = s1.parse(tmp[timeIndex]).getTime()/1000l;
						HashSet<String> set = data.get(unixtime);
						if(set==null)set = new HashSet<String>();
						set.add(i);
						data.put(unixtime, set);
					}catch(Exception e){
						e.printStackTrace();
					}
				});
			}catch(Exception e){
				e.printStackTrace();
			}
		}

		stats(data);

		while(true) {

			try {
				if(streamingFile.isEmpty()) {
					System.out.print("type msg:");
					Scanner myObj = new Scanner(System.in);
				    String in = myObj.nextLine();
				    ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC_NAME, in);
                    try {
                    	RecordMetadata metadata = (RecordMetadata)kafkaProducer.send(record).get();
                    }catch (Exception e) {
                    	e.printStackTrace();
                    }
				}else {
					long max = Files.lines(Paths.get(streamingFile)).count();
					transmitData(streamingFile, max, kafkaProducer);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}

			System.out.println(TEXT_GREEN + "REDO" + TEXT_RESET);
		}
    }

	private static void stats(TreeMap<Long, HashSet<String>> data) {
		int[] minMaxTotal = minMaxTotal(data);
		System.out.println("MIN: " + minMaxTotal[0] + " MAX: " + minMaxTotal[1] + " TOTAL RECORDS: " +
				minMaxTotal[2] + " TOTAL ENTRIES: " + data.size());

	}

	private static int[] minMaxTotal(TreeMap<Long, HashSet<String>> data) {
		int min = Integer.MAX_VALUE;
		int max = Integer.MIN_VALUE;
		int total = 0;
		for(Map.Entry<Long, HashSet<String>> s: data.entrySet()){
			int size = s.getValue().size();
			if(size<min)min = size;
			if(size>max)max = size;
			total = total + size;
		}
		return new int[]{min, max, total};
	}

	private static void transmitData(String streamingFile, long max, Producer kafkaProducer) throws Exception {
		if(realtime){
			transmitDataRealTime(streamingFile, max, kafkaProducer);
		}else{
			transmitDataRecordPerSec(streamingFile, max, kafkaProducer);
		}
	}

	private static void transmitDataRealTime(String streamingFile, long max, Producer kafkaProducer) throws Exception {
		// if realtime streaming is enabled, group the records by timestamp and transmit them into batches
		// append timestamp on current date to simulate an infinite sliding window

		try{
			long lastT = 0;
			long dt = 0;
			cntr = 0;
			progressBar pb = new progressBar();
			max = data.size();
			SimpleDateFormat s1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			s1.setTimeZone(TimeZone.getTimeZone("UTC"));
			for(Map.Entry<Long, HashSet<String>> d: data.entrySet()) {
				if(lastT!=0){
					dt = d.getKey()-lastT;
				}
				String window = s1.format(new Date(lastT*1000l)) + "--" + s1.format(new Date(d.getKey()*1000l)) +"(dt:" + dt + ")";
				cntr++;
				String s = "";
				s = "[" + makeColorStr(window, 0, TEXT_GREEN) + "]: " + d.getValue().size();
				pb.progressGUI(cntr, max, s);

				lastT = d.getKey();
				Thread.sleep((long)((dt*1000l)*sleepInterval));
				for (String r : d.getValue()) {
					try {
						ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC_NAME, r);
						RecordMetadata metadata = (RecordMetadata) kafkaProducer.send(record).get();
//						System.out.println(r + "\r");
					} catch (ExecutionException e) {
						System.out.println("Error in sending record");
						System.out.println(e);
					}
				}

			}
		}catch(Exception e){

		}
	}

	private static void transmitDataRecordPerSec(String streamingFile, long max, Producer kafkaProducer) throws Exception {
		cntr = 0;
		progressBar pb = new progressBar();
		Files.lines(Paths.get(streamingFile)).skip(1).forEach(i->{
			try {
				Thread.sleep((long)(1000*sleepInterval));
				ProducerRecord<Long, String> record = new ProducerRecord<Long, String>(TOPIC_NAME,
						i);

				String s = "";
				cntr++;

				pb.progressGUI(cntr, max, s);
				try {
					RecordMetadata metadata = (RecordMetadata)kafkaProducer.send(record).get();
				}
				catch (ExecutionException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				}
				catch (InterruptedException e) {
					System.out.println("Error in sending record");
					System.out.println(e);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		});

	}


	private static String makeColorStr(String str, int index, String color) {
		String[] tmp = str.split(";",-1);
		String s = "";
		for(int i=0;i<tmp.length;i++) {
			String x = tmp[i];
			if(i==index) {
				x = color + x + TEXT_RESET;
			}
			if(s.isEmpty())s = x;
			else s = s + ";" + x;
		}
		return s;
	}

	private static String makeColorStr(String str, String color) {
		return color + str + TEXT_RESET;
	}

	private static List<String> readFile(String filename) {
		try {
			List<String> data = Files.readAllLines(Paths.get(filename));
			return data;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return new LinkedList<String>();
	}

	public static void runConsumer() {
        Consumer<Long, String> consumer = createConsumer();

        int noMessageFound = 0;

        while (true) {
          ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
          // 1000 is the time in milliseconds consumer will wait if no record is found at broker.
          if (consumerRecords.count() == 0) {
              noMessageFound++;
              if (noMessageFound > MAX_NO_MESSAGE_FOUND_COUNT)
                // If no message found count is reached to threshold exit loop.
                break;
              else
                  continue;
          }

          //print each record.
          consumerRecords.forEach(record -> {
              System.out.println("Record Key " + record.key());
              System.out.println("Record value " + record.value());
              System.out.println("Record partition " + record.partition());
              System.out.println("Record offset " + record.offset());
          });

        }
        consumer.close();
	}
}
