package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import java.text.*;
import java.time.Duration;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;


class Kafka extends DataPoint {
	Conf conf = Conf.getInstance();
	protected final String logDir = conf.getConf("logDir");
	private static final Logger logger = LogManager.getLogger();

	int kafkaMaxPollRecords;
	int pollWaitMil;

	private KafkaConsumer<Long, byte[]> consumer;
	private KafkaProducer<Long, byte[]> producer;

	protected Properties props = new Properties();
	
	public Kafka(JSONObject dID) {
		super(dID);
	}

	//public KafkaConsumer<Long, String> createKafkaConsumer(String topic) {
	protected void setConsumerProps() {
		String consumerGrp = metaData.getJobID() + metaData.getTableID();
		String cientID = metaData.getJobID()+metaData.getTaskDetails().get("task_id");

		kafkaMaxPollRecords = Integer.parseInt(conf.getConf("kafkaMaxPollRecords"));
		pollWaitMil = Integer.parseInt(conf.getConf("kafkaPollWaitMill"));

		props.put("bootstrap.servers", urlString);
		props.put("group.id", consumerGrp);
		// props.put(ConsumerConfig.GROUP_ID_CONFIG, jobID);
		props.put("client.id", cientID);
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest"); // if we do this, we better reduce msg retention to just one day.
		props.put("max.poll.records", kafkaMaxPollRecords);
		props.put("session.timeout.ms", "30000");
		//props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		//props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		//consumer = new KafkaConsumer<Long, String>(props);
		// consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
		//consumer.subscribe(Arrays.asList(topic));

		//return consumer;
	}

	//public ConsumerRecords<Long, String> readMessages() {
	//	// ConsumerRecords<Long, String> records =
	//	// kc.poll(Duration.ofMillis(pollWaitMil));
	//	return consumer.poll(Duration.ofMillis(pollWaitMil));
	//}
	@Override
	public int sync(DataPoint srcData) {
		return 0;
	}
	public void write(ResultSet rs) {
	 	   record = new GenericData.Record(schema);	     //each record also has the schema ifno, which is a waste!   
	 	   try {
			for (int i = 0; i < fldType.size(); i++) {  //The last column is the internal key.
				/* Can't use getObject() for simplicity. :(
				 *   1. Oracle ROWID, is a special type, not String as expected
				 *   2. For NUMBER, it returns as BigDecimal, which Java has no proper way for handling and 
				 *      AVRO has problem with it as well.
				 */
				//record.put(i, rs.getObject(i+1));
				switch(fldType.get(i)) {
				case 1:
					record.put(i, rs.getString(i+1));
					break;
				case 4:
					record.put(i, rs.getLong(i+1));
					break;
				case 7:
					tempO=rs.getDate(i+1);
					if(tempO==null)
						record.put(i, null);
					else {
						//tempNum = rs.getDate(i+1).getTime();
						record.put(i, tempO.toString());
					}
					break;
				case 6:
					tempO=rs.getTimestamp(i+1);
					if(tempO==null)
						record.put(i, null);
					else {
						//record.put(i, tempO); // class java.sql.Date cannot be cast to class java.lang.Long
						//tempNum = rs.getDate(i+1).getTime();
						//record.put(i, new java.util.Date(tempNum));  //class java.util.Date cannot be cast to class java.lang.Number 
						//record.put(i, tempNum);  //but that will show as long on receiving!
						record.put(i, tempO.toString());  
					}
			//		break;
			//	case 6:
			//		record.put(i, new java.util.Timestamp(rs.getTimestamp(i+1).getTime()));
					break;
				default:
					logger.warn("unknow data type!");
					record.put(i, rs.getString(i+1));
					break;
				}
				
			}
	   		byte[] myvar = avroToBytes(record, schema);
	   		//producer.send(new ProducerRecord<Long, byte[]>("VERTSNAP.TESTOK", (long) 1, myvar),new Callback() {
	   		//producer.send(new ProducerRecord<Long, byte[]>(topic, (long) 1, myvar),new Callback() {  //TODO: what key to send?
	   		producer.send(new ProducerRecord<Long, byte[]>(topic,  myvar),
	   			new Callback() {             //      For now, no key
	   				public void onCompletion(RecordMetadata recordMetadata, Exception e) {   //execute everytime a record is successfully sent or exception is thrown
	   					if(e == null){
	   						}else{
	   							logger.error(e);
	   						}
	   					}
	   			});
	   			msgCnt++;
	 	   	}catch (SQLException e) {
			  logger.error(e);
	 	   	}
		}

	private byte[] avroToBytes(GenericRecord avroRec, Schema schema){
		byte[] myvar=null;
			
		  DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		  ByteArrayOutputStream out = new ByteArrayOutputStream();
		  BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
//other:		  BinaryEncoder encoder = EncoderFactory.get().jsonEncoder(schema, out)

		  try {
			writer.write(avroRec, encoder);
			
		    encoder.flush();
			out.close();
		    myvar = out.toByteArray();
		  } catch (IOException e) {
	  		logger.error(e);
	  	}
		return myvar;
	  }

	public int crtSrcResultSet() { 
		//List<String> docList = new ArrayList<String>();
		int cnt=0;		
		String topic=metaData.getTaskDetails().get("tgt_schema")+"."+metaData.getTaskDetails().get("tgt_table");
		System.out.println(topic);
		
		ConsumerRecords<Long, byte[]> records;
		GenericRecord a=null, b=null;
		
		String jsonSch = metaData.getAvroSchema();
		System.out.println(jsonSch);
		Schema schema = new Schema.Parser().parse(jsonSch); //TODO: ??  com.fasterxml.jackson.core.JsonParseException
		
		createKafkaConsumer();
		consumer.subscribe(Arrays.asList(topic));

		try {
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);

			int giveUp = Integer.parseInt(conf.getConf("kafkaMaxEmptyPolls"));
			int noRecordsCount = 0, cntRRN = 0;
			ByteArrayInputStream in;
			while (true) {
				records = consumer.poll(Duration.ofMillis(100));
				if (records.count() == 0) {  //no record? try again, after consective "giveUp" times.
					noRecordsCount++;
					logger.info("    consumer poll cnt: " + noRecordsCount);
					if (noRecordsCount > giveUp)
						break; // no more records. exit
					else
						continue;
				}

				for (ConsumerRecord<Long, byte[]> record : records) {
					in = new ByteArrayInputStream(record.value());
					BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
					//consumer.commitAsync();

					a = reader.read(null, decoder);
					//a = reader.read(b, decoder); 
					System.out.println(a);
					System.out.println(a.get(1));
					//sink.process(a); or construct a JSONArray, and return to the sink

					msgKeyList.add(a.toString());
					cnt++;
				}
			}
			consumer.commitAsync();
			//sink.bulkIndex(docList);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return cnt;
	}
	
	protected void setProducerProps() {
		String cientID = metaData.getJobID() + "_" + metaData.getTaskDetails().get("task_id");

		String strVal = conf.getConf("kafkaMaxBlockMS");
		String kafkaACKS = conf.getConf("kafkaACKS");
		String kafkaINDEM = conf.getConf("kafkaINDEM");
		int kafkaMaxBlockMS = Integer.parseInt(strVal);
		int kafkaRetry = Integer.parseInt(conf.getConf("kafkaRetry"));

		props.put("bootstrap.servers", urlString);
		
	    props.put("acks", kafkaACKS);
	    props.put("enable.idempotence", kafkaINDEM);
	    //props.put("message.send.max.retries", kafkaSendTries);
	   // props.put(ProducerConfig.RETRIES_CONFIG, kafkaRetry);  //? default 2147483647
	    props.put("batch.size", 1638400);

		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("max.block.ms", kafkaMaxBlockMS); // default 60000 ms
		// props.put("key.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		//props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		props.put(ProducerConfig.CLIENT_ID_CONFIG, cientID);
		// props.put("value.serializer",
		// "org.apache.kafka.common.serialization.ByteArraySerializer");
		//props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//producer = new KafkaProducer<Long, String>(props);
		//Note: If the brokers are down, it will keep trying forever ... seems no way for
		//      checking and handling . Weird !!!

		//return producer;
	}

	
	private void createKafkaProducer() {
		setProducerProps();

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());  //TODO: need more thinking!
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

		//KafkaProducer<Long, GenericRecord> prod = new KafkaProducer<Long, GenericRecord>(props);
		producer = new KafkaProducer<Long, byte[]>(props);
		ConsumerRecords<Long, String> records = consumer.poll(300);
	}
	
	protected void createKafkaConsumer() {
		setConsumerProps();
		props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		//propsC.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		consumer = new KafkaConsumer<Long, byte[]>(props);

		// consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
		//consumer.subscribe(Arrays.asList(topic));
	}

	/******** admin client, for registration/unregistration ****/
	protected AdminClient createKafkaAdmin() {
		Properties adminProps = new Properties();
		adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, urlString);
	
		AdminClient admin = AdminClient.create(adminProps);
		return admin;
	}

	@Override
	public JSONObject runDBcmd(String sqlStr, String type) {
		// TODO Auto-generated method stub
		return null;
	}
	//TODO: move the following to runDBcmd 
	@Override
	public boolean registTask(TaskMeta instructions) {
		int partitions=2;
		short replicationFactor=2;
		String topicName=instructions + "." + tgtTbl;
		try (final AdminClient adminClient = createKafkaAdmin()) {
	        try {
	            // Define topic
	            final NewTopic newTopic = new NewTopic(topicName, partitions, replicationFactor);

	            // Create topic, which is async call.
	            final CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singleton(newTopic));
	            // Since the call is Async, Lets wait for it to complete.
	            createTopicsResult.values().get(topicName).get();
	            
	        } catch (InterruptedException | ExecutionException e) {
	            if (!(e.getCause() instanceof TopicExistsException)) {
	                throw new RuntimeException(e.getMessage(), e);
	            }
	        }
		}
		
		return true;
	}
	//clean up the task relate objects
	@Override
	public boolean unregistTask(TaskMeta instructions) {
		//delete the topic
		String theTopic = metaData.getTaskDetails().get("tgt_schema")+"."
				+ metaData.getTaskDetails().get("tgt_table");
		if(!theTopic.equals("*.*")){
		try (final AdminClient adminClient = createKafkaAdmin()) {
	        DeleteTopicsResult deleteTopicsResult=adminClient.deleteTopics(Arrays.asList(theTopic));
	    }
	}
		
		return true;
	}
	
	
	public void close() {
		try {
		consumer.close();
		producer.close();
		}catch(NullPointerException e) {
			logger.info("      nothing to close.");
		}
	}

}