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


class KafkaData extends Kafka {
	Conf conf = Conf.getInstance();
	protected final String logDir = conf.getConf("logDir");
	private static final Logger logger = LogManager.getLogger();

	private xformEngine xEngine=null;
	
	int kafkaMaxPollRecords;
	int pollWaitMil;

	//private List<String> docList = new ArrayList<String>();

	//private KafkaConsumer<Long, String> consumer;
	//private KafkaProducer<Long, String> producer;
	private KafkaConsumer<Long, byte[]> consumer;
	private KafkaProducer<Long, byte[]> producer;

	//TODO: Int can be more efficient. But about String, like Oracle ROWID?
	private List<String> msgKeyList=new ArrayList<String>();
	
	//public KafkaData(String dID) throws SQLException {
	public KafkaData(JSONObject dID) throws SQLException {
		// super(dbid, url, cls, user, pwd);
		super(dID);
	}


	// ------ AVRO related; TODO: need to be consolidated--------------------------------------------------
	//https://dev.to/de_maric/guide-to-apache-avro-and-kafka-46gk
	//https://aseigneurin.github.io/2018/08/02/kafka-tutorial-4-avro-and-schema-registry.html
	//private KafkaProducer<Long, GenericRecord> createKafkaAVROProducer() {
	private void createKafkaProducer() {
		setProducerProps();

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());  //TODO: need more thinking!
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");

		//KafkaProducer<Long, GenericRecord> prod = new KafkaProducer<Long, GenericRecord>(props);
		producer = new KafkaProducer<Long, byte[]>(props);
	}
	
	@Override
	protected void createKafkaConsumer() {
		setConsumerProps();
		props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		//propsC.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		consumer = new KafkaConsumer<Long, byte[]>(props);
		// consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
		//consumer.subscribe(Arrays.asList(topic));
	}

	/**************************************/
	/*It is a sink, because it put msg to Kafka topic(s).
	 *  May need two version: one for DCC, with is <String, String>
	 *                        one is for bus. data, in AVRO  
	 */
	String topic;
	int msgCnt;
	ArrayList<Integer> fldType;
	Schema schema;
	long tempNum;
	Object tempO;
	GenericData.Record record;
	@Override
	public void setupSink() {    //sink in the sense of this app; but it is a Kafka producer
		createKafkaProducer();

		fldType = metaData.getFldJavaType();

		topic=metaData.getTaskDetails().get("tgt_schema")+"."+metaData.getTaskDetails().get("tgt_table");
		
		String jsonSch = metaData.getAvroSchema();
		schema = new Schema.Parser().parse(jsonSch); //TODO: ??  com.fasterxml.jackson.core.JsonParseException
		//schema = new Schema.Parser().parse(new File("user.avsc"));
		//Producer<Long, GenericRecord> producer = createKafkaAVROProducer();
		//createKafkaProducer();
	}
	@Override
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
	@Override
	public void write() {
		logger.info("not needed for now");
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
	
	@Override
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
	
	public void test() {
		crtSrcResultSet();
		//xformInto(null);
	}
	
	/******************* transform APIs ****************************************/
	@Override
	protected int xformInto(DataPointMgr tgtData) {
		int cnt=0;		
		String topic=metaData.getTaskDetails().get("src_schema")+"."+metaData.getTaskDetails().get("src_table");
		System.out.println(topic);
//topic="TEST.TESTOK";
//System.out.println(topic);

		ConsumerRecords<Long, byte[]> records;
		GenericRecord a=null;
		JSONObject b=null;
		
		String jsonSch = metaData.getXfrmDetails().get("src_avro").toString(); 
/*			"{\"namespace\": \"com.future.DI2020.avro\", \n" + 
				"\"type\": \"record\", \n" + 
				"\"name\": \"VERTSNAP.TESTOK\", \n" + 
				"\"fields\": [ \n" + 
				"{\"name\": \"COL\", \"type\": \"long\"} \n" + 
				", {\"name\": \"COL2\", \"type\": [\"string\", \"null\"]} \n" + 
				", {\"name\": \"COL3\", \"type\": [\"string\",\"null\"], \"logicalType\": \"date\"} \n" + 
				", {\"name\": \"COL4\",  \"type\": [\"string\",\"null\"], \"logicalType\": \"timestamp-micro\"} \n" + 
				", {\"name\": \"ORARID\", \"type\": \"string\"} \n" + 
				"] }";  
				*/
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
					//xEngine.test();
					//xEngine.transform(null);
					if (noRecordsCount > giveUp)
						break; // no more records. exit
					else
						continue;
				}

				for (ConsumerRecord<Long, byte[]> record : records) {
					in = new ByteArrayInputStream(record.value());
					BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
					consumer.commitAsync();

					a = reader.read(null, decoder);
					//a = reader.read(b, decoder); 
					//System.out.println(a);
					//System.out.println(a.get(1));
					//sink.process(a); or construct a JSONArray, and return to the sink
		//			tgtData.write(a);
b=xEngine.transform(a);
System.out.println(b);
tgtData.write(b);
					cnt++;
				}
			}
			//consumer.commitAsync();
			consumer.close();
			//sink.bulkIndex(docList);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return cnt;
		
	}
	
	
//--------------------------------------------------------------------------------------------------------
public void close() {
		try {
		consumer.close();
		producer.close();
		}catch(NullPointerException e) {
			logger.info("      nothing to close.");
		}
	}


/******** Registration APIs **********/
@Override
public boolean regSrcCheck(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
	//do nothing for now.
	return true;
}
public boolean regSrc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
	//not Vertica is not used as src so far.
	return false;
}
@Override
public boolean regSrcDcc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
	//not Vertica is not used as src so far.
	return false;
}
@Override
public boolean regTgt(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
	int partitions=2;
	short replicationFactor=2;
	String topicName=tgtSch + "." + tgtTbl;
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
	
	//also, update meta info:
	String sql = "update data_field set " + 
			"tgt_field=regexp_replace(regexp_replace(src_field, '^.* as ', ''), '#', 'NUM'), " +   //- PK field -and replace # with NUM
			"tgt_field_type=case " + 
			"when src_field_type like '%CHAR%' then 'VARCHAR('||2*src_field_len||')' " + 
			"when src_field_type like '%NUMBER%' then 'NUMBER('||src_field_len||','||coalesce(src_field_scale,0)||')' " + 
			"when src_field_type like 'DATE%' then 'DATE' " + 
			"when src_field_type like 'TIMEST%' then 'TIMESTAMP' " +  //DB2/AS400 is TIMESTMP 
			"else src_field_type " + 
			"END " + 
			"where task_id=" + tblID;
	metaData.runRegSQL(sql);
	

	return true;
}
@Override
public boolean unregisterTgt(int tblID) {
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


public void setupXformEngine(xformEngine xformEng) {
	xEngine=xformEng;
}

}