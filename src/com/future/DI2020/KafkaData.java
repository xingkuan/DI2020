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


class KafkaData extends DataPoint {
	Conf conf = Conf.getInstance();
	protected final String logDir = conf.getConf("logDir");
	private static final Logger logger = LogManager.getLogger();

	int kafkaMaxPollRecords;
	int pollWaitMil;

	//private List<String> docList = new ArrayList<String>();

	private KafkaConsumer<Long, String> consumer;
	private KafkaProducer<Long, String> producer;

	//TODO: Int can be more efficient. But about String, like Oracle ROWID?
	private List<String> msgKeyList=new ArrayList<String>();
	
	//public KafkaData(String dID) throws SQLException {
	public KafkaData(JSONObject dID, String role) throws SQLException {
		// super(dbid, url, cls, user, pwd);
		super(dID, role);
	}

	//public KafkaConsumer<Long, String> createKafkaConsumer(String topic) {
	public void createKafkaConsumer(String topic) {
		String consumerGrp = metaData.getJobID() + metaData.getTableID();
		String cientID = metaData.getJobID()+metaData.getTableDetails().get("tbl_id");

		kafkaMaxPollRecords = Integer.parseInt(conf.getConf("kafkaMaxPollRecords"));
		pollWaitMil = Integer.parseInt(conf.getConf("kafkaPollWaitMill"));

		Properties props = new Properties();
		props.put("bootstrap.servers", urlString);
		props.put("group.id", consumerGrp);
		// props.put(ConsumerConfig.GROUP_ID_CONFIG, jobID);
		props.put("client.id", cientID);
		props.put("enable.auto.commit", "false");
		props.put("auto.commit.interval.ms", "1000");
		props.put("auto.offset.reset", "earliest"); // if we do this, we better reduce msg retention to just one day.
		props.put("max.poll.records", kafkaMaxPollRecords);
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		consumer = new KafkaConsumer<Long, String>(props);
		// consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
		consumer.subscribe(Arrays.asList(topic));

		//return consumer;
	}

	public ConsumerRecords<Long, String> readMessages() {
		// ConsumerRecords<Long, String> records =
		// kc.poll(Duration.ofMillis(pollWaitMil));
		return consumer.poll(Duration.ofMillis(pollWaitMil));
	}

	private KafkaProducer<Long, String> createKafkaProducer() {
		String cientID = metaData.getJobID() + "_" + metaData.getDCCPoolID();

		String strVal = conf.getConf("kafkaMaxBlockMS");
		String kafkaACKS = conf.getConf("kafkaACKS");
		String kafkaINDEM = conf.getConf("kafkaINDEM");
		int kafkaMaxBlockMS = Integer.parseInt(strVal);
		int kafkaRetry = Integer.parseInt(conf.getConf("kafkaRetry"));

		Properties props = new Properties();
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
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		// props.put("value.serializer",
		// "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.CLIENT_ID_CONFIG, cientID);

		producer = new KafkaProducer<Long, String>(props);
		//Note: If the brokers are down, it will keep trying forever ... seems no way for
		//      checking and handling . Weird !!!

		return producer;
	}
/*
	public int syncDataFrom(DataPoint srcData) {
		int rtc=2;
	    int rrn=0;
	    long seq=0l;
	    String srcTbl;
		//ProducerRecord<Long, String> aMsg;

	    List<String> tblList = metaData.getDB2TablesOfJournal((String) metaData.getTableDetails().get("src_db_id".toString()), 
	        		(String) metaData.getTableDetails().get("src_schema")+"."+(String) metaData.getTableDetails().get("src_table"));     
	        
	    srcData.crtSrcAuxResultSet();  
	    ResultSet srcRset = srcData.getSrcResultSet();
        try {
			while (srcRset.next()) {
				rrn=srcRset.getInt("RRN");
				seq=srcRset.getLong("SEQNBR");
				srcTbl=srcRset.getString("SRCTBL");
				// ignore those for unregister tables:
				if (tblList.contains(srcTbl)) {
					final	ProducerRecord<Long, String> aMsg = new ProducerRecord<Long, String>(srcTbl, seq, String.valueOf(rrn));
					//RecordMetadata metadata = producer.send(aMsg).get();
					producer.send(aMsg, new Callback() {
	                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
	                        //execute everytime a record is successfully sent or exception is thrown
	                        if(e == null){
	                           // No Exception
	                        }else{
	    						logger.error("      exception at " + " " + aMsg.key() + ". Exit here!");
	    						logger.error(e);
	    						//set the aMsg.key and exit. Question: will the loop stop when encounter this exception?
	    						//metaData.setRefreshSeqThis(aMsg.key());
	    						metaData.getMiscValues().put("thisJournalSeq", aMsg.key());
	    						//rtc = -1;  //TODO: how to detect and handle errors here?
	                        }
	                    }
	                });
				}
			}
			rtc=0;
			logger.info("   last Journal Seq #: " + seq);
			metrix.sendMX("JournalSeq,jobId="+metaData.getJobID()+",journal="+metaData.getTableDetails().get("SRC_TABLE")+" value=" + seq + "\n");
		} catch (SQLException e) {
			logger.error("   failed to retrieve from DB2: " + e);
			rtc=0;   // ignore this one, and move on to the next one.
		} finally {
			srcData.releaseRSandSTMT();
		}
        return rtc;
	}
	*/
	@Override
	protected boolean miscPrep() {
		super.miscPrep();

		//String jTemp=metaData.getTableDetails().get("temp_id").toString();
		String jTemp=metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("temp_id"); 
		//if(jTemp.equals("D2V_")) {
		if(jTemp.equals("2DATA_")) {
			String topic = metaData.getTableDetails().get("src_schema")+"."
				+metaData.getTableDetails().get("src_table");
			createKafkaConsumer(topic);
			crtAuxSrcAsList();
		}
		return true;
	}
	protected int getDccCnt() {  //this one is very closely couple with crtAuxSrcAsList! TODO: any better way for arranging?
		return msgKeyList.size();
	}
	protected void crtAuxSrcAsList() {
		int giveUp = Integer.parseInt(conf.getConf("kafkaMaxEmptyPolls"));
		int maxMsgConsumption = Integer.parseInt(conf.getConf("kafkaMaxMsgConsumption"));

		List<String> msgKeyListT=new ArrayList<String>();

		int noRecordsCount = 0, cntRRN = 0;

		while (true) {
			ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(pollWaitMil));
			// ConsumerRecords<Long, String> records = consumerx.poll(0);
			if (records.count() == 0) {  //no record? try again, after consective "giveUp" times.
				noRecordsCount++;
				logger.info("    consumer poll cnt: " + noRecordsCount);
				if (noRecordsCount > giveUp)
					break; // no more records. exit
				else
					continue;
			}

			for (ConsumerRecord<Long, String> record : records) {
				//lastJournalSeqNum = record.key();
				msgKeyListT.add(record.value());
				cntRRN++;
				//if(cntRRN>maxMsgConsumption) { //consume only maximum of "maxMsgConsumption" msgs.
				//	break;
				//}
			}
			logger.info("    read total msg: " + (cntRRN-1));
		}
		//consumer.commitSync();  //TODO: little risk here. Ideally, that should happen after data is save in tgt.
		//consumer.close();       //   so to be called in the end of sync

		msgKeyList = msgKeyListT.stream()
	     .distinct()
	     .collect(Collectors.toList());
	    
		//metaData.end(rtc);
		metaData.setTotalMsgCnt(cntRRN-1);
	}
	public void commit() {
		consumer.commitSync();  
		consumer.close();
	}
	public void rollback() {
		//consumer.commitSync();  
		consumer.close();
	}

	protected List<String> getDCCKeyList(){
		return msgKeyList;
	}


	// ------ AVRO related; TODO: need to be consolidated--------------------------------------------------
	//https://dev.to/de_maric/guide-to-apache-avro-and-kafka-46gk
	//https://aseigneurin.github.io/2018/08/02/kafka-tutorial-4-avro-and-schema-registry.html
	//private KafkaProducer<Long, GenericRecord> createKafkaAVROProducer() {
	private KafkaProducer<Long, byte[]> createKafkaAVROProducer() {
		String cientID = metaData.getJobID() + "_" + metaData.getTableID();

		String strVal = conf.getConf("kafkaMaxBlockMS");
		String kafkaACKS = conf.getConf("kafkaACKS");
		String kafkaINDEM = conf.getConf("kafkaINDEM");
		int kafkaMaxBlockMS = Integer.parseInt(strVal);
		int kafkaRetry = Integer.parseInt(conf.getConf("kafkaRetry"));

		Properties props = new Properties();
		props.put("bootstrap.servers", urlString);
			
		props.put("acks", kafkaACKS);
		props.put("enable.idempotence", kafkaINDEM);
		props.put("batch.size", 1638400);

		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("max.block.ms", kafkaMaxBlockMS); // default 60000 ms
		// props.put("key.serializer",
		// "org.apache.kafka.common.serialization.StringSerializer");
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());  //TODO: need more thinking!
		// props.put("value.serializer",
		// "org.apache.kafka.common.serialization.ByteArraySerializer");
		//props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		//props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroSerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.CLIENT_ID_CONFIG, cientID);

		//KafkaProducer<Long, GenericRecord> prod = new KafkaProducer<Long, GenericRecord>(props);
		KafkaProducer<Long, byte[]> prod = new KafkaProducer<Long, byte[]>(props);
			//Note: If the brokers are down, it will keep trying forever ... seems no way for
			//      checking and handling . Weird !!!
		return prod;
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
	Producer<Long, byte[]> byteProducer;
	public void setupSink() {   
		createKafkaProducer();

		fldType = metaData.getFldJavaType();

		topic=metaData.getTableDetails().get("tgt_schema")+"."+metaData.getTableDetails().get("tgt_table");
		
		String jsonSch = metaData.getAvroSchema();
		schema = new Schema.Parser().parse(jsonSch); //TODO: ??  com.fasterxml.jackson.core.JsonParseException
		//schema = new Schema.Parser().parse(new File("user.avsc"));
		//Producer<Long, GenericRecord> producer = createKafkaAVROProducer();
		byteProducer = createKafkaAVROProducer();
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
			case 6:
				tempO=rs.getDate(i+1);
				if(tempO==null)
					record.put(i, null);
				else {
					//record.put(i, tempO); // class java.sql.Date cannot be cast to class java.lang.Long
				tempNum = rs.getDate(i+1).getTime();
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
			
	   		byte[] myvar = avroToBytes(record, schema);
	   		//producer.send(new ProducerRecord<Long, byte[]>("VERTSNAP.TESTOK", (long) 1, myvar),new Callback() {
	   		//producer.send(new ProducerRecord<Long, byte[]>(topic, (long) 1, myvar),new Callback() {  //TODO: what key to send?
	   		byteProducer.send(new ProducerRecord<Long, byte[]>(topic,  myvar),
	   			new Callback() {             //      For now, no key
	   				public void onCompletion(RecordMetadata recordMetadata, Exception e) {   //execute everytime a record is successfully sent or exception is thrown
	   					if(e == null){
	   						}else{
	   							logger.error(e);
	   						}
	   					}
	   			});
	   			msgCnt++;
				}
 	   	}catch (SQLException e) {
		  logger.error(e);
 	   	}
	}
	@Override
	public void write() {
		logger.info("not needed for now");
	}
/**************************************/
/*	public int syncAvroDataFrom(DataPoint srcData) {
		int rtc=2;
		int cnt=0;
		ResultSet rs=srcData.getSrcResultSet();
		ArrayList<Integer> fldType = metaData.getFldJavaType();

		String topic=metaData.getTableDetails().get("tgt_schema")+"."+metaData.getTableDetails().get("tgt_table");
		
		String jsonSch = metaData.getAvroSchema();
		Schema schema = new Schema.Parser().parse(jsonSch); //TODO: ??  com.fasterxml.jackson.core.JsonParseException
		//schema = new Schema.Parser().parse(new File("user.avsc"));
		//Producer<Long, GenericRecord> producer = createKafkaAVROProducer();
		Producer<Long, byte[]> producer = createKafkaAVROProducer();
long tempNum;
Object tempO;
		GenericData.Record record;
		//  Producer<String, byte[]> producer = null;
		  try {
	       while ( rs.next() ) {
	    	   record = new GenericData.Record(schema);	     //each record also has the schema ifno, which is a waste!      
				for (int i = 0; i < fldType.size(); i++) {  //The last column is the internal key.
					// Can't use getObject() for simplicity. :(
					//   1. Oracle ROWID, is a special type, not String as expected
					//   2. For NUMBER, it returns as BigDecimal, which Java has no proper way for handling and 
					//      AVRO has problem with it as well.
					 //
					//record.put(i, rs.getObject(i+1));
					switch(fldType.get(i)) {
					case 1:
						record.put(i, rs.getString(i+1));
						break;
					case 4:
						record.put(i, rs.getLong(i+1));
						break;
					case 7:
					case 6:
						tempO=rs.getDate(i+1);
						if(tempO==null)
							record.put(i, null);
						else {
							//record.put(i, tempO); // class java.sql.Date cannot be cast to class java.lang.Long
						tempNum = rs.getDate(i+1).getTime();
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
				//String temp = rs.getString(fldNames.size());
				//record.put(fldNames.size()-1, rs.getString(fldNames.size()));

	    	   //
	    	   // use byte, instead; ideally, use Confluent's Schena Registry 
	    	    //
	    	   //producer.send(new ProducerRecord<Long, GenericRecord>("topica", (long) 1, record));
		   		byte[] myvar = avroToBytes(record, schema);
		   		//producer.send(new ProducerRecord<Long, byte[]>("VERTSNAP.TESTOK", (long) 1, myvar),new Callback() {
		   		//producer.send(new ProducerRecord<Long, byte[]>(topic, (long) 1, myvar),new Callback() {  //TODO: what key to send?
		   		producer.send(new ProducerRecord<Long, byte[]>(topic,  myvar),new Callback() {             //      For now, no key
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //execute everytime a record is successfully sent or exception is thrown
                    	if(e == null){
                        }else{
    						logger.error(e);
                        }
                    }
                });
		   		cnt++;
	       }
		  } catch (SQLException e) {
			  logger.error(e);
		  }
		  //return cnt;
		  return rtc;
	}
*/	
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
	
	private KafkaConsumer<Long, byte[]> createKafkaAVROConsumer() {
	    Properties propsC = new Properties();

	    String consumerGrp = metaData.getJobID() + metaData.getTableID();
		String cientID = metaData.getJobID()+metaData.getTableDetails().get("tbl_id");

		kafkaMaxPollRecords = Integer.parseInt(conf.getConf("kafkaMaxPollRecords"));
		pollWaitMil = Integer.parseInt(conf.getConf("kafkaPollWaitMill"));

		propsC.put("bootstrap.servers", urlString);
		propsC.put("group.id", consumerGrp);
		// props.put(ConsumerConfig.GROUP_ID_CONFIG, jobID);
		propsC.put("client.id", cientID);
		propsC.put("enable.auto.commit", "false");
		propsC.put("auto.commit.interval.ms", "1000");
		propsC.put("auto.offset.reset", "earliest"); // if we do this, we better reduce msg retention to just one day.
		propsC.put("max.poll.records", kafkaMaxPollRecords);
		propsC.put("session.timeout.ms", "30000");
		propsC.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		//propsC.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		propsC.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

		KafkaConsumer<Long, byte[]> kafkaConsumer = new KafkaConsumer<Long, byte[]>(propsC);
		// consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
		//consumer.subscribe(Arrays.asList(topic));

		return kafkaConsumer;

	}
	//public void consumeBy(ESData sink) {
	//public JSONArray consumer() {
	//public List<String> getSrcList() {
		public void crtSrcResultSet(int actId, String preSQLs) { //parameters are not applicable here :(
		//List<String> docList = new ArrayList<String>();
				
		String topic=metaData.getTableDetails().get("tgt_schema")+"."+metaData.getTableDetails().get("tgt_table");
		ConsumerRecords<Long, byte[]> records;
		GenericRecord a=null, b=null;
		
		String jsonSch = metaData.getAvroSchema();
		Schema schema = new Schema.Parser().parse(jsonSch); //TODO: ??  com.fasterxml.jackson.core.JsonParseException

		KafkaConsumer<Long, byte[]> kafkaConsumer = createKafkaAVROConsumer();
		kafkaConsumer.subscribe(Arrays.asList(topic));

		try {
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);

			ByteArrayInputStream in;
			while (true) {
				records = kafkaConsumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<Long, byte[]> record : records) {
					in = new ByteArrayInputStream(record.value());
					BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
					kafkaConsumer.commitAsync();

					a = reader.read(null, decoder);
					//a = reader.read(b, decoder); 
					System.out.println(a);
					System.out.println(a.get(1));
					//sink.process(a); or construct a JSONArray, and return to the sink

					msgKeyList.add(a.toString());
				}
			}
			//sink.bulkIndex(docList);
		} catch (IOException e) {
			e.printStackTrace();
		}
		//return docList;
	}
	public void test() {
		xformInto(null);
	}
	@Override
	public void xformInto(DataPoint tgtData) {
		String topic=metaData.getTableDetails().get("tgt_schema")+"."+metaData.getTableDetails().get("tgt_table");
		ConsumerRecords<Long, byte[]> records;
		GenericRecord a=null, b=null;
		
		String jsonSch = metaData.getAvroSchema();
		Schema schema = new Schema.Parser().parse(jsonSch); //TODO: ??  com.fasterxml.jackson.core.JsonParseException

		KafkaConsumer<Long, byte[]> kafkaConsumer = createKafkaAVROConsumer();
		kafkaConsumer.subscribe(Arrays.asList(topic));

		xformEngine jsEngin = new xformEngine();
		
		try {
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);

			ByteArrayInputStream in;
			while (true) {
				records = kafkaConsumer.poll(Duration.ofMillis(100));
				for (ConsumerRecord<Long, byte[]> record : records) {
				    System.out.println("Partition: " + record.partition() 
				        + " Offset: " + record.offset()
				        + " Value: " + java.util.Arrays.toString(record.value()) 
				        + " ThreadID: " + Thread.currentThread().getId()  );
			//TODO: is that right? 
					in = new ByteArrayInputStream(record.value());
					BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
					kafkaConsumer.commitAsync();

					a = reader.read(null, decoder);
					//a = reader.read(b, decoder); 
					System.out.println(a);
					System.out.println(a.get(1));
					System.out.println(a.get(2));  //TODO: still in long, not DATE!?
					System.out.println(a.get(3)); 
					if(tgtData!=null) {
						b = jsEngin.transform(a);
						tgtData.write(a);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	private void testJSGraal() {
	   ScriptEngine graalEngine = new ScriptEngineManager().getEngineByName("graal.js");
	   //load the transformation JS script
	   try {
			graalEngine.eval("print('Hello Graal World!');");
			graalEngine.eval("function sum(a,b){return a.concat(b);}");
		    String v = (String)graalEngine.eval("sum(\"Hello, \", \"the other world!\")");
		    System.out.println(v);
	    } catch (ScriptException e) {
			e.printStackTrace();
		}   
	   return ;
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
public boolean regDcc(int tblID, String PK, String srcSch, String srcTbl, String dccPgm, String jurl, String tgtSch, String tgtTbl, String dccDBid) {
	/*
	cmdLine = "/opt/kafka/bin/kafka-topics.sh --zookeeper usir1xrvkfk02:2181 --delete --topic " 
			+ srcSch + "."	+ srcTbl + "\n\n" 
			+ "/opt/kafka/bin/kafka-topics.sh --create " + "--zookeeper usir1xrvkfk02:2181 "
			+ "--replication-factor 2 " + "--partitions 2 " + "--config retention.ms=86400000 " 
			+ "--topic " + srcSch + "." + srcTbl + " \n\n";
	sqlStr += "/opt/kafka/bin/kafka-topics.sh --zookeeper usir1xrvkfk02:2181 --delete --topic " 
			+ tgtSch + "."	+ srcTbl + "\n\n" 
			+ "/opt/kafka/bin/kafka-topics.sh --create " + "--zookeeper usir1xrvkfk02:2181 "
			+ "--replication-factor 2 " + "--partitions 2 " + "--config retention.ms=86400000 " 
			+ "--topic " + tgtSch + "." + tgtTbl + " \n";
	*/
	int partitions=2;
	short replicationFactor=2;
	String topicName=srcSch + "." + srcTbl;
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
	return true;
}
@Override
public boolean unregisterTgt(int tblID) {
	//delete the topic
	String theTopic = metaData.getTableDetails().get("tgt_schema")+"."
			+ metaData.getTableDetails().get("tgt_table");
	if(!theTopic.equals("*.*")){
	try (final AdminClient adminClient = createKafkaAdmin()) {
        DeleteTopicsResult deleteTopicsResult=adminClient.deleteTopics(Arrays.asList(theTopic));
    }
}
	return true;
}
@Override
public boolean unregisterDcc(int tblID) {
	//delete DCC topic
	String theTopic = metaData.getTableDetails().get("src_schema")+"."
			+ metaData.getTableDetails().get("src_table")+"DCC";
	try (final AdminClient adminClient = createKafkaAdmin()) {
        DeleteTopicsResult deleteTopicsResult=adminClient.deleteTopics(Arrays.asList(theTopic));
    }
	return true;
}


public AdminClient createKafkaAdmin() {
	Properties adminProps = new Properties();
	adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, urlString);

	AdminClient admin = AdminClient.create(adminProps);
	return admin;
}

/***************************************************/

}