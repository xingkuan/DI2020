package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.text.*;
import java.time.Duration;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;


class KafkaData extends DataPointer {
	Conf conf = Conf.getInstance();
	protected final String logDir = conf.getConf("logDir");
	private static final Logger ovLogger = LogManager.getLogger();

	int kafkaMaxPollRecords;
	int pollWaitMil;


	private KafkaConsumer<Long, String> consumer;
	private KafkaProducer<Long, String> producer;

	//TODO: Int can be more efficient. But about String, like Oracle ROWID?
	private List<String> msgKeyList=new ArrayList<String>();
	
	//public KafkaData(String url, String cls, String user, String pwd) {
	public KafkaData(String dID) throws SQLException {
		// super(dbid, url, cls, user, pwd);
		super(dID);
	}

	//public KafkaConsumer<Long, String> createKafkaConsumer(String topic) {
	public void createKafkaConsumer(String topic) {
		String consumerGrp = metaData.getJobID() + metaData.getTableID();
		String cientID = metaData.getJobID();

		kafkaMaxPollRecords = Integer.parseInt(conf.getConf("kafkaMaxPollRecords"));
		pollWaitMil = Integer.parseInt(conf.getConf("kafkaPollWaitMill"));

		Properties props = new Properties();
		props.put("bootstrap.servers", URL);
		props.put("group.id", consumerGrp);
		// props.put(ConsumerConfig.GROUP_ID_CONFIG, jobID);
		props.put("client.id", cientID);
		props.put("enable.auto.commit", "true");
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
		props.put("bootstrap.servers", URL);
		
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

	public int syncDataFrom(DataPointer srcData) {
		int rtc=2;
	    int rrn=0;
	    long seq=0l;
	    String srcTbl;
		//ProducerRecord<Long, String> aMsg;

	    List<String> tblList = metaData.getDB2TablesOfJournal(metaData.getSrcDBinfo().get("db_id").toString(), 
	        		(String) metaData.getTableDetails().get("SRC_TABLE")+"."+(String) metaData.getTableDetails().get("SRC_TABLE"));     
	        
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
	    						ovLogger.error("      exception at " + " " + aMsg.key() + ". Exit here!");
	    						ovLogger.error(e);
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
			ovLogger.info("   last Journal Seq #: " + seq);
			metrix.sendMX("JournalSeq,jobId="+metaData.getJobID()+",journal="+metaData.getTableDetails().get("SRC_TABLE")+" value=" + seq + "\n");
		} catch (SQLException e) {
			ovLogger.error("   failed to retrieve from DB2: " + e);
			rtc=0;   // ignore this one, and move on to the next one.
		} finally {
			srcData.releaseRSandSTMT();
		}
        return rtc;
}
	
	protected boolean miscPrep() {
		String topic = metaData.getTableDetails().get("src_schema")+"."
				+metaData.getTableDetails().get("src_table");
		super.miscPrep();
		createKafkaConsumer(topic);
		return true;
	}
	
	public void crtAuxSrcAsList() {
		int giveUp = Integer.parseInt(conf.getConf("kafkaMaxEmptyPolls"));
		int maxMsgConsumption = Integer.parseInt(conf.getConf("kafkaMaxMsgConsumption"));

		List<String> msgKeyListT=new ArrayList<String>();

		int noRecordsCount = 0, cntRRN = 0;

		while (true) {
			ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(pollWaitMil));
			// ConsumerRecords<Long, String> records = consumerx.poll(0);
			if (records.count() == 0) {  //no record? try again, after consective "giveUp" times.
				noRecordsCount++;
				ovLogger.info("    consumer poll cnt: " + noRecordsCount);
				if (noRecordsCount > giveUp)
					break; // no more records. exit
				else
					continue;
			}

			for (ConsumerRecord<Long, String> record : records) {
				//lastJournalSeqNum = record.key();
				msgKeyListT.add(record.value());
				cntRRN++;
				if(cntRRN>maxMsgConsumption) { //consume only maximum of "maxMsgConsumption" msgs.
					break;
				}
			}
			ovLogger.info("    read total msg: " + (cntRRN-1));
		}
		consumer.close();

		msgKeyList = msgKeyListT.stream()
	     .distinct()
	     .collect(Collectors.toList());
	    
		//metaData.end(rtc);
		metaData.setTotalMsgCnt(cntRRN-1);
	}
	protected List<String> getSrcResultList(){
		return msgKeyList;
	}

	public void setupSink() {
		createKafkaProducer();
	}
	
	/*
	public int writeKafka(byte[] myvar) {
	Producer<String, byte[]> 	producer = new KafkaProducer<String, byte[]>(props);
	  try {
        System.out.println("Sending message in bytes : " + myvar);
        System.out.println("... : " + java.util.Arrays.toString(myvar));
        ProducerRecord<String, byte[]> rec = new ProducerRecord<>("tes", myvar);
        producer.send(rec);
	} finally {
	      producer.close();
	    }	
	  
	  return 0;
  }

	public int writeKafka(GenericRecord avroRec, Schema schema) {
		byte[] myvar = avroToBytes(avroRec, schema);
		
	Producer<String, byte[]> 	producer = new KafkaProducer<String, byte[]>(props);
	  try {
       System.out.println("Sending message in bytes : " + myvar);
       System.out.println("... : " + java.util.Arrays.toString(myvar));
       ProducerRecord<String, byte[]> rec = new ProducerRecord<>("tes", myvar);
       producer.send(rec);
	} finally {
	      producer.close();
	    }	
	  
	  return 0;
 }
	  private byte[] avroToBytes(GenericRecord avroRec, Schema schema){
		  byte[] myvar=null;
		
		  DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		  ByteArrayOutputStream out = new ByteArrayOutputStream();
	      BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
	      
	      try {
			writer.write(avroRec, encoder);
		
	      encoder.flush();
	      myvar = out.toByteArray();
	      } catch (IOException e) {
	  		// TODO Auto-generated catch block
	  		e.printStackTrace();
	  	}
		return myvar;
		  
	  }

	
private void consumer() {	  
	...
	    props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

	    KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<String, byte[]>(props);
	    //kafkaConsumer.subscribe(Arrays.asList("tes"));
	    kafkaConsumer.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
	    
		try {
			Schema schema = new Schema.Parser().parse(new File("user.avsc"));
			
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);


		ByteArrayInputStream in;
	    while (true) {
	      ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(100);
	      for (ConsumerRecord<String, byte[]> record : records) {
	    	  
	        System.out.println("Partition: " + record.partition() 
	            + " Offset: " + record.offset()
	            + " Value: " + java.util.Arrays.toString(record.value()) 
	            + " ThreadID: " + Thread.currentThread().getId()  );

	    	in = new ByteArrayInputStream(record.value());
	  		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
	  		
	  		GenericRecord a = reader.read(null, decoder);
	  		System.out.println(a);
	  		System.out.println(a.get("name"));
	  		System.out.println(a.get("favorite_number"));
	  		System.out.println(a.get("favorite_color"));
	      }
	    }
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	  }
public void testConsumer() {
...
	props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<String, byte[]>(props);
//kafkaConsumer.subscribe(Arrays.asList("tes"));
kafkaConsumer.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));

try {
	Schema schema = new Schema.Parser().parse(new File("user.avsc"));
	
	DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);


ByteArrayInputStream in;
while (true) {
  ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(100);
  for (ConsumerRecord<String, byte[]> record : records) {
	  
    System.out.println("Partition: " + record.partition() 
        + " Offset: " + record.offset()
        + " Value: " + java.util.Arrays.toString(record.value()) 
        + " ThreadID: " + Thread.currentThread().getId()  );

	in = new ByteArrayInputStream(record.value());
		BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(in, null);
		
		GenericRecord a = reader.read(null, decoder);
		System.out.println(a);
		System.out.println(a.get("name"));
		System.out.println(a.get("favorite_number"));
		System.out.println(a.get("favorite_color"));
  }
}
} catch (IOException e) {
	// TODO Auto-generated catch block
	e.printStackTrace();
}
}
void testProducer(){
	  Schema schema;
	  GenericRecord avroRec;
	//  Producer<String, byte[]> producer = null;
	  try {
		schema = new Schema.Parser().parse(new File("user.avsc"));
		///////
    	 MetaRep rep = new MetaRep();
    	 String strSch = rep.getAvroSchema("user");
    	 Schema schema = new Schema.Parser().parse(strSch);
    	GenericRecord avroRec;
       while ( rs.next() ) {
          // int cnt = rs.getInt("cnt");
           System.out.println(rs.getString("name"));
           
           avroRec = new GenericData.Record(schema);
   		avroRec.put("name", rs.getString("name"));
   		avroRec.put("favorite_number", rs.getInt("fav_num"));
   		avroRec.put("favorite_color", rs.getString("fav_col"));

   		System.out.println("Original Message : "+ avroRec);
   		
   		kafka.writeKafka(avroRec, schema);
           
       }

		///////
		
		
		avroRec = new GenericData.Record(schema);
		avroRec.put("name", "Ben");
		avroRec.put("favorite_number", 7);
		avroRec.put("favorite_color", "red");

		System.out.println("Original Message : "+ avroRec);
		
		byte[] myvar = avroToBytes(avroRec, schema);

		writeKafka(myvar);

	  } catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
		 //     producer.close();
		    }
	  
}
*/ 
	public void close() {
		try {
		consumer.close();
		producer.close();
		}catch(NullPointerException e) {
			ovLogger.info("      nothing to close.");
		}
	}
	/*
	 * public boolean tblRefreshTry() { KafkaConsumer<Long, String> consumerx =
	 * createKafkaConsumer("JOHNLEE2.TESTTBL2");
	 * 
	 * int noRecordsCount=0, cntRRN=0; int giveUp=10; String rrnList=""; long
	 * lastJournalSeqNum=0l;
	 * 
	 * while (true) { ConsumerRecords<Long, String> records = consumerx.poll(0);
	 * 
	 * if (records.count()==0) { noRecordsCount++; if (noRecordsCount > giveUp)
	 * break; else continue; }
	 * 
	 * for (ConsumerRecord<Long, String> record : records) {
	 * //System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(),
	 * record.key(), record.value()); if(cntRRN==0) rrnList = record.value(); else
	 * rrnList = rrnList + "," + record.value(); lastJournalSeqNum=record.key();
	 * cntRRN++; }
	 * 
	 * //in case there are more in Kafka broker: rrnList=""; noRecordsCount=0;
	 * 
	 * for (ConsumerRecord<Long, String> record : records) {
	 * 
	 * System.out.println("key: " + record.key()); System.out.println("value: " +
	 * record.value());
	 * 
	 * System.out.println("Partition: " + record.partition() + " Offset: " +
	 * record.offset() + " Value: " + record.value() + " ThreadID: " +
	 * Thread.currentThread().getId() );
	 * 
	 * } }
	 * 
	 * return true; }
	 */
}