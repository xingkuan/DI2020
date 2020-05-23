package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.text.*;
import java.time.Duration;
import java.sql.*;
import oracle.jdbc.*;
import oracle.jdbc.pool.OracleDataSource;

import org.apache.logging.log4j.Logger;

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

	//public KafkaData(String url, String cls, String user, String pwd) {
	public KafkaData(String dID) throws SQLException {
		// super(dbid, url, cls, user, pwd);
		super(dID);
	}

	public KafkaConsumer<Long, String> createKafkaConsumer(String topic) {
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

		KafkaConsumer<Long, String> consumer = new KafkaConsumer<Long, String>(props);
		// consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
		consumer.subscribe(Arrays.asList(topic));

		return consumer;
	}

	public ConsumerRecords<Long, String> readMessages() {
		// ConsumerRecords<Long, String> records =
		// kc.poll(Duration.ofMillis(pollWaitMil));
		return consumer.poll(Duration.ofMillis(pollWaitMil));
	}

	private KafkaProducer<Long, String> createKafkaProducer() {
		String cientID = metaData.getJobID() + "_" + metaData.getAuxPoolID();

		String strVal = conf.getConf("kafkaMaxBlockMS");
		String kafkaACKS = conf.getConf("kafkaACKS");
		String kafkaINDEM = conf.getConf("kafkaINDEM");
		int kafkaMaxBlockMS = Integer.parseInt(strVal);

		Properties props = new Properties();
		props.put("bootstrap.servers", URL);
		
	    props.put("acks", kafkaACKS);
	    props.put("enable.idempotence", kafkaINDEM);
	    //props.put("message.send.max.retries", kafkaSendTries);
	//    props.put("retries", kafkaSendTries);
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

		return producer;
	}

	public void syncDataFrom(DataPointer srcData) {
		boolean rtc = false;
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
	                        }
	                    }
	                });
					
				}
			}
			rtc=true;
			ovLogger.info("   last Journal Seq #: " + seq);
			metrix.sendMX("JournalSeq,jobId="+metaData.getJobID()+",journal="+metaData.getTableDetails().get("SRC_TABLE")+" value=" + seq + "\n");
		} catch (SQLException e) {
			ovLogger.error("   failed to retrieve from DB2: " + e);
			rtc=true;   // ignore this one, and move on to the next one.
		} finally {
			srcData.releaseRSandSTMT();
		}
}
	
	protected boolean miscPrep() {
		super.miscPrep();
		return true;
	}
	public void setupSink() {
		createKafkaProducer();
	}
	
	public void close() {
		consumer.close();
		producer.close();
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