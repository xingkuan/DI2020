package com.future.DI2020;

import java.io.*;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collector;
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
import org.json.simple.JSONArray;
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


class KafkaDCC extends Kafka {
	Conf conf = Conf.getInstance();
	protected final String logDir = conf.getConf("logDir");
	private static final Logger logger = LogManager.getLogger();

	private KafkaConsumer<Long, String> consumer;
	private KafkaProducer<Long, String> producer;

	int kafkaMaxPollRecords;
	int pollWaitMil;

	//TODO: Int can be more efficient. But about String, like Oracle ROWID?
	private List<String> msgKeyList=new ArrayList<String>();
	
	private int rtc=2;
    private int rrn=0;
    private long seq=0l;
    private String srcTbl;
    private List<String> activeTblList;
    
	public KafkaDCC(JSONObject dID, String role) {
		super(dID, role);
	}
	@Override
	protected void createKafkaConsumer() {
		setConsumerProps();
		props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		consumer = new KafkaConsumer<Long, String>(props);
		// consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
		//consumer.subscribe(Arrays.asList(topic));

		//return consumer;
	}

	public ConsumerRecords<Long, String> readMessages() {
		return consumer.poll(Duration.ofMillis(pollWaitMil));
	}

	private void createKafkaProducer() {
		setProducerProps();
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		// props.put("value.serializer",
		// "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		producer = new KafkaProducer<Long, String>(props);
		//Note: If the brokers are down, it will keep trying forever ... seems no way for
		//      checking and handling . Weird !!!

		//return producer;
	}

	@Override
	public void setupSink() {   //sink in the sense of this app; but it is a Kafka producer.  
		createKafkaProducer();

		String journal=metaData.getTaskDetails().get("tgt_schema")+"."+metaData.getTaskDetails().get("tgt_table");
		
		String jsonSch = metaData.getAvroSchema();
		iniActiveTblList();
		//schema = new Schema.Parser().parse(new File("user.avsc"));
		//Producer<Long, GenericRecord> producer = createKafkaAVROProducer();
		//createKafkaProducer();
	}
	private void iniActiveTblList(){
		String tblSrcDB=metaData.getTaskDetails().get("src_db_id").toString();
		String tblSrcSch=metaData.getTaskDetails().get("src_schema").toString();
		String tblSrcTbl=metaData.getTaskDetails().get("src_table").toString();
		String strSQL = "select src_schema||'.'||src_table as name from task "
				+ "where src_db_id ='" + tblSrcDB + "' "
				+ "and src_dcc_tbl='" + tblSrcSch + "." + tblSrcTbl + "' "
				+ "and tgt_schema !='*' order by 1";
		JSONArray ja = metaData.SQLtoJSONArray(strSQL);
	    activeTblList = (List<String>) ja.stream()
	    		.map( o -> ((JSONObject) o).get("name"))
	    		.collect(Collectors.toList());
	}
	@Override
	public void write(ResultSet rs) {
		try {
			rrn=rs.getInt("RRN");
			seq=rs.getLong("SEQNBR");
			srcTbl=rs.getString("SRCTBL");
			// ignore those for unregister tables:
			if (activeTblList.contains(srcTbl)) {
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
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	@Override
	protected boolean miscPrep() {
		super.miscPrep();

		//String jTemp=metaData.getTaskDetails().get("template_id").toString();
		String jTemp=metaData.getActDetails().get("act_id").toString()+metaData.getActDetails().get("template_id"); 
		//if(jTemp.equals("D2V_")) {
		if(jTemp.equals("2DATA_")) {
			String topic = metaData.getTaskDetails().get("src_schema")+"."
				+metaData.getTaskDetails().get("src_table");
			createKafkaConsumer();
			consumer.subscribe(Arrays.asList(topic));
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

		//List<String> msgKeyListT=new ArrayList<String>();

		int noRecordsCount = 0, cntRRN = 0;

		while (true) {
			//ConsumerRecords<Long, String> records = consumer.poll(Duration.ofMillis(pollWaitMil));
			ConsumerRecords<Long, String> records = consumer.poll(300);
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
				msgKeyList.add(record.value());
				cntRRN++;
				//if(cntRRN>maxMsgConsumption) { //consume only maximum of "maxMsgConsumption" msgs.
				//	break;
				//}
			}
			logger.info("    read total msg: " + (cntRRN-1));
		}
		//consumer.commitSync();  //TODO: little risk here. Ideally, that should happen after data is save in tgt.
		//consumer.close();       //   so to be called in the end of sync

		msgKeyList = msgKeyList.stream()
	     .distinct()
	     .collect(Collectors.toList());
	    
		//metaData.end(rtc);
		metaData.setTotalMsgCnt(cntRRN-1);
	}
	public void commit() {
		//consumer.commitSync();  
		//byteProducer.commitTransaction();
		if(producer!=null)
			producer.close();
		if(consumer!=null)
			consumer.close();
	}
	public void rollback() {
		//consumer.commitSync();  
		consumer.close();
	}

	protected List<String> getDCCKeyList(){
		return msgKeyList;
	}


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
	//TODO: no action.
/*	int partitions=2;
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
    */
	return true;
}
@Override
//public boolean unregisterDcc(int tblID) {
public boolean unregisterTgt(int tblID) {
	//TODO: you don't create this topic, and no need for unregist it!
	/*String theTopic = metaData.getTaskDetails().get("src_schema")+"."
			+ metaData.getTaskDetails().get("src_table");
	try (final AdminClient adminClient = createKafkaAdmin()) {
        DeleteTopicsResult deleteTopicsResult=adminClient.deleteTopics(Arrays.asList(theTopic));
    }*/
	return true;
}

}