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
//import oracle.jdbc.*;
//import oracle.jdbc.pool.OracleDataSource;

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


class KafkaKey extends DataPoint  {
	Conf conf = Conf.getInstance();
	protected final String logDir = conf.getConf("logDir");
	private static final Logger logger = LogManager.getLogger();

	private KafkaConsumer<Long, String> consumer;
	private KafkaProducer<Long, String> producer;

	protected Properties props = new Properties();
	
	int kafkaMaxPollRecords;
	int pollWaitMil;

	//TODO: Int can be more efficient. But about String, like Oracle ROWID?
	private List<String> msgKeyList=new ArrayList<String>();
	
	private int rtc=2;
    private int rrn=0;
    private long seq=0l;
    private String srcTbl;
    private List<String> activeTblList;
    
	public KafkaKey(JSONObject dID) {
		super(dID);
	}
	
	@Override
	public void setDetail(JSONObject dtl) {
		dataDetail=dtl;
		if(dataDetail.get("dbRole")=="R"){
			setupConsumer();
		}else if (dataDetail.get("dbRole")=="W") {
			setupProducer();
		}
	}
	
	private void setupConsumer() {
		// TODO Auto-generated method stub
		int cnt=0;
		List alist = null;

		TaskMeta metaData = TaskMeta.getInstance();
		
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
		props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		KafkaConsumer consumer = new KafkaConsumer<Long, String>(props);
		// consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
		String topic="JOHNLEE2.TESTTBL2";
		consumer.subscribe(Arrays.asList(topic));

		//return consumer.poll(Duration.ofMillis(pollWaitMil));
//		return Arrays.asList(alist, true);
	}

	private void setupProducer() {
		TaskMeta metaData = TaskMeta.getInstance();

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

		props.put("key.deserializer", "org.apache.kafka.common.serialization.LongDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		consumer = new KafkaConsumer<Long, String>(props);
		// consumerx.subscribe(Arrays.asList("JOHNLEE2.TESTTBL2"));
		//consumer.subscribe(Arrays.asList(topic));

		//return consumer;
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
		// props.put("value.serializer",
		// "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		producer = new KafkaProducer<Long, String>(props);
		//Note: If the brokers are down, it will keep trying forever ... seems no way for
		//      checking and handling . Weird !!!

		//return producer;
		
		iniActiveTblList();
	}

	@Override
	public int upwrite(GenericRecord rs, int fldCnt) {	
		//shouldn't be needed: why replicate kafka to kafka?
		return 0;
	}
	@Override
	public int upwrite(ResultSet rs, int fldCnt) {	//JDBC to Kafka as key id.
		//fldCnt got to be 1; it is not used here anyway
		
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
							//rtc = -1;  //TODO: how to detect and handle errors here?
	                    }
//			metaData.getMiscValues().put("thisJournalSeq", aMsg.key());
	                }
	            });
			}
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		return kafkaMaxPollRecords;
	}

	@Override
	public List<String> getDCCkeys() {
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
//		metaData.setTotalMsgCnt(cntRRN-1);
		return msgKeyList;
	}
	

	private void iniActiveTblList(){
		TaskMeta metaData = TaskMeta.getInstance();

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

	public void close() {
		try {
		consumer.close();
		producer.close();
		}catch(NullPointerException e) {
			logger.info("      nothing to close.");
		}
	}


/******** Registration APIs **********/
	/******** admin client, for registration/unregistration ****/
	protected AdminClient createKafkaAdmin() {
		Properties adminProps = new Properties();
		adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, urlString);
	
		AdminClient admin = AdminClient.create(adminProps);
		return admin;
	}

	@Override
	public int runDBcmd(String cmd, String type) {
		TaskMeta metaData = TaskMeta.getInstance();
		
		Map taskDetail = metaData.getTaskDetails();
		String topicName=(String) taskDetail.get("topic");

		if(cmd.equals("rgist")) {
			int partitions=2;
			short replicationFactor=2;
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
		}else if(cmd.equals("unregist")) {
			//delete the topic
			String theTopic = metaData.getTaskDetails().get("tgt_schema")+"."
					+ metaData.getTaskDetails().get("tgt_table");
			if(!theTopic.equals("*.*")){
			try (final AdminClient adminClient = createKafkaAdmin()) {
		        DeleteTopicsResult deleteTopicsResult=adminClient.deleteTopics(Arrays.asList(theTopic));
		    }
			
			}
		}
		return 0;
	}
	

}