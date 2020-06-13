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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
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
		props.put("bootstrap.servers", URL);
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
	
	protected boolean miscPrep(String jobTempID) {
		super.miscPrep(jobTempID);

		if(metaData.getTableDetails().get("temp_id").toString().equals("D2V_")) {
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
				//if(cntRRN>maxMsgConsumption) { //consume only maximum of "maxMsgConsumption" msgs.
				//	break;
				//}
			}
			ovLogger.info("    read total msg: " + (cntRRN-1));
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

	public void setupSink() {
		createKafkaProducer();
	}

	// ------ AVRO related --------------------------------------------------------------------
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
		props.put("bootstrap.servers", URL);
			
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
  
	public int syncAvroDataFrom(DataPointer srcData) {
		int rtc=2;
		int cnt=0;
		ResultSet rs=srcData.getSrcResultSet();
		ArrayList<Integer> fldType = metaData.getFldJavaType();

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
						tempNum = rs.getDate(i+1).getTime();
						record.put(i, tempNum);
						}
				//		break;
				//	case 6:
				//		record.put(i, new java.util.Timestamp(rs.getTimestamp(i+1).getTime()));
						break;
					default:
						ovLogger.warn("unknow data type!");
						record.put(i, rs.getString(i+1));
						break;
					}
				}
				//String temp = rs.getString(fldNames.size());
				//record.put(fldNames.size()-1, rs.getString(fldNames.size()));

	    	   /*
	    	    * use byte, instead; ideally, use Confluent's Schena Registry 
	    	    */
	    	   //producer.send(new ProducerRecord<Long, GenericRecord>("topica", (long) 1, record));
		   		byte[] myvar = avroToBytes(record, schema);
		   		producer.send(new ProducerRecord<Long, byte[]>("VERTSNAP.TESTOK", (long) 1, myvar),new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //execute everytime a record is successfully sent or exception is thrown
                    	if(e == null){
                        }else{
    						ovLogger.error(e);
                        }
                    }
                });
		   		cnt++;
	       }
		  } catch (SQLException e) {
			  ovLogger.error(e);
		  }
		  //return cnt;
		  return rtc;
	}
	
	private byte[] avroToBytes(GenericRecord avroRec, Schema schema){
		byte[] myvar=null;
			
		  DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
		  ByteArrayOutputStream out = new ByteArrayOutputStream();
		  BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
		      
		  try {
			writer.write(avroRec, encoder);
			
		    encoder.flush();
			out.close();
		    myvar = out.toByteArray();
		  } catch (IOException e) {
	  		ovLogger.error(e);
	  	}
		return myvar;
	  }

    Properties propsC = new Properties();
    {
    propsC.put("bootstrap.servers", "usir1xrvkfk01:9092");
    propsC.put("group.id", "group-1");
    propsC.put("enable.auto.commit", "true");
    propsC.put("auto.commit.interval.ms", "1000");
    propsC.put("auto.offset.reset", "earliest");
    propsC.put("session.timeout.ms", "30000");
    propsC.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
    }
	public void testConsumer() {
		propsC.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

	KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<String, byte[]>(propsC);
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
			System.out.println(a.get(1));
			System.out.println(a.get(2));
			System.out.println(a.get(3));
	  }
	}
	} catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	}

//--------------------------------------------------------------------------------------------------------
public void close() {
		try {
		consumer.close();
		producer.close();
		}catch(NullPointerException e) {
			ovLogger.info("      nothing to close.");
		}
	}
}