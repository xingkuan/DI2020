module projDI {
	requires java.sql;
	requires kafka.clients;
	requires json.simple;
	requires org.apache.logging.log4j;
	requires jt400;
	requires vertica.jdbc;
	requires ojdbc6;
	requires org.apache.avro;
	requires com.fasterxml.jackson.core;
	requires com.fasterxml.jackson.annotation;
}