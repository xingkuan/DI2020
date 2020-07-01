create database didb;

\c didb

CREATE TABLE DATA_POINT
(
  DB_ID         VARCHAR(15)   PRIMARY KEY,
  DB_CAT        VARCHAR(15),
  DB_TYPE       VARCHAR(15),
  DB_USR        VARCHAR(25),
  DB_PWD        VARCHAR(25),
  DB_DRIVER     VARCHAR(150),
  DB_CONN       VARCHAR(150),
  DB_INFO       VARCHAR(225)
)
;
-- Let's explicitly know that the DB are RDBMS or KAFKA, for both SRC, TGT and DCC
CREATE TABLE SYNC_TABLE
(
  TBL_ID              INTEGER PRIMARY KEY,
  TEMP_ID			  VARCHAR(20),  --DJ2K, D2V, O2V, D2K, O2K...
  TBL_PK              VARCHAR(50),
  POOL_ID             INTEGER,
  INIT_DT	          DATE,
  INIT_DURATION       INTEGER,
  CURR_STATE          INTEGER,
  SRC_DB_ID           VARCHAR(15),
  SRC_SCHEMA          VARCHAR(25),
  SRC_TABLE           VARCHAR(50),
  SRC_DCC_PGM         VARCHAR(30), --either trigger name as sch.trig or EXT
  SRC_DCC_TBL         VARCHAR(30), --either sch.tbl or jrnl_lib.jrnl_member(for DB2)
  --SRC_STMTS			  JSONB, --{"init": {"setup": ... ,"teardown": ...},"syn": {"setup":... "teardown":...}}
  TGT_DB_ID           VARCHAR(15),
  TGT_SCHEMA          VARCHAR(25),
  TGT_TABLE           VARCHAR(50),
  --TGT_STMTS			  JSONB, --{"init": {"setup": ... ,"teardown": ...},"syn": {"setup":... "teardown":...}}
  DCC_DB_ID           VARCHAR(15),  -- eith SRC_DB_ID or a KAFKA. More likely KAFKA only!!!
                                    -- SRC_STMTS will pull in this field (and the next one)
                                    -- to compose the steps for extract src data.    
                      -- Also, if DCC_DB_ID!=SRC_DB_ID, another entry will be automatically created, for extract
                      -- CCD keys into KAFKA topics
  DCC_STORE           VARCHAR(50),  -- Basically Kafka topic name; =src_dcc_tbl
  TS_REGIST           TIMESTAMP(6),
  TS_LAST_REF         TIMESTAMP(6),
  SEQ_LAST_REF        BIGINT,
  CONSTRAINT unique_src UNIQUE (SRC_DB_ID, SRC_SCHEMA, SRC_TABLE)
)
; 

CREATE TABLE SYNC_TABLE_FIELD
(
  TBL_ID 			INTEGER,
  FIELD_ID          INTEGER,
  SRC_FIELD         VARCHAR(50),
  SRC_FIELD_TYPE    VARCHAR(20),
  SRC_FIELD_LEN     INTEGER,
  SRC_FIELD_SCALE   INTEGER,
  TGT_FIELD         VARCHAR(50),
  TGT_FIELD_TYPE    VARCHAR(20),
  JAVA_TYPE         INT,
  AVRO_TYPE         VARCHAR(80),
  primary key (tbl_id, field_id)
)
;
CREATE TABLE SYNC_TEMPLATE
(
  TEMP_ID    VARCHAR(20),  --DJ2K, D2V, O2V, D2K, O2K...
  ACT_ID     INTEGER,      --0: enable(trigger, jurl extr); 1: init tbl; 2: dcc; 3: syn; 4: syn via kafka
  INFO       VARCHAR(100),
  STMTS		 JSONB,        --ideally, configurable SQLs or whatever
  primary key (temp_id, act_id)
)
;

-- Simple transformation. target field and the transformation function,
-- between AVRO (or JSON) records
CREATE TABLE AVRO
(
   AVRO_ID varchar(50) PRIMARY KEY, 
   AVRO_SCHEMA jsonb
);

INSERT INTO avro_schema (avro_id, avro_schema ) 
VALUES ('user',  
'{"namespace": "example.avro", 
 "type": "record", 
 "name": "User", 
 "fields": [ 
     {"name": "name", "type": "string"}, 
     {"name": "favorite_number",  "type": ["int", "null"]}, 
     {"name": "favorite_color", "type": ["string", "null"]} 
 ]  
}');


CREATE TABLE XFORM0  (
   X_ID        INTEGER PRIMARY KEY,
   SRC_DB_ID   CARCHAR(50),
   SRC_NAME    VARCHAR(50),
   SRC_AVRO_ID CARCHAR(50),
   TGT_DB_ID   VARCHAR(15),
   TGT_NAME    VARCHAR(50),
   TGT_AVRO    JSON,
   XFORM0      JSON
)
;

create user repuser password 'passwd';
grant connect on database didb to repuser;
grant all privileges on database didb to repuser;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES in schema public to repuser;

---------------------------------------------------
insert into DATA_POINT (
  DB_ID,
  DB_CAT, DB_TYPE,
  DB_USR, DB_PWD,
  DB_DRIVER, DB_CONN,
  DB_INFO)
values 
('DB2D', 
 'RDBMS', 'DB2/AS400',
 'johnlee2', 'C3line1998', 
 'com.ibm.as400.access.AS400JDBCDriver',
 'jdbc:as400://DEVELOPM:2551/DB2_RETSYS', 
 'DB2/AS400 Dev'),
('DB2T', 
 'RDBMS', 'DB2/AS400',
 'VERTSYNC', 'G123UESS', 
 'com.ibm.as400.access.AS400JDBCDriver',
 'jdbc:as400://XXXX:2551/XXX', 
 'DB2/AS400 Test'),
('VERTX', 
 'RDBMS', 'VERTICA',
 'dbadmin', 'Bre@ker321', 
 'com.vertica.jdbc.Driver',
 'jdbc:vertica://vertx1:5433/vertx', 
 'Vert x'),
('KAFKA1', 
 'MQ', 'KAFKA',
 'xxx', 'xxx', 
 '',
 'usir1xrvkfk01:9092,usir1xrvkfk02:9092,usir1xrvkfk03:9092', 
 'kafka 1'),
('ORA1', 
 'RDBMS', 'ORACLE',
 'johnlee', 'johnlee213', 
 'oracle.jdbc.OracleDriver',
 'jdbc:oracle:thin:@172.27.136.136:1521:CRMP64', 
 'Oracle Dev'),
('ES1', 
 'Search Engine', 'ES',
 'xxx', 'xxx', 
 '',
 'http://dbatool02:9200', 
 'ElasticSearch')
;

insert into SYNC_TEMPLATE
(
  TEMP_ID, ACT_ID, INFO
) values 
('DJ2K', 0, 'set meta_table.SEQ_LAST_REF to a starting seq.'),
('DJ2K', 2, 'sync(extract) DCC to kafka.'),
('D2V_', 0, 'simply set meta_table.curr_state=2.'),
('D2V_', 1, 'intial copy src to tgt.'),
('D2V_', 2, 'sync src to tgt via kafka'),
('D2V_', 9, 'audit'),
('D2V', 1, 'ex. temp: intial copy src to tgt.'),
('D2V', 2, 'ex. temp: sync src to tgt via trig.'),
('D2V', 9, 'audit.'),
('D2K_', 2, 'sync src data to kafka topic via kafka.'),
('O2V', 0, 'enable trig'),
('O2V', 1, 'initial copy src to tgt'),
('O2V', 2, 'sync src to tgt via trig'),
('O2V', 9, 'audit'),
('O2K', 0, 'enable trig'),
('O2K', 2, 'sync src to tgt(kafka) via trig'),
('O2K', 21, 'testing code'),
('O2K_', 2, 'sync src data to kafka topic via kafka.'),
('K2E', 2, 'Kafak topic to ES doc')
;

---------------------
create table dp_job(job_id varchar(30), job_desc varchar(100), job_stmt jsonb);
grant all on dp_job to myavro;
INSERT INTO dp_job (job_id, job_desc, job_stmt) 
VALUES ('job 1', 'test ...',   
'{"src_avro": "user", 
  "src_url": "jdbc:oracle:thin:@crmdbclonetest:1521:crmp65", 
  "userID": "system",
  "userPW": "lanchong", 
  "src_driver": "oracle.jdbc.OracleDriver",
  "stmts": [
    {"name":"step 1",
     "stmt":"insert into johnlee.tuser (name, fav_num, fav_col) values (''from job 1'', 8, ''red'') ", 
     "rs": "n" },
    {"name":"step 2", 
     "stmt":"commit",
     "rs": "n" },
    {"name":"step 3",
     "stmt":"select * from johnlee.tuser",
     "rs": "y"}
  ]
}'
);
[[[ in Oracle: create table johnlee.tuser (name varchar2(20), fav_num int, fav_col varchar2(10)); ]]]

