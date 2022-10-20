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
  DB_INFO       VARCHAR(225),
  INSTRUCTIONS	JSONB    --the templates of registration, initialization, remove, incremental
)
;
-- Let's explicitly know that the DB are RDBMS or KAFKA, for both SRC, TGT and DCC
CREATE TABLE TASK
(
  TASK_ID             INTEGER PRIMARY KEY,
  POOL_ID             INTEGER,
  CURR_STATE          INTEGER,
  SRC_DB_ID           VARCHAR(15),
  SRC_TABLE           VARCHAR(50),
  MBR_LST       text, --DB2/400 journal case: list of tables to extract from journal
  SRC_SQL             text ,  --if customer SQL is used.
  TGT_DB_ID           VARCHAR(15),
  TGT_TABLE           VARCHAR(50),
  TS_REGIST           TIMESTAMP(6),
  INIT_DT	          DATE,
  INIT_DURATION       INTEGER,
  TS_LAST_REF         TIMESTAMP(6),
  SEQ_LAST_REF        BIGINT
--  CONSTRAINT unique_src UNIQUE (SRC_DB_ID, SRC_SCHEMA, SRC_TABLE)
)
; 

CREATE TABLE DATA_FIELD
(
  TASK_ID 			INTEGER,
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

-- Simple transformation. target field and the transformation function,
--                        between AVRO (or JSON) records;
-- (for more needing transformation, use Sparks-based transformation).
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


CREATE TABLE XFORM_SIMPLE  (
   X_ID        INTEGER PRIMARY KEY,  --Let it be that of task_id
   SRC_AVRO    varchar(2000),
   TGT_AVRO    varchar(2000),
   XFORM0      varchar(2000)
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
  DB_INFO,
  INSTRUCTIONS)
values 
('DB2KS', 
 'JDBC', 'DB2/AS400',
 'johnlee2', 'C3line1998', 
 'com.ibm.as400.access.AS400JDBCDriver',
 'jdbc:as400://DEVELOPM:2551/DB2_RETSYS', 
 'DB2/AS400 data id',
 '{	"bareSQL": "select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR 
				 FROM table (Display_Journal('<JLLIB = srcschema>', '<JNAME = srctbl >', '*CURCHAIN', 
					rName , cast('<STARTTS>' as TIMESTAMP),  
					cast('<STARTSEQ>' as decimal(21,0)),  
					'R',  
					'',  
					'' ,  '' , '*QDDS', '',  
					'', '', ''  
					) ) as x order by 2 asc",
     "rs": "n" 
     },
  "registration": [
  		... one journal member can correspond to multiple tables -> so will be a list of table
    {"name":"step 1",
     "stmt":"insert into task (taskid, srcdb, srctbl, mbr_lst, tgtdb, tgttbl,regist_ts)
     			values(<TASKID>, '<SRDDBID>', '<SRCTBL>', '<MBRLIST>', '<TGTDBID>',
     			'<TGTTBLID>', '<REGITTS>')", 
     "rs": "n" },
  ]
  }
  "remove": [],
  "initRun": [ 
  	{"name":"xxx",
     "stmt":"SELECT current timestamp FROM sysibm.sysdummy1;
			String sqlStmt = " select COUNT_OR_RRN as RRN,  SEQUENCE_NUMBER AS SEQNBR"
					+ " FROM table (Display_Journal('" + jLibName + "', '" + jName + "', " + "   '" + rLib + "', '"
					+ rName + "', " + "   cast('" + strTS + "' as TIMESTAMP), " // pass-in the start timestamp;
					+ "   cast(null as decimal(21,0)), " // starting SEQ #
					+ "   'R', " // JOURNAL CODE:
					+ "   ''," // JOURNAL entry:UP,DL,PT,PX
					+ "   '" + srcSch + "', '" + srcTbl + "', '*QDDS', ''," // Object library, Object name, Object type,
																			// Object member
					+ "   '', '', ''" // User, Job, Program
					+ ") ) as x order by 2 asc"; 
     	"rs": "n" 
     }
  ],
  "incrementalRun": [
  		"DECLARE GLOBAL TEMPORARY TABLE qtemp.DCC"+taskID + "(" + tskDetailJSON.get("data_pk") + " " + keyDataType + ") " 
				+" NOT LOGGED"; 

		"INSERT INTO qtemp.DCC" + taskID + " VALUES (?)" ;

		sql = getBareSrcSQL() + ", qtemp.DCC"+taskID + " b "
				+ " where a..rrn(a)=b." +tskDetailJSON.get("data_pk");  //TOTO: may have problem!
  
  ]
  }
  }'
 ),
insert into DATA_POINT (
  DB_ID,
  DB_CAT, DB_TYPE,
  DB_USR, DB_PWD,
  DB_DRIVER, DB_CONN,
  DB_INFO,
  INSTRUCTIONS)
values 
('DB2DS', 
 'JDBC', 'DB2/AS400',
 'johnlee2', 'C3line1998', 
 'com.ibm.as400.access.AS400JDBCDriver',
 'jdbc:as400://DEVELOPM:2551/DB2_RETSYS', 
 'DB2/AS400 data',
 '{	"selectStmt": "select a.*, DATAKEY from SRCTBLE ",
   "registration": [
  	"checking":	... better contains a checking verifying src table is in the list of the "data keys"... ,
  	"create target": "create table ...."
  ],
  "remove": [],
  "initRun": [   ],
  "incrementalRun": []
  }
  }'
 ),
('VERT1DT', 
 'JDBC', 'VERTICA',
 'dbadmin', 'Bxx321', 
 'com.vertica.jdbc.Driver',
 'jdbc:vertica://vertx1:5433/vertx', 
 'Vert x',
 '{	"createStmt": "create table <TGTTBLNAME> <FLDDEFINTIONS>",
    "registration": [
  	"grant ... ",
  	"grant ...."
    ],
  "remove": ["drop table <TBLNAME>"],
  "initRun": [   ],
  "incrementalRun": []
 }'
 );
 
 
 insert into DATA_POINT (
  DB_ID,
  DB_CAT, DB_TYPE,
  DB_USR, DB_PWD,
  DB_DRIVER, DB_CONN,
  DB_INFO,
  INSTRUCTIONS)
values 
 ('KAFKA1DT', 
 'MQ', 'KAFKA',
 'xxx', 'xxx', 
 '', 'usir1xrvkfk01:9092,usir1xrvkfk02:9092,usir1xrvkfk03:9092', 
 'kafka data sink',
 '{	
   "createStmt": {"topicname", "xxx"},
   "registration": [    ],
  "remove": [],
  "initRun": [   ],
  "incrementalRun": []
 }'
 ),
 ('KAFKA1DS', 
 'MQ', 'KAFKA',
 'xxx', 'xxx', 
 '', 'usir1xrvkfk01:9092,usir1xrvkfk02:9092,usir1xrvkfk03:9092', 
 'kafka data consumer',
  ''
 );
 insert into DATA_POINT (
  DB_ID,
  DB_CAT, DB_TYPE,
  DB_USR, DB_PWD,
  DB_DRIVER, DB_CONN,
  DB_INFO,
  INSTRUCTIONS)
values 
 ('KAFKA1DT', 
 'MQ', 'KAFKA',
 'xxx', 'xxx', 
 '', 'usir1xrvkfk01:9092,usir1xrvkfk02:9092,usir1xrvkfk03:9092', 
 'kafka data sink',
  ''
 ),
 ('KAFKA1DS', 
 'MQ', 'KAFKA',
 'xxx', 'xxx', 
 '', 'usir1xrvkfk01:9092,usir1xrvkfk02:9092,usir1xrvkfk03:9092', 
 'kafka data consumer',
  ''
 );
 insert into DATA_POINT (
  DB_ID,
  DB_CAT, DB_TYPE,
  DB_USR, DB_PWD,
  DB_DRIVER, DB_CONN,
  DB_INFO,
  INSTRUCTIONS)
values('ORA1DS', 
 'JDBC', 'ORACLE',
 'johnlee', 'johnlee213', 
 'oracle.jdbc.OracleDriver',
 'jdbc:oracle:thin:@172.27.136.136:1521:CRMP64', 
 'Oracle Dev',
 '{	"bareSQL": "select * from a.* from <SRCTBL> a ";
 	"registration": [
    {"db":"ORA1D", 
     "name":"create log tbl",
     "stmt":"create table <SRCTBL>_DCCLOG (datakey varchar(20), action char[1], ts_dcc date) tablespace TSDCC ", 
     "rs": "n" },
    {"db":"ORA1DS",
     "name":"create log trigger", 
     "stmt":"create or replace trigger <SRCTBL>_DCCTRG   
				after insert or update or delete on SRCTBLE  
				for each row  
				begin
				DECLARE
     				action  char(1);
				BEGIN 
				IF DELETING THEN 
 					action := 'D';
				END IF;
				IF INSERTING THEN 
 					action := 'I';
				END IF;
				IF UPDATING THEN 
 					action := 'U';
				END IF;
				
				insert into <SRCTBL>_DCCLOG (datakey, action, ts_dcc )\n"  
				values ( :new.rowid, sysdate   );  
				end; \n",
     "rs": "n" },
    {"db":"ORA1DS",
     "name":"the disabled trigger",
     "stmt":"alter trigger TRIGNAME disable;",
     "rs": "n"}
  ],
  "remove": [
    {"db":"ORA1DS",
     "name":"step 1",
     "stmt":"drop table LOGTBLNAME ", 
     "rs": "n" },
    {"db":"ORA1DS",
     "name":"step 2", 
     "stmt":"drop trigger TRIGNAME ",
     "rs": "n" }
  ],
  "initRun": [
    {"name":"step", 
     "stmt":"alter trigger TRIGNAME enable;",
     "rs": "n" },
    {"name":"step 1",
     "sql":"bareSQL + datakey ", 
     "rs": "y" }
  ],
  "incrementalRun": [
    {"name":"step 1",
     "sql":"select sysdate from dual ", 
     "rs": "y" },
    {"name":"step 1",
     "sql":"select distinct datakey from LOGTBLNAME where ts_dcc < CURRTS", 
     "rs": "y" },    
     {"name":"dataSQL",
     "sql":"bareSQL + datakey + where", 
     "rs": "y" },
    {"name":"step 2", 
     "stmt":"delete from LOGTBLNAME where ts_dcc < CURRTS ",
     "rs": "n" }
  ]}'
  }'
 ),
('ES1DT', 
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
('DCC', 0, 'set meta_table.SEQ_LAST_REF to a starting seq.'),
('DCC', 2, 'sync(extract) DCC to kafka.'),
('DATA_', 0, 'simply set meta_table.curr_state=2.'),
('DATA_', 1, 'intial copy src to tgt.'),
('DATA_', 2, 'sync src to tgt via kafka'),
('DATA_', 9, 'audit'),
--('D2V', 1, 'ex. temp: intial copy src to tgt.'),
--('D2V', 2, 'ex. temp: sync src to tgt via trig.'),
--('D2V', 9, 'audit.'),
--('D2K_', 2, 'sync src data to kafka topic via kafka.'),
('DATA', 0, 'enable trig'),
('DATA', 1, 'initial copy src to tgt'),
('DATA', 2, 'sync src to tgt via trig'),
('DATA', 9, 'audit'),
--('O2K', 0, 'enable trig'),
--('O2K', 2, 'sync src to tgt(kafka) via trig'),
--('O2K', 21, 'testing code'),
--('O2K_', 2, 'sync src data to kafka topic via kafka.'),
--('K2E', 2, 'Kafak topic to ES doc')
;

insert into XFORM_SIMPLE  (X_ID,  
SRC_AVRO, 
TGT_AVRO, XFORM0
) values (11,
'??',
'??'
);

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

