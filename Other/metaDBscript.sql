systemctl status postgresql-9.6
postgres=# show data_directory;
     data_directory
-------------------------
 /var/lib/pgsql/9.6/data
cd /var/lib/pgsql/9.6/data
vi pg_hba.conf
psql -h dbatool01 -d didb -U repuser -W


create database didb;

create user repuser password 'passwd';
grant connect on database didb to repuser;
grant all privileges on database didb to repuser;

\c didb

CREATE TABLE DATA_POINT
(
  DB_ID      VARCHAR(15)   PRIMARY KEY,
  DB_ROLE    VARCHAR(5),  --reference only. R for Read from or W for Write to
  DB_CAT     VARCHAR(15),
  DB_VENDOR  VARCHAR(15),
  DB_DRIVER  VARCHAR(150),
  DB_CONN    VARCHAR(150),
  DB_USR     VARCHAR(25),
  DB_PWD     VARCHAR(25),
  DB_DESC    VARCHAR(225),
  INSTRUCT   TEXT    --the templates of registration, initialization, remove, incremental...
)
;
grant select,update,delete,insert on data_point to repuser;

-- Let's explicitly know that the DB are RDBMS or KAFKA, for both SRC, TGT and DCC
CREATE TABLE TASK
(
  TASK_ID         INTEGER PRIMARY KEY,
  POOL_ID         INTEGER,
  CURR_STATE      INTEGER,	-- -1: not runable(e.g populated by DB trigger); 
  							--  0: not initialized; 
  							--  1: disabled;
  							--	2: runable;
  							--	3: being run 
  SRC_DB_ID       VARCHAR(15),
  SRC_TBL         VARCHAR(50),
  SRC_STMT        TEXT,		--if customer SQL is used.
  FLD_CNT      int,
  TGT_DB_ID       VARCHAR(15),
  TGT_TBL         TEXT,		-- can be a list of, eg. DB2 journal table members
  TGT_STMT        TEXT,		--if JDBC.
  AVRO_SCHEMA     jsonb,
  REG_DT            DATE,
  INIT_DT	        DATE,
  INIT_DURATION     INTEGER,	-- seconds
  TS_LAST_REF       TIMESTAMP(6),
  SEQ_LAST_REF      BIGINT
--  CONSTRAINT unique_src UNIQUE (SRC_DB_ID, SRC_SCHEMA, SRC_TABLE)
)
; 
grant select,update,delete,insert on task to repuser;

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
  DB_ID,DB_ROLE,
  DB_CAT, DB_VENDOR,
  DB_DRIVER, DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values 
('DB2AK', 'R',
 'JDBC', 'DB2/AS400',
 'com.ibm.as400.access.AS400JDBCDriver', 'jdbc:as400://DEVELOPM:2551/DB2_RETSYS', 
 'johnlee2', 'C3line1998', 
 'DB2/AS400 data id',
 '{
  "cdcDB":"KAFKAK1",
  "cdcTbl":"<SRCTBL>",	
  "bareSQL":"select COUNT_OR_RRN as DATAKEY,SEQUENCE_NUMBER AS SEQNBR 
  FROM table (Display_Journal(''<DIJRNL>'', ''<DIJMBR>'', ''*CURCHAIN'', '''', '''',
  cast(''<DICDCTS>'' as TIMESTAMP), cast(''<DICDCSEQ>'' as decimal(21,0)),
   ''R'','''','''' ,'''',''*QDDS'','''','''','''','''') ) as x
   order by 2 asc",
  "bareSQL1":"select COUNT_OR_RRN as DATAKEY,SEQUENCE_NUMBER AS SEQNBR
  FROM table (Display_Journal(''<DIJRNL>'', ''<DIJMBR>'', '''', '''', '''',
  cast(''<DISTARTTS>'' as TIMESTAMP), cast(''<DISTARTSEQ>'' as decimal(21,0)),
  ''R'','''','''' ,'''',''*QDDS'','''','''','''','''') ) as x
  order by 2 asc",
  "regist":[],
  "unregist":[],
  "initRun":[
   {"note":"if the CDC marks are not set yet, set them.", 
    "cmd":"select max(COUNT_OR_RRN) as CDCDATAKEY,max(SEQUENCE_NUMBER AS SEQNBR) as CDCSEQNUM, max(ENTRY_TIMESTAMP) as CDCTS
     FROM table (Display_Journal(''<DIJRNL>'', ''<DIJMBR>'', ''*CURCHAIN'', '''', '''',
     cast(''<DISTARTTS>'' as TIMESTAMP),cast(''<DISTARTSEQ>'' as decimal(21,0)),  
     ''R'','''','''','''',''*QDDS'','''','''','''','''') ) as x 
     order by 2 asc", 
    "type":"MULTIV", 
    "return": ["KEY, SEQ","TS"] 
   }
  ] 
 }'
 );
insert into DATA_POINT (
  DB_ID,DB_ROLE,
  DB_CAT, DB_VENDOR,
  DB_DRIVER, DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values 
('DB2ADS', 'R',
 'JDBC', 'DB2/AS400',
 'com.ibm.as400.access.AS400JDBCDriver', 'jdbc:as400://DEVELOPM:2551/DB2_RETSYS', 
 'johnlee2', 'C3line1998', 
 'DB2/AS400 data',
 '{
   "cdcDB":"KAFKAK1",	
   "bareSQL": "select a.*, rrn(a) as datakey from <DISRCTBL> as a",
   "incRun": [],
   "incRun":[],
   "regist":[],
   "unregist":[]
  }'
 );
 
insert into DATA_POINT (
  DB_ID,DB_ROLE,
  DB_CAT, DB_VENDOR,
  DB_DRIVER, DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values(
'VERTD1', 'W', 
 'JDBC', 'VERTICA',
 'com.vertica.jdbc.Driver', 'jdbc:vertica://vert41:5433/vertc', 
 'dbadmin', 'D0gB0ne5y', 
 'Vert x',
 '{	
  "notes":"no specific instrunction for Vertica destination" 
  }'
 );
 
insert into DATA_POINT (
  DB_ID,DB_ROLE,
  DB_CAT, DB_VENDOR,
  DB_DRIVER, DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values(
 'ORAK1', 'R', 
 'JDBC', 'ORACLE',
 'oracle.jdbc.OracleDriver', 'jdbc:oracle:thin:@crmdbtest2:1521:CRMP64', 
 'VERTSNAP', 'v3rtsnap', 
 'Oracle Dev key source',
 '{	
  "note":"trigger based approach for CDC",
  "cdcObj":"CDC<DISRCTBL>",
  "cdcObjNameLen":15,
  "bareSQL": "select CDC_DATA_KEY from <CDCLOG>",
  "initRun":[
   {"note":"get the CDC TS, which will be saved in repo",
   	"cmd":"select sysdate from from dual",
    "type":"SINGLEV", 
   	"return": ["TS"]
   },
   {"name":"enable cdc trigger", 
    "cmd":"alter trigger <CDCLOG> enable",
    "type":"NOV", 
   }
  ]
  "incRun":[ ],
  "preRegist":[
   {"name":"verify log tbl name not used",
    "cmd":"select case when 
    exists(select 0 from user_objects where OBJECT_NAME=''<CDCTBL>'') then -1 else 0  end as rtcode 
    from dual",
    "type":"SINGLEV", 
   	"return": ["RC"]
   },
   {"name":"verify log trigger name is not used",
    "cmd":"select case when 
    exists(select 0 from user_objects where OBJECT_NAME=''<CDCTRG>'') then -1 else 0  end as rtcode 
    from dual",
    "type":"SINGLEV", 
   	"return": ["RC"]
   }
  ],
  "regist": [
   {"name":"create log tbl name",
    "cmd":"create table <CDCTBL> (CDCKEY varchar2(20), action char(1), ts_dcc date) ",
    "type":"NOV"
   },
   {"name":"create log trigger", 
    "cmd":"create or replace trigger <CDCTRG>   
		after insert or update or delete on <DISRCTBL>  
		for each row  
		begin
		DECLARE
    	action  char(1);
		BEGIN 
		IF DELETING THEN 
 			action := ''D'';
		END IF;
		IF INSERTING THEN 
 			action := ''I'';
		END IF;
		IF UPDATING THEN 
 			action := ''U'';
		END IF;
			
		insert into <CDCTBL> (datakey, action, ts_dcc )  
		values ( :new.rowid, sysdate );  
		end; ",
    "type":"NOV"
	},
    {"name":"trigger is disabled",
     "cmd":"alter trigger <CDCTRG> disable",
     "type":"NOV"
    }
  ],
  "unregist": [
    {"name":"drop cdc log tbl",
     "cmd":"drop table <CDCTBL>",
     "type":"NOV"
    },
    {"name":"drop cdc trigger", 
     "cmd":"drop trigger <CDCTRG>",
     "type":"NOV"
    }
  ], 
  "disable": [
    {"name":"disable cdc trigger", 
     "cmd":"alter trigger <CDCTRG> disable",
     "type":"NOV"
    }
   ] 
  }'
);

insert into DATA_POINT (
  DB_ID,DB_ROLE,
  DB_CAT, DB_VENDOR,
  DB_DRIVER, DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values(
 'ORAD1', 'R', 
 'JDBC', 'ORACLE',
 'oracle.jdbc.OracleDriver', 'jdbc:oracle:thin:@crmdbtest2:1521:CRMP64', 
 'VERTSNAP', 'v3rtsnap', 
 'Oracle Dev',
 '{
  "note":"only action here will be the selecting the data",
  "CDCDB":"ORAK1",
  "bareSQL": "select a.*, a.rowid as CDCKEY from <DISRCTBL> a ",
 }'
);

insert into DATA_POINT (
  DB_ID,DB_ROLE,
  DB_CAT, DB_VENDOR,
  DB_DRIVER, DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values( 
 'KAFKAK1', 'WR',
 'MQK', 'KAFKA',
 '', 'usir1xrvkfk01:9092,usir1xrvkfk02:9092,usir1xrvkfk03:9092', 
 'xxx', 'xxx', 
 'kafka data consumer',
 '{
  "note":"all details are actually handled in Java class."
  "regist": [
    {"name":"create topic",
     "cmd":"creade topic",
     "type":"NOV"
    }
  ],
  "unregist": [
    {"name":"delete topic",
     "cmd":"delete topic",
     "type":"NOV"
    }
  ]
 }'
);

insert into DATA_POINT (
  DB_ID,DB_ROLE,
  DB_CAT, DB_VENDOR,
  DB_DRIVER, DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values (
 'KAFKAD1', 'WR',
 'MQD', 'KAFKA',
 '', 'usir1xrvkfk01:9092,usir1xrvkfk02:9092,usir1xrvkfk03:9092', 
 'xxx', 'xxx', 
 'kafka data sink',
 '{	
  "regist": [
    {"name":"create topic",
     "cmd":"creade topic",
     "type":"NOV"
    }
  ],
  "unregist": [
    {"name":"delete topic",
     "cmd":"delete topic",
     "type":"NOV"
    }
  ]
 }'
);
 
insert into DATA_POINT (
  DB_ID,DB_ROLE,
  DB_CAT, DB_VENDOR,
  DB_DRIVER, DB_CONN,
  DB_USR, DB_PWD,
  DB_DESC,
  INSTRUCT)
values 
('ES1DT', 'DT',
 'ES', 'ES',
 '','http://dbatool02:9200', 
 'xxx', 'xxx', 
 'ElasticSearch',
'{	
  "regist": [
    {"name":"create index",
     "cmd":"creade index",
     "type":"NOV"
    }
  ],
  "unregist": [
    {"name":"drop index",
     "cmd":"drop index",
     "type":"NOV"
    }
  ]
 }'
)
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

