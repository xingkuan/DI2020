
#
# This is the main script for registering table for replication.
#
export VSYCNHOME=/home/dbadmin/vSync400/Java
export CLASSPATH=$CLASSPATH:$VSYCNHOME/libs/log4j-api-2.7.jar:$VSYCNHOME/libs/log4j-core-2.7.jar:$VSYCNHOME/libs/jt400.jar:$VSYCNHOME/libs/ojdbc6.jar:$VSYCNHOME/libs/vertica-jdbc-8.1.1-0.jar
cd $VSYCNHOME

SRCDBID=$1
SRCSCH=$2
SRCTBL=$3
SRCJRNL=$4
TGTDBID=$5
TGTSCH=$6
TGTTBL=$7
POOLID=$8
SYNCTYPE=$9
OUTDIR=${10}
TBLID=${11}

VHOST=vertx1
VUSER=dbadmin
VPASS='Bre@ker321'
repDB='RMAN01';
repUser='vertsnap';
repPwd='BAtm0B1L#';

echo "source table:   $SRCSCH.$SRCTBL"
echo "source journal: $SRCJRNL"
echo "target table:   $TGTSCH.$TGTTBL"
echo "pool ID:        $POOLID"

#java -Djava.security.egd=file:/dev/../dev/urandom -cp ./bin:$CLASSPATH com.guess.vsync400.RegisterTbl400 7 JDAADM INVORD JDAADM.INVORD 4 test INVORD 11 1 c:\Users\johnlee\
java -Djava.security.egd=file:/dev/../dev/urandom -cp ./bin:$CLASSPATH com.guess.vsync400.RegisterTbl400 $SRCDBID $SRCSCH $SRCTBL $SRCJRNL $TGTDBID $TGTSCH $TGTTBL $POOLID $SYNCTYPE $OUTDIR $TBLID

sqlplus $repUser/$repPwd@$repDB @hadRegistered.sql
read -p "regsiter. Press enter to continue ..."

sqlplus $repUser/$repPwd@$repDB @repoTblDDL.sql
sqlplus $repUser/$repPwd@$repDB @repoColsDDL.sql
sqlplus $repUser/$repPwd@$repDB @repoJ400row.sql

read -p "create tgt tbl in vertica. Press enter to continue ..."
vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS -f $OUTDIR/verticaDDL.sql



kafkaTopic.sh

rm repoTblDML.sql
rm repoColsDML.sql
rm verticaDDL.sql
rm repoJ400row.sql
rm kafkaTopic.sh

