
#
# This is the main script for registering table for replication.
#

JPOOL=2  # Job pool ID. Basically, tables are organized into different pools to be refreshed together.
RTYPE=1  # For now, all are type 1. 1 for incremental refresh; 2 for total refresh.
TBL=$1
TBLID=$2
SRCSCH=CRMDEV  #CRMDEV, SEWN
TGTSCH=VCRM    #VCRM  , VSEWN
VHOST=vertx1
VUSER=dbadmin
VPASS="Bre@ker321"

echo $TBL  $TBLID
read -p "pre check. Press enter to continue ..."
sqlplus /nolog @tbl00_preCheck $SRCSCH $TBL $TBL $TGTSCH

read -p "regsiter. Press enter to continue ..."
sqlplus /nolog @tbl01_setup_Wid $SRCSCH $TBL $TBL $TGTSCH $JPOOL $RTYPE $TBLID

read -p "create tgt tbl in vertica. Press enter to continue ..."
#fixing SQL errors
ed wrkVertica.sql <<STMT
,s/NUMBER(,)/NUMBER/g
wq
STMT
vsql -h$VHOST -U$VUSER -w -U dbadmin -w$VPASS -f wrkVertica.sql
