#!/bin/bash
# Third Argument to this script gives the environment
if [ $3 == dev ] 
	then
	source fair_development/fc10_tables_full_load/ct_fact_data_tables/arguments_dev.sh
elif [ $3 == qas ]
	then
	source fair_development/fc10_tables_full_load/ct_fact_data_tables/arguments_qas.sh
elif [ $3 == prd ]
	then
	source fair_development/fc10_tables_full_load/ct_fact_data_tables/arguments_prd.sh
else
	echo "Third argument should be dev or prd or qas"
	exit 1
fi
#READ LAST UPDATED DATE FROM FILE AND ASSIGN TO A VARIABLE "LAST_UPDATE"

last_update=`hdfs dfs -cat $location/$1_$updated/part-m-00000 | cut -d ',' -f1`
echo $last_update
echo $1" starting sqoop job to get new timestamp"

seqno=`hdfs dfs -cat $location/$1_$updated/part-m-00000 | cut -d ',' -f3`
echo $seqno " is unique row number"

if [ "$last_update" == "" ]
then echo "updated timestamp is null error exit the job"
exit 1
fi

#SELECTING COUNT OF UPDATED VALUES SINCE LAST IMPORT AND IF THIS IS GREATER THAN 10 GO TO FULL UPDATE

count=`sqoop eval --connect $fc10_jdbc_connector \
--username $user_name --password-file $fc10_password_file \
--query "SELECT count(*) FROM (SELECT DISTINCT COPY_DATE,UPDPER,ROW_NUMBER() OVER ( ORDER BY COPY_DATE,UPDPER) AS SeqNo FROM CS_FACT_DATA_COPY_LOG WHERE SCOPE = $scope) where  SeqNo > $seqno AND copy_date >= '$last_update'" \
--driver com.sap.db.jdbc.Driver| cut -d'|' -s -f2 | grep -o '[0-9]\+'`

if [ $count -gt 10 ]
then echo "more than 10 periods of the table are upddated, going with full table update"

./FAIR/fact_full/fact_full_import.sh $1 $scope

echo "SUCCESS"
exit 0

fi


#IMPOTING NEW UPDATED TIMESTAMP AND PERIOD ID FROM SAP HANA TABLE

sqoop import --connect $fc10_jdbc_connector \
--username $user_name --password-file $fc10_password_file \
--query "SELECT * FROM (SELECT DISTINCT COPY_DATE,UPDPER,ROW_NUMBER() OVER ( ORDER BY COPY_DATE,UPDPER) AS SeqNo FROM $source_db.CS_FACT_DATA_COPY_LOG WHERE SCOPE = $scope) WHERE \$CONDITIONS AND SeqNo > $seqno AND copy_date >= '$last_update' ORDER BY seqno ASC limit 1" \
--driver com.sap.db.jdbc.Driver --delete-target-dir --target-dir $location/$1_$updated -m 1

if [ $? -ne 0 ]
then echo "sqoop eval failed, exiting"
exit 1
fi

#CHECKING IF WE HAVE ANY UPDATE FROM LAST UPDATED TIMESTAMP

cnt=`hdfs dfs -cat $location/$1_$updated/part-m-00000 | wc -w`

if [ $cnt -eq 0 ];
then
echo "source table ct_fact_data_$1 is not updated since last import, updating the timestamp to file"
sqoop import --connect $fc10_jdbc_connector \
--username $user_name --password-file $fc10_password_file \
--query "SELECT * FROM (SELECT DISTINCT COPY_DATE,UPDPER,ROW_NUMBER() OVER ( ORDER BY COPY_DATE,UPDPER) AS SeqNo FROM CS_FACT_DATA_COPY_LOG WHERE SCOPE = $scope) WHERE \$CONDITIONS ORDER BY seqno DESC limit 1" \
--driver com.sap.db.jdbc.Driver --delete-target-dir --target-dir $location/$1_$updated -m 1

if [ $? -ne 0 ]
then echo "sqoop importing updated timestamp failed, exiting"
exit 1
fi

echo $1" source table is not updated since last import, exiting job"
echo "SUCCESS"
exit 0
else
echo $1" source table: ct_fact_data_$1  is updated since last import and new import is started"

#ASSIGN UPDPER TO A VARIABLE "UPDPER"

updper=`hdfs dfs -cat $location/$1_$updated/part-m-00000 | cut -d ',' -f2`
echo $updper

#IMPORTING ONLY UPDATED RECORDS USING UPDPER

sqoop import --connect $fc10_jdbc_connector \
--username $user_name --password-file $fc10_password_file \
--fetch-size=100000 --direct \
--query "SELECT * FROM (SELECT mod(entity+accnt+nature+flow,5) as SPLITTER,* FROM ct_fact_data_$1 WHERE VARIANT = $variant AND period = $updper) WHERE \$CONDITIONS" \
--boundary-query "select 0,4 from dummy"  \
--as-parquetfile --split-by SPLITTER --driver com.sap.db.jdbc.Driver --delete-target-dir \
--target-dir $location/ct_fact_data_$1_$updper -m 5

if [ $? -ne 0 ]
then echo "sqoop failed, exiting"
exit 1
fi

#UPDATING THE FINAL TABLE USING HIVE

beeline -u "$hive_host" -d org.apache.hive.jdbc.HiveDriver -hiveconf location="$location" -hiveconf db="$target_internal_db" -hiveconf table="$table"  -hiveconf period="$updper" -hiveconf tab="$1" -f  fair_development/fc10_tables_partition_load/partition.hql

if [ $? -ne 0 ]
then echo "rebuliding new partitions failed, exiting"
exit 1
fi

#INVALIDATING METADATA IN IMPALA

impala-shell -k --ssl -i $impala_host -q "invalidate metadata "$target_internal_db"."$table"_"$1"_raw"
impala-shell -k --ssl -i $impala_host -q "refresh "$target_internal_db"."$table"_"$1"_raw"
#impala-shell -k --ssl -i $impala_host -q "COMPUTE INCREMENTAL STATS "$target_internal_db"."$table"_"$1"_raw partition (period =$updper)"

if [ $? -ne 0 ]
then echo "invalidate incoming metadata failed, exiting"
exit 1
fi

# COUNT OF RECORDS FROM SOURCE

val_source=$(sqoop eval --connect $fc10_jdbc_connector \
--username $user_name --password-file $fc10_password_file \
--query "select count(*) from fccfr.ct_fact_data_$1 WHERE period = $updper AND VARIANT = $variant" \
--driver com.sap.db.jdbc.Driver| cut -d'|' -s -f2 | grep -o '[0-9]\+')

# COUNT OF RECORDS FROM TARGET

val_target=`impala-shell -k --ssl -i $impala_host -q  "SELECT count(*) FROM PRD_PRODUCT_FAIR.CT_FACT_DATA_$1_raw WHERE period =$updper" | cut -d'-' -f2 |cut -d '|' -f2 | cut -d')' -f2`

# EQUATING COUNT OF RECORDS FROM SOURCE AND COUNT OF RECORDS FROM TARGET

if [ $val_source -ne $val_target ]
then echo "table is not imported succesfully imported"
exit 1
fi

echo "AUTOMATED TESTING IS DONE,COUNT OF TABLE FROM SOURCE AND DESTINATION MATCHES PERIOD $updper IS SUCCESFULLY IMPORTED"

# DELETING THE RAW PERIOD DATA AFTER UPDATE IN OUR CT_FACT_DATA TABLE

hdfs dfs -rm -r $location/ct_fact_data_$1_$updper

# EXTRACTING YEAR USING UPDPER AND ASSIGNING VALUE TO VARIABLE "year"
 
year=$(sqoop eval --connect $fc10_jdbc_connector \
--username $user_name --password-file $fc10_password_file \
--query "select LEFT(period_alpha,4) as year from fccfr.id_period where period_id = $updper" \
--driver com.sap.db.jdbc.Driver | cut -d'|' -s -f2 | grep -o '[0-9]\+')

echo "$year"
# UPDATING QTD VALUES FOR THE WHOLE YEAR

impala-shell -k --ssl -i $impala_host -f fair_development/fc10_tables_full_load/ct_fact_data_tables/qtdcalc.sql --var=year="$year" --var=db="$target_internal_db" --var=tab="$1" --var=scope="$2"

if [ $? -ne 0 ]
	then 
	echo "updating QTD values in $1 table failed"
	exit 1
fi

beeline -u "$hive_host" -d org.apache.hive.jdbc.HiveDriver -e "ANALYZE TABLE "$target_internal_db"."$table"_"$1" PARTITION(YEAR = "$year") COMPUTE STATISTICS"
impala-shell -k --ssl -i $impala_host -q "compute incremental stats "$target_internal_db"."$table"_"$1""
echo "SUCCESS"
fi
