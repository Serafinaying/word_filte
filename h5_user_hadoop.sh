#!/bin/sh
HADOOP_BIN="/usr/local/webserver/hadoop/bin/hadoop"
HADOOP_JAR="/usr/local/webserver/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.0.jar"

WORK_SPACE="/home/rank/wangdanying/h5_user"
day2=`date "+%Y-%m-%d" -d "-5 days"`
day1=`date "+%Y-%m-%d" -d "-65 days"`

beg_date=`date -d "${day1}" +%s`
end_date=`date -d "${day2}" +%s`

for (( i=${end_date};i>=${beg_date};i=i-86400))
do
	day=`date -d @${i} +%Y-%m-%d`
	path="/user/hive/warehouse/di.db/piwikmeta/day=${day}/*"
	$HADOOP_BIN fs -test -e /user/hive/warehouse/di.db/piwikmeta/day=${day}
	if (($? == 1))
	then
		((beg_date=beg_date-86400))
		continue
	fi
	behavior[i]=${path}
done

USER_CLEAN="/user/rank/wangdanying/user_h5/user_clean/${day2}"
USER_STAT="/user/rank/wangdanying/user_h5/user_behave_stat/${day2}"

###########################################################
$HADOOP_BIN fs -rm -r ${USER_CLEAN}
$HADOOP_BIN jar $HADOOP_JAR \
		-D mapred.map.tasks=97 \
		-D mapred.reduce.tasks=337 \
		-D mapred.job.name="wangdanying_task" \
		-D stream.num.map.output.key.fields=1 \
		-D mapred.text.key.partitioner.options="-k1,1" \
		-D mapred.text.key.comparator.options="-k1,1" \
		-input ${behavior[*]} \
		-output ${USER_CLEAN}\
		-mapper "user_clean.sh" \
		-reducer "cat" \
		-file ${WORK_SPACE}/user_clean.sh 

###########################################################
$HADOOP_BIN	fs -rm -r ${USER_STAT}
$HADOOP_BIN jar $HADOOP_JAR\
		-D mapred.map.tasks=97 \
		-D mapred.reduce.tasks=100 \
		-D mapred.job.name="wangdanying_task" \
		-D stream.num.map.output.key.fields=1 \
		-D mapred.text.key.partitioner.options="-k1,1" \
		-D mapred.text.key.comparator.options="-k1,1" \
		-input ${USER_CLEAN}\
		-output ${USER_STAT}\
		-mapper "deal_map.sh"\
		-reducer "deal_reducer.sh"\
		-file ${WORK_SPACE}/deal_map.sh\
		-file ${WORK_SPACE}/deal_reducer.sh

rm -rf user_stat
$HADOOP_BIN fs -cat ${USER_STAT}/* > user_stat
