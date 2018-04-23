#!/bin/bash
# Script to start the job manager
# args: <master> <deployMode> <akkaAdress> <actorName> <workDir> [<proxyUser>]
set -e

get_abs_script_path() {
   pushd . >/dev/null
   cd $(dirname $0)
   appdir=$(pwd)
   popd  >/dev/null
}
get_abs_script_path

. $appdir/setenv.sh

GC_OPTS="-XX:+UseConcMarkSweepGC
         -verbose:gc -XX:+PrintGCTimeStamps
         -XX:MaxPermSize=512m
         -XX:+CMSClassUnloadingEnabled "

JAVA_OPTS="-XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true"

MAIN="spark.jobserver.JobManager"

# copy files via spark-submit and read them from current (container) dir
if [ $2 = "cluster" -a -z "$REMOTE_JOBSERVER_DIR" ]; then
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS
    --master $1 --deploy-mode cluster
    --conf spark.yarn.submit.waitAppCompletion=false
    --files $appdir/log4j-cluster.properties,$conffile"
  JAR_FILE="$appdir/spark-job-server.jar"
  CONF_FILE=$(basename $conffile)
  LOGGING_OPTS="-Dlog4j.configuration=log4j-cluster.properties"

# mesos cluster mode
elif [ $2 == "cluster" -a "$MESOS_CLUSTER_DISPATCHER" ]; then
  if [ $9 != "DEFAULT_MESOS_DISPATCHER" ]; then
    MESOS_CLUSTER_DISPATCHER=$9
  fi
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS
    --master $MESOS_CLUSTER_DISPATCHER --deploy-mode cluster
    --conf spark.yarn.submit.waitAppCompletion=false"
  JAR_FILE="$REMOTE_JOBSERVER_DIR/spark-job-server.jar"
  CONF_FILE="$REMOTE_JOBSERVER_DIR/$(basename $conffile)"
  LOGGING_OPTS="-Dlog4j.configuration=$REMOTE_JOBSERVER_DIR/log4j-cluster.properties"

# use files in REMOTE_JOBSERVER_DIR
elif [ $2 == "cluster" ]; then
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS
    --master $1 --deploy-mode cluster
    --conf spark.yarn.submit.waitAppCompletion=false"
  JAR_FILE="$REMOTE_JOBSERVER_DIR/spark-job-server.jar"
  CONF_FILE="$REMOTE_JOBSERVER_DIR/$(basename $conffile)"
  LOGGING_OPTS="-Dlog4j.configuration=$REMOTE_JOBSERVER_DIR/log4j-cluster.properties"

# client mode, use files from app dir
else
  JAR_FILE="$appdir/spark-job-server.jar"
  CONF_FILE="$conffile"
  LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-server.properties -DLOG_DIR=$5"
  GC_OPTS="$GC_OPTS -Xloggc:$5/gc.out"
fi

# set driver cores and memory option
SPARK_DRIVER_OPTIONS="--driver-cores $7"
if [ "$8" != "0" ]; then
  SPARK_DRIVER_OPTIONS="$SPARK_DRIVER_OPTIONS --driver-memory $8"
else
  SPARK_DRIVER_OPTIONS="$SPARK_DRIVER_OPTIONS --driver-memory $JOBSERVER_MEMORY"
fi

if [ "${10}" != "NULL" ]; then
  SPARK_DRIVER_OPTIONS="$SPARK_DRIVER_OPTIONS --conf spark.kubernetes.driver.pod.name=${10}"
fi

if [ -n "${11}" ]; then
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --proxy-user ${11}"
fi

if [ -n "$JOBSERVER_KEYTAB" ]; then
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --keytab $JOBSERVER_KEYTAB"
fi


cmd='$SPARK_HOME/bin/spark-submit --class $MAIN $SPARK_DRIVER_OPTIONS
      --conf "spark.executor.extraJavaOptions=$LOGGING_OPTS"
      $SPARK_SUBMIT_OPTIONS
      --driver-java-options "$GC_OPTS $JAVA_OPTS $LOGGING_OPTS $CONFIG_OVERRIDES $SPARK_SUBMIT_JAVA_OPTIONS"
      $JAR_FILE $3 $4 $6'

eval $cmd 2>&1 > $5/spark-job-server.out

