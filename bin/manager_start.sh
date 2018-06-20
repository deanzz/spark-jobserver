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

#GC_OPTS="-XX:+UseConcMarkSweepGC
#         -verbose:gc -XX:+PrintGCTimeStamps
#         -XX:MaxPermSize=512m
#         -XX:+CMSClassUnloadingEnabled "

GC_OPTS="-XX:+UseG1GC
         -verbose:gc -XX:+PrintGCTimeStamps
         -XX:MaxPermSize=512m"


ALLUXIO_OPTS="-Dalluxio.user.file.writetype.default=THROUGH -Dalluxio.user.file.readtype.default=NO_CACHE -Dalluxio.user.file.delete.unchecked=true "

JAVA_OPTS="-XX:MaxDirectMemorySize=$MAX_DIRECT_MEMORY
           -XX:+HeapDumpOnOutOfMemoryError -Djava.net.preferIPv4Stack=true
           -DconfigGroupName=kmtest
           -DconfigServiceName=sparkJobs
           -DconfigServerHost=config-server
           -DconfigServerPort=2502
           $ALLUXIO_OPTS
          "
LOG_OPTIONS="-Dlog4j.configuration=file:/opt/spark/conf/log4j.properties"

MAIN="spark.jobserver.JobManager"
MASTER=$1
DEPLOY_MODE=$2
CLUSTER_ENTRY_NODE=$3
CONTEXT_ACTOR_NAME=$4
CONTEXT_DIR=$5
JOBSERVER_PORT=$6
JOBSERVER_HOST=$7
DRIVER_CORES=$8
DRIVER_MEMORY=$9
MESOS_DISPATCHER=${10}
K8S_POD_NAME=${11}
K8S_NODE_SELECTOR=${12}
SPARK_PROXY_USER_PARAM=${13}

# copy files via spark-submit and read them from current (container) dir
if [ $DEPLOY_MODE = "cluster" -a -z "$REMOTE_JOBSERVER_DIR" ]; then
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS
    --master $MASTER --deploy-mode cluster
    --conf spark.yarn.submit.waitAppCompletion=false
    --conf spark.kubernetes.container.image=$SPARK_DOCKER_IMAGE
    --conf spark.kubernetes.driver.container.image=$SPARK_DOCKER_IMAGE
    --conf spark.kubernetes.driver.label.spark-driver=ture
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
    --conf spark.kryoserializer.buffer.max=512m
    --conf spark.kryoserializer.buffer=512k
    --conf spark.kryo.classesToRegister=scala.collection.mutable.ArrayBuffer,scala.collection.mutable.ListBuffer
    --conf spark.sql.caseSensitive=true
    --conf spark.sql.autoBroadcastJoinThreshold=-1
    --conf spark.executor.heartbeatInterval=60s
    --conf spark.driver.maxResultSize=3g
    --conf spark.ui.port=4040
  "

  JAR_FILE="$appdir/spark-job-server.jar"
  # CONF_FILE=$(basename $conffile)
  # LOGGING_OPTS="-Dlog4j.configuration=log4j-cluster.properties"

# mesos cluster mode
elif [ $DEPLOY_MODE == "cluster" -a "$MESOS_CLUSTER_DISPATCHER" ]; then
  if [ $MESOS_DISPATCHER != "DEFAULT_MESOS_DISPATCHER" ]; then
    MESOS_CLUSTER_DISPATCHER=$MESOS_DISPATCHER
  fi
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS
    --master $MESOS_CLUSTER_DISPATCHER --deploy-mode cluster
    --conf spark.yarn.submit.waitAppCompletion=false
  "

  JAR_FILE="$REMOTE_JOBSERVER_DIR/spark-job-server.jar"
  # CONF_FILE="$REMOTE_JOBSERVER_DIR/$(basename $conffile)"
  # LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-cluster.properties"

# use files in REMOTE_JOBSERVER_DIR
elif [ $DEPLOY_MODE == "cluster" ]; then
  SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS
    --master $MASTER --deploy-mode cluster
    --conf spark.yarn.submit.waitAppCompletion=false
    --conf spark.kubernetes.container.image=$SPARK_DOCKER_IMAGE
    --conf spark.kubernetes.driver.container.image=$SPARK_DOCKER_IMAGE
    --conf spark.kubernetes.driver.label.spark-driver=ture
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
    --conf spark.kryoserializer.buffer.max=512m
    --conf spark.kryoserializer.buffer=512k
    --conf spark.kryo.classesToRegister=scala.collection.mutable.ArrayBuffer,scala.collection.mutable.ListBuffer
    --conf spark.sql.caseSensitive=true
    --conf spark.sql.autoBroadcastJoinThreshold=-1
    --conf spark.executor.heartbeatInterval=60s
    --conf spark.driver.maxResultSize=3g
    --conf spark.ui.port=4040
  "

  JAR_FILE="$REMOTE_JOBSERVER_DIR/spark-job-server.jar"
  # CONF_FILE="$REMOTE_JOBSERVER_DIR/$(basename $conffile)"
  # LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-cluster.properties"

# client mode, use files from app dir
else
  JAR_FILE="$appdir/spark-job-server.jar"
  # CONF_FILE="$conffile"
  # LOGGING_OPTS="-Dlog4j.configuration=file:$appdir/log4j-server.properties -DLOG_DIR=$CONTEXT_DIR"
  GC_OPTS="$GC_OPTS -Xloggc:$CONTEXT_DIR/gc.out"
fi

if [ "$K8S_POD_NAME" != "NULL" ]; then
   SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --conf spark.kubernetes.driver.pod.name=$K8S_POD_NAME"
fi

# set driver cores and memory option
if [ "$DRIVER_CORES" != "NULL" ]; then
   # request cores
   SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --conf spark.driver.cores=$DRIVER_CORES"
   # limit cores
   SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --conf spark.kubernetes.driver.limit.cores=$DRIVER_CORES"
fi

if [ "$DRIVER_MEMORY" != "NULL" ]; then
   SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --conf spark.driver.memory=$DRIVER_MEMORY"
fi

if [ "$K8S_NODE_SELECTOR" != "NULL" ]; then
   SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --conf spark.kubernetes.node.selector.nodename=$K8S_NODE_SELECTOR"
fi

if [ -n "$SPARK_PROXY_USER_PARAM" ]; then
   SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --proxy-user $SPARK_PROXY_USER_PARAM"
fi

if [ -n "$JOBSERVER_KEYTAB" ]; then
   SPARK_SUBMIT_OPTIONS="$SPARK_SUBMIT_OPTIONS --keytab $JOBSERVER_KEYTAB"
fi

cmd='$SPARK_HOME/bin/spark-submit --class $MAIN $SPARK_DRIVER_OPTIONS
      --conf "spark.executor.extraJavaOptions=$ALLUXIO_OPTS"
      $SPARK_SUBMIT_OPTIONS
      --driver-java-options "$GC_OPTS $JAVA_OPTS $CONFIG_OVERRIDES $SPARK_SUBMIT_JAVA_OPTIONS $LOG_OPTIONS"
      $JAR_FILE $CLUSTER_ENTRY_NODE $CONTEXT_ACTOR_NAME $JOBSERVER_PORT $JOBSERVER_HOST'

echo "SPARK_DOCKER_IMAGE:" $SPARK_DOCKER_IMAGE
echo "SPARK_HOME:" $SPARK_HOME
echo "MAIN:" $MAIN
echo "SPARK_DRIVER_OPTIONS:" $SPARK_DRIVER_OPTIONS
echo "LOG_OPTIONS:" $LOG_OPTIONS
echo "ALLUXIO_OPTS:" $ALLUXIO_OPTS
echo "SPARK_SUBMIT_OPTIONS:" $SPARK_SUBMIT_OPTIONS
echo "GC_OPTS:" $GC_OPTS
echo "JAVA_OPTS:" $JAVA_OPTS
echo "CONFIG_OVERRIDES:" $CONFIG_OVERRIDES
echo "SPARK_SUBMIT_JAVA_OPTIONS:" $SPARK_SUBMIT_JAVA_OPTIONS
echo "JAR_FILE:" $JAR_FILE
echo "CLUSTER_ENTRY_NODE:" $CLUSTER_ENTRY_NODE
echo "CONTEXT_ACTOR_NAME:" $CONTEXT_ACTOR_NAME
echo "CONTEXT_DIR:" $CONTEXT_DIR
echo "JOBSERVER_PORT:" $JOBSERVER_PORT

echo "cmd:" $cmd

eval $cmd 2>&1 > $CONTEXT_DIR/spark-job-server.out

#RESULT=$(curl http://jobserver.kmtongji.com/contexts/$K8S_POD_NAME)
#echo "curl http://jobserver.kmtongji.com/contexts/$K8S_POD_NAME: $RESULT"  >> $CONTEXT_DIR/spark-job-server.out

#---config under spark-defaults.conf----
#spark.mesos.coarse      true
#
#spark.driver.extraJavaOptions      -Dlog4j.configuration=file:./conf/log4j-server.properties -Dalluxio.user.file.writetype.default=THROUGH -Dalluxio.user.file.readtype.default=NO_CACHE
#spark.executor.extraJavaOptions    -Dlog4j.configuration=file:./conf/log4j-server.properties -Dalluxio.user.file.writetype.default=THROUGH -Dalluxio.user.file.readtype.default=NO_CACHE
#
##spark.driver.extraClassPath     /home/ec2-user/nlp/stanford-chinese-corenlp-2017-06-09-models.jar:/home/ec2-user/nlp/stanford-corenlp-3.8.0.jar:/home/ec2-user/nlp/stanford-english-corenlp-2017-06-09-models.jar
##spark.executor.extraClassPath   /home/ec2-user/nlp/stanford-chinese-corenlp-2017-06-09-models.jar:/home/ec2-user/nlp/stanford-corenlp-3.8.0.jar:/home/ec2-user/nlp/stanford-english-corenlp-2017-06-09-models.jar
#
#spark.serializer                   org.apache.spark.serializer.KryoSerializer
#
#spark.kryoserializer.buffer.max    512m
#spark.kryoserializer.buffer        512k
#spark.kryo.classesToRegister       scala.collection.mutable.ArrayBuffer,scala.collection.mutable.ListBuffer
#
#spark.executor.extraJavaOptions   -Xss4m
#
#spark.sql.caseSensitive           true
#
#spark.sql.autoBroadcastJoinThreshold -1
#spark.executor.heartbeatInterval   60s
#
#spark.driver.maxResultSize        3g