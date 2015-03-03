#!/bin/bash
bin=`dirname $0`
bin=`cd $bin;pwd`

if [ $# != 1 ];then
  echo "USAGE : $0 [configfile] "
  exit 1
fi

JAVA_HOME=$JAVA_HOME
CLASSPATH=./*:./libs/*:.
JVMPARAMS="-Xloggc:./logs/gc.log -Xmx2048m -Dlogback.configurationFile=./conf/logback.xml "
nohup $JAVA_HOME/bin/java $JVMPARAMS -classpath $CLASSPATH com.giant.etllog.EtlLog $* & >/dev/null 2>&1 
#$JAVA_HOME/bin/java $JVMPARAMS -classpath $CLASSPATH com.giant.etllog.EtlLog $*
