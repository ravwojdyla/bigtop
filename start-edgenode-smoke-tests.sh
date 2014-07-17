#/bina/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_HOME=/usr/lib/hadoop
export HTTPFS_PROXY=http://flume-edge-collector-001.lon.spotify.net:14000
export LUIGI_CONF_DIR=/etc/luigi/
export HIVE_HOME=/usr/lib/hive
export HADOOP_MAPRED_HOME=/usr/lib/hadoop-mapreduce/
export HADOOP_SECURITY_AUTHORIZATION=false
export HBASE_HOME=none
export ZOOKEEPER_HOME=none
export SQOOP_URL=none
export MYSQL_HOST=none
export DB_USER=hive
export DB_PASSWORD=hive

CRUNCH_EXAMPLES_JAR="/tmp/apache-crunch-0.8.3-hadoop2-bin/crunch-examples-0.8.3-hadoop2-job.jar"

function rc_check {
    if [ $? -ne 0 ]; then
        echo "Failure - exit!"
        exit -1
    fi
}

function prereq_for_crunch {
    if [ -f $CRUNCH_EXAMPLES_JAR ]; then
       return
    fi
    if [ ! -f /tmp/apache-crunch-0.8.3-hadoop2-bin.tar.gz ]; then
        echo "Crunch tar in not available - downloading ..."
        curl http://apache.mirrors.spacedump.net/crunch/crunch-0.8.3/apache-crunch-0.8.3-hadoop2-bin.tar.gz > /tmp/apache-crunch-0.8.3-hadoop2-bin.tar.gz
        rc_check
   fi
   tar xvzf /tmp/apache-crunch-0.8.3-hadoop2-bin.tar.gz -C /tmp
   rc_check
}

for PROJECT in "$@"
do

    # -------------------------
    # build a project
    # -------------------------
    BUILD=$PROJECT
    if [ "$BUILD" == "hdfs" ] || [ "$BUILD" == "mapreduce" ] || [ "$BUILD" == "yarn" ]; then
        BUILD=hadoop
    fi
    mvn3 clean install -DskipTests -DskipITs -DperformRelease -o -nsu -f ./bigtop-tests/test-artifacts/$BUILD/pom.xml

    # -------------------------
    # execute a project
    # -------------------------
    EXECUTION_POM_FILE="bigtop-tests/test-execution/smokes/$BUILD/pom.xml"
    LOG4J_LEVEL="org.apache.bigtop.itest.log4j.level=TRACE"

    if [ "$PROJECT" == "hdfs" ]; then
	# ------------------------
        # hdfs
	# ------------------------
        mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestTextSnappy*' -f $EXECUTION_POM_FILE
        # mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestHDFSQuota*' -f $EXECUTION_POM_FILE
        # mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestNameNodeHA*' -f $EXECUTION_POM_FILE
        # mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestHDFSBalancer*' -f $EXECUTION_POM_FILE
        # mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestDFSAdmin*' -f $EXECUTION_POM_FILE
    # mapreduce
    elif [ "$PROJECT" == "mapreduce" ]; then
        mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestHadoopSmoke*' -f $EXECUTION_POM_FILE
        # mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestHadoopExamples*' -f $EXECUTION_POM_FILE
    # yarn
    elif [ "$PROJECT" == "yarn" ]; then
        mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestNode*' -f $EXECUTION_POM_FILE
    # sqoop
    elif [ "$PROJECT" == "sqoop" ]; then
        # mvn3 clean verify -D$LOG4J_LEVEL -D'db.tblname=TBLS' -D'db.dbname=meta_db' -D'db.host=hive.hdfsha.cloud.spotify.net' -D'org.apache.maven-failsafe-plugin.testInclude=**/TestSqoopImportPostgres*' -f $EXECUTION_POM_FILE
    # snakebite, luigi, httpfs
	echo "Sqoop smoke tests are currently disabled"
    elif [ "$PROJECT" == "crunch" ]; then
        prereq_for_crunch
        mvn3 clean verify -DargLine="-Dorg.apache.bigtop.itest.crunch.smoke.crunch.jar=${CRUNCH_EXAMPLES_JAR}" -D$LOG4J_LEVEL -f $EXECUTION_POM_FILE
    elif [ "$PROJECT" == "snakebite" ] || [ "$PROJECT" == "luigi" ] || [ "$PROJECT" == "httpfs" ] ; then
        mvn3 clean verify -D$LOG4J_LEVEL -f $EXECUTION_POM_FILE
    fi
done
