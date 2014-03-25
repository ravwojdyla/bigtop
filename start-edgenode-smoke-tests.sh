#/bina/bash

export HADOOP_CONF_DIR=/etc/hadoop/conf
export HADOOP_HOME=/usr/lib/hadoop
export HTTPFS_PROXY=flume-edge-collector-001.lon.spotify.net:14443
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

for PROJECT in "$@"
do

    # -------------------------
    # build a project
    # -------------------------
    mvn3 clean install -DskipTests -DskipITs -DperformRelease -o -nsu -f ./bigtop-tests/test-artifacts/$PROJECT/pom.xml

    # -------------------------
    # execute a project
    # -------------------------
    EXECUTION_POM_FILE="bigtop-tests/test-execution/smokes/$PROJECT/pom.xml"
    LOG4J_LEVEL="org.apache.bigtop.itest.log4j.level=TRACE"

    # hadoop = hdfs + mapreduce + yarn
    if [ "$PROJECT" == "hadoop" ]; then
        # hdfs
        mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestTextSnappy*' -f $EXECUTION_POM_FILE
        # mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestHDFSQuota*' -f $EXECUTION_POM_FILE
        # mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestNameNodeHA*' -f $EXECUTION_POM_FILE
        mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestHDFSBalancer*' -f $EXECUTION_POM_FILE
        # mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestDFSAdmin*' -f $EXECUTION_POM_FILE
        # mapreduce
        mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestHadoopSmoke*' -f $EXECUTION_POM_FILE
        # mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestHadoopExamples*' -f $EXECUTION_POM_FILE
        # yarn
        mvn3 clean verify -D$LOG4J_LEVEL -D'org.apache.maven-failsafe-plugin.testInclude=**/TestNode*' -f $EXECUTION_POM_FILE
    # sqoop
    elif [ "$PROJECT" == "sqoop" ]; then
        # mvn3 clean verify -D$LOG4J_LEVEL -D'db.tblname=TBLS' -D'db.dbname=meta_db' -D'db.host=hive.hdfsha.cloud.spotify.net' -D'org.apache.maven-failsafe-plugin.testInclude=**/TestSqoopImportPostgres*' -f $EXECUTION_POM_FILE
    # snakebite, luigi, httpfs
	echo "Sqoop smoke tests are currently disabled"
    elif [ "$PROJECT" == "snakebite" ] || [ "$PROJECT" == "luigi" ] || [ "$PROJECT" == "httpfs" ] ; then
        mvn3 clean verify -D$LOG4J_LEVEL -f $EXECUTION_POM_FILE
    fi

done
