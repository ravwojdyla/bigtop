/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.bigtop.itest.hadoop.scalding

import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertTrue
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test
import org.apache.bigtop.itest.shell.Shell
import org.apache.bigtop.itest.TestUtils
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

public class TestWordcount {
    private static Log LOG = LogFactory.getLog(TestWordcount.class)

    private static final String RUNNER_SCRIPT = "hadoop jar" 

    private static final String USERNAME = System.getProperty("user.name")
    private static final String DATA_DIR = System.getProperty("data.dir", "data")
    private static final String WORDCOUNT_JAR = System.getenv("SCALDING_WORDCOUNT_JAR")
    static { 
	assertNotNull("SCALDING_WORDCOUNT_JAR has to be set to run this test",
		WORDCOUNT_JAR)
    }

    private static final String WORDCOUNT_CLASS = System.getenv("SCALDING_WORDCOUNT_CLASS");
    static {
        if (WORDCOUNT_CLASS == null) {
		WORDCOUNT_CLASS = "com.snowplowanalytics.hadoop.scalding.WordCountJob";
	}
    }
    private static final String OUTPUT_DIR = "/tmp/smoke-scalding-output"
    private static final String INPUT_DIR = "/tmp/smoke-scalding-input"

    private static Shell shHDFS = new Shell("/bin/bash", USERNAME)
    private static Shell sh = new Shell("/bin/bash")

    @BeforeClass
    public static void setUp() {
        if (INPUT_DIR != null) {
            sh.exec("mkdir $INPUT_DIR")
            sh.exec("cp $DATA_DIR/* $INPUT_DIR")
            sh.exec("hadoop fs -mkdir $INPUT_DIR")
            sh.exec("hadoop fs -put $DATA_DIR/* $INPUT_DIR")
        }

        if (OUTPUT_DIR != null) {
            sh.exec("mkdir $OUTPUT_DIR")
        }
    }

    @AfterClass
    public static void tearDown() {
        if (INPUT_DIR != null) {
            sh.exec("rm -rf $INPUT_DIR")
            sh.exec("hadoop fs -rmr $INPUT_DIR")
        }
        if (OUTPUT_DIR != null) {
            sh.exec("rm -rf $OUTPUT_DIR")
            sh.exec("hadoop fs -rmr $OUTPUT_DIR")
        }
    }

    @Test
    public void testWordCountHadoop() {
        String outputFile = "$OUTPUT_DIR/helloword-hadoop.txt"
        String expectedFile = "wordcount-helloworld-hdfs.expected"

        sh.exec("$RUNNER_SCRIPT $WORDCOUNT_JAR $WORDCOUNT_CLASS --input $INPUT_DIR/helloworld.txt --output $OUTPUT_DIR --hdfs")
        assertTrue("run of scalding job failed", sh.getRet() == 0)
        sh.exec("hadoop fs -getmerge $OUTPUT_DIR/part* $outputFile")
        assertTrue("Unable to getmerge the output of the scalding job", sh.getRet() == 0)
        sh.exec("diff $outputFile $expectedFile")
        assertTrue("the scalding job failed to generate a proper output", sh.getRet() == 0)
    }
}
