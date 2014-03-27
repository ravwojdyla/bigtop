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
package org.apache.bigtop.itest.httpfs

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.bigtop.itest.shell.Shell;
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory

public class TestHttpFs {
    private static Log LOG = LogFactory.getLog(TestHttpFs.class)
 
    private static final String USERNAME = System.getProperty("user.name");
    private static final String HTTPFS_PROXY = System.getenv('HTTPFS_PROXY');
    static {
        assertNotNull("HTTPFS_PROXY has to be set to run this test",
            HTTPFS_PROXY);
    }
  
    private static final String HTTPFS_PREFIX = "$HTTPFS_PROXY/webhdfs/v1";
    private static final String HTTPFS_SUCCESS = "{\"boolean\":true}";
    
    private static final String DATA_DIR = System.getProperty("data.dir", "text-files");
        
    private static String testHttpFsFolder =  "/tmp/httpfssmoke-" + (new Date().getTime());
    private static String testHttpFsFolderRenamed = "$testHttpFsFolder-renamed";
    
    private static Shell sh = new Shell("/bin/bash");
    // it will used to cleanup directories, as they are created with via curl with user.name=$USERNAME
    private static Shell shUSERNAME = new Shell("/bin/bash", USERNAME);

    @BeforeClass
    public static void setUp() {
    }

    @AfterClass
    public static void tearDown() {
        // clean up of existing folders using USERNAME of user who created them via curl
        shUSERNAME.exec("hadoop fs -test -d $testHttpFsFolder");
        if (shUSERNAME.getRet() == 0) {
            shUSERNAME.exec("hadoop fs -rmr -skipTrash $testHttpFsFolder");
            assertTrue("Deletion of previous testHttpFsFolder from HDFS failed",
                shUSERNAME.getRet() == 0);
        }
        shUSERNAME.exec("hadoop fs -test -d $testHttpFsFolderRenamed");
        if (shUSERNAME.getRet() == 0) {
            shUSERNAME.exec("hadoop fs -rmr -skipTrash $testHttpFsFolderRenamed");
            assertTrue("Deletion of previous testHttpFsFolderRenamed from HDFS failed",
                shUSERNAME.getRet() == 0);
        }
    }

    public void assertValueExists(List<String> values, String expected) {
        boolean exists = false;
        for (String value: values) {
            if (value.contains(expected)) {
                exists = true;
            }
        }
        assertTrue(expected + " NOT found!", exists);
    }
    
    private void createDir(String dirname) {
        sh.exec("curl -d '' -i -X PUT '$HTTPFS_PREFIX$dirname?user.name=$USERNAME&op=MKDIRS'");
        assertTrue("curl command to create a dir failed", sh.getRet() == 0);
        assertValueExists(sh.getOut(), HTTPFS_SUCCESS);
        sh.exec("hadoop fs -test -d $dirname");
	assertTrue("a directory $dirname does not exist", sh.getRet() == 0);
    }

    @Test
    public void testCreateDir() {
        createDir(testHttpFsFolder)
    }

    @Test
    public void testRenameDir() { 
        createDir(testHttpFsFolder);
        sh.exec("curl -d '' -i -X PUT '$HTTPFS_PREFIX$testHttpFsFolder?user.name=$USERNAME&op=RENAME&destination=$testHttpFsFolderRenamed'");

	// check if a directory was successfully renamed
        assertTrue("curl command to rename a dir failed", sh.getRet() == 0);
        assertValueExists(sh.getOut(), HTTPFS_SUCCESS);
        sh.exec("hadoop fs -test -d $testHttpFsFolderRenamed");
	assertTrue("a directory $testHttpFsFolderRenamed does not exist", sh.getRet() == 0);
    }

    @Test
    public void testDeleteDir() {
        createDir(testHttpFsFolder);
        sh.exec("curl -i -X DELETE '$HTTPFS_PREFIX$testHttpFsFolder?user.name=$USERNAME&op=DELETE'");

	// check if a directory was successfuly removed
        assertTrue("curl command to delete a dir failed", sh.getRet() == 0);
        assertValueExists(sh.getOut(), HTTPFS_SUCCESS);
        sh.exec("hadoop fs -test -d $testHttpFsFolder");
	assertTrue("a directory $testHttpFsFolder still exists", sh.getRet() != 0);
    }
    
    @Test
    public void testStatusDir() { 
        createDir(testHttpFsFolder);
        sh.exec("curl -i '$HTTPFS_PREFIX$testHttpFsFolder?user.name=$USERNAME&op=GETFILESTATUS'");
        assertTrue("curl command to create a dir failed", sh.getRet() == 0);
        assertValueExists(sh.getOut(), "FileStatus");
        assertValueExists(sh.getOut(), "DIRECTORY");
    }

    @Test
    public void testCreateFile() {
        String filename = "helloworld.txt";
        String filenameContent = 'Hello World!';
        
        createDir(testHttpFsFolder);
        sh.exec("curl -d '' -i -X PUT '$HTTPFS_PREFIX$testHttpFsFolder/$filename?user.name=$USERNAME&op=CREATE'");
        assertTrue("curl command to create a file failed with CREATE command", sh.getRet() == 0);
        String datanodeLocation = null;
        sh.getOut().each {
            if (it.startsWith("Location:")) {
                datanodeLocation = it.split(' ')[1];
                return true;
            }
        }
        LOG.debug("Datanode location: $datanodeLocation");
        sh.exec("curl -i -X PUT -T $DATA_DIR/$filename '$datanodeLocation' --header 'Content-Type:application/octet-stream'");

	// check if the file was created
        assertTrue("curl command to create a file failed when sending data to $datanodeLocation", sh.getRet() == 0);
        sh.exec("hadoop fs -test -e $testHttpFsFolder/$filename");
	assertTrue("a file $testHttpFsFolder/$filename does not exist", sh.getRet() == 0);

	// check if we can read the content of this file
        sh.exec("curl -i -L '$HTTPFS_PREFIX$testHttpFsFolder/$filename?user.name=$USERNAME&op=OPEN'");
        assertTrue("curl command to create a file failed with OPEN command", sh.getRet() == 0);
        assertValueExists(sh.getOut(), filenameContent);
    }
}
