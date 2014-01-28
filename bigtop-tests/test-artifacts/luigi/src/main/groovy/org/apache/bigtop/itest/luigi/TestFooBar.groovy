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
package org.apache.bigtop.itest.luigi

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

public class TestFooBar {
    private static Log LOG = LogFactory.getLog(TestFooBar.class)
 
    private static final String USERNAME = System.getProperty("user.name")
    private static final String LUIGI_CONF_DIR = System.getenv('LUIGI_CONF_DIR')
    static {
        assertNotNull("LUIGI_CONF_DIR has to be set to run this test",
            LUIGI_CONF_DIR)
    }
      
    private static final String EXAMPLES_DIR = System.getProperty("examples.dir", "examples")
    private static final String OUTPUT_DIR = "/tmp/bar"

    private static Shell sh = new Shell("/bin/bash") 

    @BeforeClass
    public static void setUp() {
        if (OUTPUT_DIR != null) {
            sh.exec("mkdir $OUTPUT_DIR")
        }
    }

    @AfterClass
    public static void tearDown() {
        if (OUTPUT_DIR != null) {
            sh.exec("rm -rf $OUTPUT_DIR")
        }
    }

    @Test
    public void testFooBar() {
        int numIterations = 10
        sh.exec("python $EXAMPLES_DIR/foo.py $OUTPUT_DIR $numIterations")
        assertTrue("python command to run the luigi job failed", sh.getRet() == 0)
	sh.exec("[ `ls $OUTPUT_DIR | wc -l` -eq $numIterations ]")
        assertTrue("the luigi job failed to generate a proper output", sh.getRet() == 0)
    }
}
