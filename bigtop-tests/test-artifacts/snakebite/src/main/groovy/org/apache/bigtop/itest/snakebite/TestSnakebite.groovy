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
package org.apache.bigtop.itest.hadoop.snakebite;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.bigtop.itest.shell.Shell;
import org.apache.bigtop.itest.TestUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TestSnakebite {
    private static Log LOG = LogFactory.getLog(TestSnakebite.class);
    private static final String USERNAME = System.getProperty("user.name");
    private static Shell shell = new Shell("/bin/bash", USERNAME);
    private static String smokeTestFolder =
        "/tmp/snakebite-smoke-" + (new Date().getTime());
    private static String smokeTestFolderTmp =
        "/tmp/snakebite-smoke-" + (new Date().getTime()) + "_tmp";
    private static String testFile = "elephants.txt";
    private static final String DATA_DIR = System.getProperty("data.dir", "text-files");
    
    @BeforeClass
    public static void setUp() {
        // Snakebite shuld be generating a config automatically.
        // Delete the .snakebiterc if it exists, then recreate and verify
        if(shell.exec("test -e ~/.snakebiterc").getRet() == 0) {
            assertTrue("Deletion of ~/.snakebiterc failed",
                       (shell.exec("rm ~/.snakebiterc").getRet() == 0));
        }

        // Create the .snakbiterc file by running a simple command
        shell.exec("snakebite ls");
        assertTrue("Creation of ~/.snakebiterc failed", (shell.getRet() == 0));
    }
    
    @AfterClass
    public static void tearDown() {
        if (testDir(smokeTestFolder)) {
            shell.exec("snakebite rm -R $smokeTestFolder");
            assertTrue("Deletion of previous smokeTestFolder from HDFS failed",
                       shell.getRet() == 0);
        }
        if (testDir(smokeTestFolderTmp)) {
            shell.exec("snakebite rm -R $smokeTestFolderTmp");
            assertTrue("Deletion of previous smokeTestFolderTmp from HDFS failed",
                       shell.getRet() == 0);
        }
    }
    
    public void assertValueExists(List<String> values, String expected) {
        LOG.debug("EXPECTED = \"" + expected + "\"");
        boolean exists = false;
        for (String value: values) {
            LOG.debug("FOUND = \"" + value + "\"");
            if (expected.startsWith(value)) {
                exists = true;
            }
        }
        assertTrue(expected + " NOT found!", exists);
    }
    
    public static boolean createDir(String dirname) {
        shell.exec("snakebite mkdir $dirname");
        return (shell.getRet() == 0);
    }
    
    public static boolean createFile(String filename) {
        shell.exec("snakebite touchz $filename");
        return (shell.getRet() == 0);
    }
    
    public static boolean rmDir(String dirname) {
        shell.exec("snakebite rmdir $dirname");
        return (shell.getRet() == 0);
    }
    
    public static boolean rmFile(String filename) {
        shell.exec("snakebite rm $filename");
        return (shell.getRet() == 0);
    }
    
    public static boolean testDir(String dirname) {
        shell.exec("snakebite test -d $dirname");
        return (shell.getRet() == 0);
    }
    
    public static boolean testFile(String filename) {
        shell.exec("snakebite test -e $filename");
        return (shell.getRet() == 0);
    }
    
    public static boolean chMod(String target) {
        shell.exec("snakebite chmod 777 $target");
        return (shell.getRet() == 0);
    }
    
    public static boolean touchz(String target) {
        shell.exec("snakebite touchz $target");
        return (shell.getRet() == 0);
    }
    
    public static boolean mvFile(String src, String dst) {
        shell.exec("snakebite mv $src $dst");
        return (shell.getRet() == 0);
    }
    
    public static boolean cpFile(String src, String dst) {
        shell.exec("snakebite cp $src $dst");
        return (shell.getRet() == 0);
    }
    
    // Currently, no way to use snakebite to move a file to hdfs
    public static boolean putFile(String srcFile, String targetFile) {
        shell.exec("hdfs dfs -put $srcFile $targetFile");
        return (shell.getRet() == 0);
    }
    
    // Tests actually begin here
    @Test
    public void testCreateDir() {
        assertTrue("Snakebite command to create directory failed",
                   createDir(smokeTestFolder));
    }
    
    @Test
    public void testRmDir() {
        if(testDir(smokeTestFolder) == false) {
            createDir(smokeTestFolder);
        }
        assertTrue("Folder does not exist before deletion",
                   testDir(smokeTestFolder));
        assertTrue("Snakebite command to remove directory failed",
                   rmDir(smokeTestFolder));
    }
    
    @Test
    public void testCatAndRmFile() {
        String expectedOutput = "I love elephants!";
        String inputFile = "${DATA_DIR}/${testFile}";
        String outputFile = "${smokeTestFolder}/${testFile}";
        if (testDir(smokeTestFolder) == false) {
            assertTrue("Create Directory failed", createDir(smokeTestFolder));
        }
        
        // Chmod 777 the directory (since snakbite uses hdfs user by default)
        assertTrue("Chmod 777 of $smokeTestFolder failed", chMod(smokeTestFolder));
        
        // Put the file using the hdfs command
        assertTrue("Hdfs put command failed", putFile(inputFile, outputFile));
        
        // Cat the file and check output
        shell.exec("snakebite cat $outputFile");
        assertTrue("Snakebite cat command failed", shell.getRet() == 0);
        assertValueExists(shell.getOut(), expectedOutput);
        
        shell.exec("snakebite rm $outputFile");
        assertTrue("Snakebite rm command failed", shell.getRet() == 0);
    }
    
    @Test
    public void testCreateAndDeleteFile() {
        String tempFileName = "someFile";
        if (testDir(smokeTestFolder) == false) {
            assertTrue("Create Directory failed", createDir(smokeTestFolder));
        }
        
        //Make sure file does not exist
        assertTrue("Temp file exists, but should not",
                   (testFile(tempFileName) == false));
        
        //Create file and make sure it exists
        assertTrue("Failed to touch a zero length file", touchz(tempFileName));
        assertTrue("File does not exist in the expected location",
                   testFile(tempFileName));
        
        //Delete file
        assertTrue("Failed to delete created file", rmFile(tempFileName));
    }
    
    @Test
    public void testMoveFile() {
        String moveFileName = "tmp_move";
        String outputFile = "${smokeTestFolder}/${moveFileName}";
        String outputFile2 = "${smokeTestFolderTmp}/${moveFileName}";
        
        if (testDir(smokeTestFolder) == false) {
            assertTrue("Create Directory failed", createDir(smokeTestFolder));
        }
        
        if (testDir(smokeTestFolderTmp) == false) {
            assertTrue("Create Temp Directory failed", createDir(smokeTestFolderTmp));
        }
        
        // Touch a file in the original dir
        assertTrue("Touch command failed before the move", touchz(outputFile));
        
        // Try and move the file
        assertTrue("Failed to move file", mvFile(outputFile, outputFile2));
        assertTrue("Expected file after move doesn't exist",
                   testFile(outputFile2));
        assertTrue("Original file after move still exists",
                   testFile(outputFile) == false);
    }
    
    /*
     * Commented out because cp is currently not supported for snakebite
     *
     @Test
     public void testCopyFile() {
     String copyFileName = "tmp_copy";
     String outputFile = "${smokeTestFolder}/${copyFileName}";
     String outputFile2 = "${smokeTestFolderTmp}/${copyFileName}";
     
     if (testDir(smokeTestFolder) == false) {
     assertTrue("Create Directory failed", createDir(smokeTestFolder));
     }
     
     if (testDir(smokeTestFolderTmp) == false) {
     assertTrue("Create Temp Directory failed", createDir(smokeTestFolderTmp));
     }
     
     // Touch a file in the original dir
     assertTrue("Touch command failed before the copy", touchz(outputFile));
     
     // Try and copy the file
     assertTrue("Failed to copy file", cpFile(outputFile, outputFile2));
     assertTrue("Original file after copy doesn't exists",
     testFile(outputFile));
     assertTrue("Expected file after copy doesn't exist",
     testFile(outputFile2));
     }
    */
}
