/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.bigtop.itest.sqoop

import org.apache.sqoop.client.SqoopClient
import org.apache.sqoop.validation.Status;

import static org.junit.Assert.assertEquals
import static org.junit.Assert.assertNotNull
import static org.junit.Assert.assertNotSame
import static org.junit.Assert.assertTrue

import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.Test

import org.apache.bigtop.itest.JarContent
import org.apache.bigtop.itest.shell.Shell

class TestSqoopImportPostgres {

  private static String user =
    System.getenv("DB_USER");
  private static String password =
    System.getenv("DB_PASSWORD");
  private static final String DB_USER =
    (user == null) ? "root" : user;
  private static final String DB_PASSWORD =
    (password == null) ? "" : password;

  private static final String DB_HOST =
    System.getProperty("db.host", "localhost");
  private static final String DB_TYPE =
    System.getProperty("db.type", "postgresql");

  private static final String DB_TOOL =
    (DB_TYPE.equals("postgresql") ? "psql" : "mysql");

  private static final String DB_COMMAND =
    "$DB_TOOL -h $DB_HOST --user=$DB_USER" +
    (("".equals(DB_PASSWORD)) ? "" : " --password=$DB_PASSWORD");
  private static final String DB_DBNAME =
    System.getProperty("db.dbname", "psqltestdb");
  private static final String DB_TBLNAME =
    System.getProperty("db.tblname", "psqltesttbl");

  private static final String SQOOP_CONNECTION_STRING =
    "jdbc:$DB_TYPE://$DB_HOST/$DB_DBNAME";
  private static final String SQOOP_CONNECTION =
    "--connect jdbc:$DB_TYPE://$DB_HOST/$DB_DBNAME --username=$DB_USER" +
    (("".equals(DB_PASSWORD)) ? "" : " --password=$DB_PASSWORD");

  static {
    System.out.println("SQOOP_CONNECTION string is " + SQOOP_CONNECTION );
  }

  private static final String IMPORT_DIR = "/tmp/smoke-sqoop-import"

  private static Shell sh = new Shell("/bin/bash -s");

  static void cleanupDir(String dirname) {
    // clean up of existing folders
    sh.exec("hadoop fs -test -e $dirname");
    if (sh.getRet() == 0) {
      sh.exec("hadoop fs -rmr -skipTrash $dirname");
      assertTrue("Deletion of previous $dirname from HDFS failed", sh.getRet() == 0);
    }
  }

  @BeforeClass
  static void setUp() {
  }

  @AfterClass
  static void tearDown() {
    cleanupDir(IMPORT_DIR)
  }

  @Test
  public void testListTables() {
    sh.exec("sqoop-list-tables $SQOOP_CONNECTION");
    assertTrue("Can not list tables present in a database", sh.getRet() == 0)
  }

  @Test
  public void testImport() {
    cleanupDir(IMPORT_DIR)
    sh.exec("sqoop-import $SQOOP_CONNECTION -m 1 --table $DB_TBLNAME --target-dir $IMPORT_DIR");
    assertTrue("Can not import a table", sh.getRet() == 0)
  }

  @Test
  public void testAvdancedImport() {
    cleanupDir(IMPORT_DIR)
    // a direct (fast) import into avro files compressed by gzip
    sh.exec("sqoop-import $SQOOP_CONNECTION -m 1 --table $DB_TBLNAME --target-dir $IMPORT_DIR " +
      "--direct -z --compression-codec gzip --as-avrodatafile");
    assertTrue("Can not import a table", sh.getRet() == 0)
  }
}
