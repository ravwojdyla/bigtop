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
package org.apache.bigtop.itest.hadoop.hdfs;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.apache.bigtop.itest.JarContent;
import org.apache.bigtop.itest.shell.Shell;
import org.apache.hadoop.conf.Configuration;

public class TestNameNodeHA {
 
  private static Shell shHDFS = new Shell("/bin/bash", "hdfs");
  private static Configuration conf;
  private static String namenode1;
  private static String namenode2;

  private static String ACTIVE = "active"
  private static String STANDBY = "standby"

  @BeforeClass
  public static void setUp() {
    conf = new Configuration();
    conf.addResource('hdfs-site.xml');

    String dfsNameservices = conf.get("dfs.nameservices");
    assertNotNull("dfs.nameservices has to be set to run this test", dfsNameservices)

    String dfsHANamenodes = conf.get("dfs.ha.namenodes.$dfsNameservices");
    assertNotNull("dfs.ha.namenodes.$dfsNameservices has to be set to run this test", dfsHANamenodes)

    String[] namenodes = dfsHANamenodes.split(',');
    namenode1 = namenodes[0];
    namenode2 = namenodes[1];
    assertNotNull("namenode1 has to be set to run this test", namenode1)
    assertNotNull("namenode2 has to be set to run this test", namenode2)
  }

  @AfterClass
  public static void tearDown() {
  }

  private String getServiceState(String namenode) {
    shHDFS.exec("hdfs haadmin -getServiceState $namenode")
    assertTrue("hdfs haadmin -getServiceState command failed",
      shHDFS.getRet() == 0)

    String state = shHDFS.getOut()[0]
    assertTrue("Namenode is not in active nor in standby state",
      isActiveOrStandbyState(state))

    return state
  }

  private boolean isActiveOrStandbyState(String state) {
    return state.equals(ACTIVE) || state.equals(STANDBY)
  }

  private boolean isActiveStandbyState(String state1, String state2) {
    return (state1.equals(ACTIVE) && state2.equals(STANDBY)) || (state2.equals(ACTIVE) && state1.equals(STANDBY))
  }

  @Test
  public void testActiveStandbyConfiguration() {
    String state1 = getServiceState(namenode1)
    String state2 = getServiceState(namenode2)

    assertTrue("Namenodes are not in active-standby configuration ",
      isActiveStandbyState(state1, state2))
  }

  @Test
  public void testTransitionToStandbyAndActive() {
    String state1 = getServiceState(namenode1)
    String state2 = getServiceState(namenode2)

    assertTrue("Namenodes are not in active-standby configuration ",
      isActiveStandbyState(state1, state2))

    String activeNN = (state1.equals(ACTIVE) ? namenode1 : namenode2)

    // move the active NN into standby-mode
    shHDFS.exec("hdfs haadmin -transitionToStandby $activeNN")
    assertTrue("hdfs haadmin -transitionToStandby command failed",
      shHDFS.getRet() == 0)
    assertTrue("Previously active NN is not standby after transition",
      getServiceState(activeNN).equals(STANDBY))

    // move this NN back from standby-mode to active-mode
    shHDFS.exec("hdfs haadmin -transitionToActive $activeNN")
    assertTrue("hdfs haadmin -transitionToActive command failed",
      shHDFS.getRet() == 0)
    assertTrue("Previously standy NN is not active after transition",
      getServiceState(activeNN).equals(ACTIVE))
  }

  @Test
  public void testFailover() {
    String state1 = getServiceState(namenode1)
    String state2 = getServiceState(namenode2)

    assertTrue("Namenodes are not in active-standby configuration ",
      isActiveStandbyState(state1, state2))

    String activeNN = (state1.equals(ACTIVE) ? namenode1 : namenode2)
    String standbyNN = (state1.equals(STANDBY) ? namenode1 : namenode2)

    // perform failover
    shHDFS.exec("hdfs haadmin -failover $activeNN $standbyNN")
    assertTrue("hdfs haadmin -failover command failed",
      shHDFS.getRet() == 0)

    // check if failover was successful (assuming no fencing)
    assertTrue("Previously active NN is not standby after failover",
      getServiceState(activeNN).equals(STANDBY))
    assertTrue("Previously standby NN is not active after failover",
      getServiceState(standbyNN).equals(ACTIVE))
  }
}

