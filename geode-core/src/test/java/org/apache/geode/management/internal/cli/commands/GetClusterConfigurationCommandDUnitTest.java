/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.management.internal.cli.commands;

import static org.apache.geode.distributed.internal.DistributionConfig.GROUPS_NAME;
import static org.apache.geode.management.internal.cli.commands.GetClusterConfigurationCommand.COMMAND;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.test.dunit.rules.LocatorServerStartupRule;
import org.apache.geode.test.dunit.rules.MemberVM;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.rules.GfshShellConnectionRule;


@Category(DistributedTest.class)
public class GetClusterConfigurationCommandDUnitTest {
  @ClassRule
  public static LocatorServerStartupRule cluster = new LocatorServerStartupRule();

  @ClassRule
  public static GfshShellConnectionRule gfsh = new GfshShellConnectionRule();

  @ClassRule
  public static TemporaryFolder tempFolder = new TemporaryFolder();


  private static File xmlFile;
  private static MemberVM locator;
  private CommandResult commandResult;

  @BeforeClass
  public static void beforeClass() throws Exception {
    xmlFile = tempFolder.newFile("my.xml");
    locator = cluster.startLocatorVM(0);
    Properties properties = new Properties();
    properties.setProperty(GROUPS_NAME, "groupB");
    cluster.startServerVM(1, properties, locator.getPort());
    gfsh.connectAndVerify(locator);
    gfsh.executeAndVerifyCommand("create region --name=regionA --type=REPLICATE");
    gfsh.executeAndVerifyCommand("create region --name=regionB --type=REPLICATE --group=groupB");
  }

  @Test
  public void getClusterConfig() {
    commandResult = gfsh.executeAndVerifyCommand(COMMAND);
    commandResult.resetToFirstLine();
    assertThat(commandResult.nextLine()).contains("<region name=\"regionA\">")
        .doesNotContain("<region name=\"regionB\">");
  }


  @Test
  public void getClusterConfigInGroup() {
    commandResult = gfsh.executeAndVerifyCommand(COMMAND + " --group=groupB");
    commandResult.resetToFirstLine();
    assertThat(commandResult.nextLine()).contains("<region name=\"regionB\">")
        .doesNotContain("<region name=\"regionA\">");
  }

  @Test
  public void getClusterConfigWithFile() throws IOException {
    commandResult =
        gfsh.executeAndVerifyCommand(COMMAND + " --xml-file=" + xmlFile.getAbsolutePath());
    commandResult.resetToFirstLine();
    assertThat(commandResult.nextLine())
        .startsWith("cluster configuration exported to " + xmlFile.getAbsolutePath());

    assertThat(xmlFile).exists();
    String content = FileUtils.readFileToString(xmlFile, Charset.defaultCharset());
    assertThat(content).startsWith("<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>")
        .contains("<region name=\"regionA\">");
  }
}
