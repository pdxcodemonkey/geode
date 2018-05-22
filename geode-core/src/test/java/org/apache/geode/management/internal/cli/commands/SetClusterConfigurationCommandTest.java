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

import static org.apache.geode.management.internal.cli.commands.SetClusterConfigurationCommand.COMMAND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;

import org.apache.commons.io.FileUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;

import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.ClusterConfigurationService;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;


@Category(UnitTest.class)
public class SetClusterConfigurationCommandTest {

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private SetClusterConfigurationCommand command;
  private CommandResult commandResult;
  private File xmlFile;
  private ClusterConfigurationService ccService;
  private String commandWithFile;
  private Configuration configuration;

  @Before
  public void setUp() throws Exception {
    command = spy(SetClusterConfigurationCommand.class);
    ccService = mock(ClusterConfigurationService.class);
    xmlFile = tempFolder.newFile("my.xml");
    commandWithFile = COMMAND + " --xml-file=" + xmlFile.getAbsolutePath() + " ";
    doReturn(true).when(command).isSharedConfigurationRunning();
    doReturn(ccService).when(command).getSharedConfiguration();
    doReturn(Collections.emptySet()).when(command).findMembers(any());
    doReturn(xmlFile).when(command).getXmlFile();
    configuration = new Configuration("group");
  }

  @Test
  public void mandatory() {
    commandResult = gfsh.executeCommandWithInstance(command, COMMAND);
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(commandResult.getContent().toString()).contains("Invalid command");
    commandResult = gfsh.executeCommandWithInstance(command, COMMAND + "--xml-file=''");
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(commandResult.getContent().toString()).contains("Invalid command");
  }

  @Test
  public void defaultValue() {
    GfshParseResult parseResult = gfsh.parse(COMMAND + " --xml-file=my.xml");
    assertThat(parseResult.getParamValue("group")).isEqualTo("cluster");
    assertThat(parseResult.getParamValue("xml-file")).isEqualTo("my.xml");
    assertThat(parseResult.getParamValue("configure-running-servers")).isEqualTo("false");
    assertThat(parseResult.getParamValue("ignore-running-servers")).isEqualTo("false");
  }

  @Test
  public void preValidation() {
    commandResult = gfsh.executeCommandWithInstance(command, COMMAND + " --xml-file=abc");
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(commandResult.getContent().toString()).contains("Invalid file type");

    commandResult = gfsh.executeCommandWithInstance(command, commandWithFile + " --group=''");
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(commandResult.getContent().toString()).contains("Group option can not be empty");

    commandResult =
        gfsh.executeCommandWithInstance(command, commandWithFile + " --group='group1,group2'");
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(commandResult.getContent().toString())
        .contains("Only a single group name is supported");

    commandResult = gfsh.executeCommandWithInstance(command,
        commandWithFile + "--ignore-running-servers --configure-running-servers");
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(commandResult.getContent().toString())
        .contains("configure or ignore can not both be true");
  }

  @Test
  public void clusterConfigurationNotRunning() {
    doReturn(false).when(command).isSharedConfigurationRunning();
    commandResult = gfsh.executeCommandWithInstance(command, commandWithFile);
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(commandResult.getContent().toString())
        .contains("Cluster configuration service is not running");
  }

  @Test
  public void noMemberFound() throws IOException {
    String xmlContent = "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><cache/>";
    FileUtils.write(xmlFile, xmlContent, Charset.defaultCharset());
    when(ccService.getConfiguration(any())).thenReturn(configuration);

    commandResult = gfsh.executeCommandWithInstance(command, commandWithFile);
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(configuration.getCacheXmlContent()).isEqualTo(xmlContent);
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(commandResult.getContent().toString())
        .contains("Successfully set the 'cluster' configuration to the content of");
  }

  @Test
  public void invalidXml() throws IOException {
    String xmlContent =
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?><cache><region></cache>";
    FileUtils.write(xmlFile, xmlContent, Charset.defaultCharset());
    when(ccService.getConfiguration(any())).thenReturn(configuration);

    assertThatThrownBy(() -> gfsh.executeCommandWithInstance(command, commandWithFile))
        .hasStackTraceContaining("org.xml.sax.SAXParseException");
  }

  @Test
  public void existingMembers() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any());
    commandResult = gfsh.executeCommandWithInstance(command, commandWithFile);
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(commandResult.getContent().toString()).contains(
        "Can not set the cluster configuration if there are running servers in 'cluster' group");
  }

  @Test
  public void existingMembersWithBounce() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any());
    when(ccService.hasXmlConfiguration()).thenReturn(true);
    commandResult =
        gfsh.executeCommandWithInstance(command, commandWithFile + "--configure-running-servers");
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.ERROR);
    assertThat(commandResult.getContent().toString())
        .contains("Can not configure servers that are already configured");
  }

  @Test
  public void existingMembersWithIgnore() {
    doReturn(Collections.singleton(mock(DistributedMember.class))).when(command).findMembers(any());
    when(ccService.getConfiguration(any())).thenReturn(configuration);

    commandResult =
        gfsh.executeCommandWithInstance(command, commandWithFile + "--ignore-running-servers");
    System.out.println(commandResult.getContent().toString());
    assertThat(commandResult.getStatus()).isEqualTo(Result.Status.OK);
    assertThat(commandResult.getContent().toString())
        .contains("Successfully set the 'cluster' configuration to the content of");
    assertThat(commandResult.getContent().toString())
        .contains("xisting servers are not affected with this configuration change");
  }
}
