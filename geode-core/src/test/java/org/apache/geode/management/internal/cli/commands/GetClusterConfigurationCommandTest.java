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

import static org.apache.geode.management.internal.cli.commands.GetClusterConfigurationCommand.COMMAND;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.test.junit.categories.UnitTest;
import org.apache.geode.test.junit.rules.GfshParserRule;


@Category(UnitTest.class)
public class GetClusterConfigurationCommandTest {
  private static String CLUSTER_XML =
      "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"no\"?>\n"
          + "<cache xmlns=\"http://geode.apache.org/schema/cache\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" copy-on-read=\"false\" is-server=\"false\" lock-lease=\"120\" lock-timeout=\"60\" search-timeout=\"300\" version=\"1.0\" xsi:schemaLocation=\"http://geode.apache.org/schema/cache http://geode.apache.org/schema/cache/cache-1.0.xsd\">\n"
          + "<region name=\"regionForCluster\">\n"
          + "    <region-attributes data-policy=\"replicate\" scope=\"distributed-ack\"/>\n"
          + "  </region>\n" + "</cache>\n";

  @ClassRule
  public static GfshParserRule gfsh = new GfshParserRule();

  private GetClusterConfigurationCommand command;
  private InternalClusterConfigurationService ccService;
  private Configuration configuration;

  @Before
  public void setUp() throws Exception {
    command = spy(GetClusterConfigurationCommand.class);
    ccService = mock(InternalClusterConfigurationService.class);
    doReturn(true).when(command).isSharedConfigurationRunning();
    doReturn(ccService).when(command).getConfigurationService();
    configuration = new Configuration("cluster");
  }

  @Test
  public void checkDefaultValue() {
    GfshParseResult parseResult = gfsh.parse(COMMAND + " --xml-file=my.xml");
    assertThat(parseResult.getParamValue("group")).isEqualTo("cluster");
    assertThat(parseResult.getParamValue("xml-file")).isEqualTo("my.xml");
  }

  @Test
  public void checkGroupNames() {
    gfsh.executeAndAssertThat(command, COMMAND + " --group=''").statusIsError()
        .containsOutput("Group option can not be empty");

    gfsh.executeAndAssertThat(command, COMMAND + " --group='group1,group2'").statusIsError()
        .containsOutput("Only a single group name is supported");
  }

  @Test
  public void clusterConfigurationNotRunning() {
    doReturn(false).when(command).isSharedConfigurationRunning();

    gfsh.executeAndAssertThat(command, COMMAND).statusIsError()
        .containsOutput("Cluster configuration service is not running");
  }

  @Test
  public void groupNotExist() {
    when(ccService.getConfiguration("groupA")).thenReturn(null);
    gfsh.executeAndAssertThat(command, COMMAND + " --group=groupA").statusIsError()
        .containsOutput("No cluster configuration for 'groupA'.");
  }

  @Test
  public void get() {
    when(ccService.getConfiguration(any())).thenReturn(configuration);
    configuration.setCacheXmlContent(CLUSTER_XML);
    gfsh.executeAndAssertThat(command, COMMAND).statusIsSuccess()
        .containsOutput("<?xml version=\\\"1.0\\\"").containsOutput("<\\/cache>");
  }
}
