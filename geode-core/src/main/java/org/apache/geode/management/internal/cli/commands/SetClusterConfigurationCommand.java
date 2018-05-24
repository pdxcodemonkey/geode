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

import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toSet;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.xml.sax.SAXException;

import org.apache.geode.cache.execute.ResultCollector;
import org.apache.geode.distributed.ClusterConfigurationService;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.functions.CliFunctionResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.remote.CommandExecutionContext;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.FileResult;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.functions.GetRegionNamesFunction;
import org.apache.geode.management.internal.configuration.functions.RecreateCacheFunction;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission;


public class SetClusterConfigurationCommand extends InternalGfshCommand {
  public static final String COMMAND = "set-cluster-configuration";
  public static final String COMMAND_HELP =
      "this commands overwrites the existing cluster configuration for the specified group.";
  public static final String XML_FILE = "xml-file";
  public static final String CONFIGURE_RUNNING_SERVERS = "configure-running-servers";
  public static final String CONFIGURE_HELP =
      "Running servers will be configured with the content of xml file only when the these servers do not have any existing configurations.";
  public static final String IGNORE_RUNNING_SERVERS = "ignore-running-servers";
  public static final String IGNORE_HELP =
      "Running servers will be left alone. It's assumed that the content of the xml you are setting here has no conflict with these servers' configuration.";


  @CliCommand(value = COMMAND, help = COMMAND_HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.SetClusterConfigurationCommand$Interceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = ResourcePermission.Resource.CLUSTER,
      operation = ResourcePermission.Operation.MANAGE)
  public CommandResult setConfig(
      @CliOption(key = CliStrings.GROUP,
          unspecifiedDefaultValue = ClusterConfigurationService.CLUSTER_CONFIG) String group,
      @CliOption(key = XML_FILE, mandatory = true) String xmlFile,
      @CliOption(key = CONFIGURE_RUNNING_SERVERS, help = CONFIGURE_HELP,
          specifiedDefaultValue = "true", unspecifiedDefaultValue = "false") boolean configure,
      @CliOption(key = IGNORE_RUNNING_SERVERS, help = IGNORE_HELP, specifiedDefaultValue = "true",
          unspecifiedDefaultValue = "false") boolean ignore)
      throws IOException, TransformerException, ParserConfigurationException, SAXException {
    if (!isSharedConfigurationRunning()) {
      return ResultBuilder
          .createGemFireErrorResult("Cluster configuration service is not running.");
    }

    InternalClusterConfigurationService ccService = getConfigurationService();
    Set<DistributedMember> serversInGroup = findMembers(group);

    InfoResultData infoData = ResultBuilder.createInfoResultData();
    ccService.lockSharedConfiguration();
    try {
      if (serversInGroup.size() > 0) {
        if (!configure && !ignore) {
          String message = String.format(
              "Can not set the cluster configuration if there are running servers in '%s' group and there no options to indicate what to do with them.",
              group);
          return ResultBuilder.createUserErrorResult(message);
        }

        if (configure) {
          // make sure the servers are vanilla servers, users hasn't done anything on them.
          // server might belong to multiple group, so we can't just check one group's xml is null,
          // has to make sure
          // all group's xml are null
          if (ccService.hasXmlConfiguration()) {
            return ResultBuilder
                .createUserErrorResult("Can not configure servers that are already configured.");
          }
          // if no existing cluster configuration, to be safe, further check to see if running
          // servers has regions already defined
          Set<String> regionsOnServers = serversInGroup.stream().map(this::getRegionNamesOnServer)
              .flatMap(Collection::stream).collect(toSet());

          if (!regionsOnServers.isEmpty()) {
            return ResultBuilder
                .createGemFireErrorResult("Can not configure servers with existing regions: "
                    + regionsOnServers.stream().collect(joining(",")));
          }
        }
      }

      File file = getXmlFile();

      // update the xml in the cluster configuration service
      Configuration configuration = ccService.getConfiguration(group);
      if (configuration == null) {
        configuration = new Configuration(group);
      }
      configuration.setCacheXmlFile(file);
      ccService.setConfiguration(group, configuration);
      infoData.addLine(
          "Successfully set the '" + group + "' configuration to the content of " + xmlFile);
    } finally {
      ccService.unlockSharedConfiguration();
    }

    if (serversInGroup.size() > 0) {
      if (configure) {
        infoData.addLine("Configure the servers in '" + group + "' group: ");
        // if at this point, we are pretty sure that the running servers are vanilla servers, it's
        // safe to recreate
        // the cache. This would help if users started locators and servers already, but hasn't done
        // anything else, it's
        // ok to set the configuration xml
        // Restart the cache of each member on this group only
        Set<CliFunctionResult> functionResults =
            serversInGroup.stream().map(this::reCreateCache).collect(toSet());
        for (CliFunctionResult functionResult : functionResults) {
          if (functionResult.isSuccessful()) {
            infoData.addLine("Successfully configured " + functionResult.getMemberIdOrName());
          } else {
            infoData.addLine("Failed to configure " + functionResult.getMemberIdOrName()
                + " due to " + functionResult.getMessage());
          }
        }
      } else if (ignore) {
        infoData.addLine("Existing servers are not affected with this configuration change.");
      }
    }

    return ResultBuilder.buildResult(infoData);
  }

  File getXmlFile() {
    List<String> filePathFromShell = CommandExecutionContext.getFilePathFromShell();
    File file = new File(filePathFromShell.get(0));
    return file;
  }

  Set<DistributedMember> findMembers(@CliOption(key = CliStrings.GROUP,
      unspecifiedDefaultValue = ClusterConfigurationService.CLUSTER_CONFIG) String group) {
    Set<DistributedMember> serversInGroup;
    if (ClusterConfigurationService.CLUSTER_CONFIG.equals(group)) {
      serversInGroup = getAllNormalMembers();
    } else {
      serversInGroup = findMembers(new String[] {group}, null);
    }
    return serversInGroup;
  }

  Set<String> getRegionNamesOnServer(DistributedMember server) {
    ResultCollector rc = executeFunction(new GetRegionNamesFunction(), null, server);
    List<Set<String>> results = (List<Set<String>>) rc.getResult();
    return results.get(0);
  }

  CliFunctionResult reCreateCache(DistributedMember server) {
    ResultCollector rc = executeFunction(new RecreateCacheFunction(), null, server);
    List<CliFunctionResult> results = (List<CliFunctionResult>) rc.getResult();
    return results.get(0);
  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    public Result preExecution(GfshParseResult parseResult) {
      String xmlFile = parseResult.getParamValueAsString(XML_FILE);

      if (!xmlFile.endsWith("xml")) {
        return ResultBuilder
            .createUserErrorResult("Invalid file type. The file extension must be .xml.");
      }

      String group = parseResult.getParamValueAsString(CliStrings.GROUP);
      if (StringUtils.isBlank(group)) {
        return ResultBuilder.createUserErrorResult("Group option can not be empty.");
      }

      if (group.contains(",")) {
        return ResultBuilder.createUserErrorResult("Only a single group name is supported.");
      }

      boolean reConfigure = (Boolean) parseResult.getArguments()[2];
      boolean ignore = (Boolean) parseResult.getArguments()[3];

      if (reConfigure && ignore) {
        return ResultBuilder.createUserErrorResult("configure or ignore can not both be true.");
      }

      FileResult fileResult = new FileResult();
      File file = new File(xmlFile).getAbsoluteFile();
      if (!file.exists()) {
        return ResultBuilder.createUserErrorResult("'" + xmlFile + "' not found.");
      }

      fileResult.addFile(file);

      String message = "This command will replace the existing cluster configuration, if any, for `"
          + group + "` group" + " with the content of " + xmlFile + "\n"
          + "Please make sure you have backed up the old configuration in case you need it later.\n\n"
          + "Continue? ";

      if (readYesNo(message, Response.YES) == Response.NO) {
        return ResultBuilder
            .createShellClientAbortOperationResult("Aborted import of " + xmlFile + ".");
      }

      if (ignore) {
        message =
            "When --ignore-running-servers, the configuration you are trying to import should NOT have any conflict with the configuration"
                + "of existing running servers if any, otherwise you may not be able to start new servers. "
                + "\nIt is also expected that you would restart the servers with the old configuration after new servers have come up."
                + "\n\nContinue? ";
        if (readYesNo(message, Response.YES) == Response.NO) {
          return ResultBuilder
              .createShellClientAbortOperationResult("Aborted import of " + xmlFile + ".");
        }
      }

      return fileResult;
    }
  }
}
