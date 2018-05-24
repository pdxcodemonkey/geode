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

import static org.apache.geode.management.internal.cli.i18n.CliStrings.GROUP;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;

import org.apache.geode.distributed.ClusterConfigurationService;
import org.apache.geode.management.cli.CliMetaData;
import org.apache.geode.management.cli.Result;
import org.apache.geode.management.internal.cli.AbstractCliAroundInterceptor;
import org.apache.geode.management.internal.cli.GfshParseResult;
import org.apache.geode.management.internal.cli.i18n.CliStrings;
import org.apache.geode.management.internal.cli.result.CommandResult;
import org.apache.geode.management.internal.cli.result.InfoResultData;
import org.apache.geode.management.internal.cli.result.ResultBuilder;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.security.ResourceOperation;
import org.apache.geode.security.ResourcePermission.Operation;
import org.apache.geode.security.ResourcePermission.Resource;

;


public class GetClusterConfigurationCommand extends InternalGfshCommand {
  public static final String COMMAND = "get-cluster-configuration";
  public static final String COMMAND_HELP =
      "this command displays the existing cluster configuration for the specified group.";
  public static final String XML_FILE = "xml-file";

  @CliCommand(value = COMMAND, help = COMMAND_HELP)
  @CliMetaData(
      interceptor = "org.apache.geode.management.internal.cli.commands.GetClusterConfigurationCommand$Interceptor",
      relatedTopic = {CliStrings.TOPIC_GEODE_CONFIG})
  @ResourceOperation(resource = Resource.CLUSTER, operation = Operation.READ)
  public CommandResult getConfig(
      @CliOption(key = GROUP,
          unspecifiedDefaultValue = ClusterConfigurationService.CLUSTER_CONFIG) String group,
      @CliOption(key = XML_FILE) String xmlFile) {
    if (!isSharedConfigurationRunning()) {
      return ResultBuilder
          .createGemFireErrorResult("Cluster configuration service is not running.");
    }

    Configuration configuration = getConfigurationService().getConfiguration(group);
    if (configuration == null) {
      return ResultBuilder.createUserErrorResult("No cluster configuration for '" + group + "'.");
    }

    InfoResultData info = ResultBuilder.createInfoResultData();
    info.addLine(configuration.getCacheXmlContent());

    return ResultBuilder.buildResult(info);
  }

  public static class Interceptor extends AbstractCliAroundInterceptor {
    public Result preExecution(GfshParseResult parseResult) {
      String group = parseResult.getParamValueAsString(GROUP);
      if (StringUtils.isBlank(group)) {
        return ResultBuilder.createUserErrorResult("Group option can not be empty.");
      }

      if (group.contains(",")) {
        return ResultBuilder.createUserErrorResult("Only a single group name is supported.");
      }

      String xmlFile = parseResult.getParamValueAsString(XML_FILE);
      if (xmlFile != null) {
        // make sure the file does not exist so that we don't overwrite some existing file
        File file = new File(xmlFile).getAbsoluteFile();
        if (file.exists()) {
          String message = file.getAbsolutePath() + " already exists. Overwrite it? ";
          if (readYesNo(message, Response.YES) == Response.NO) {
            return ResultBuilder
                .createShellClientAbortOperationResult("Aborted. " + xmlFile + "already exists.");
          }
        }
      }
      return ResultBuilder.createInfoResult("");
    }

    @Override
    public Result postExecution(GfshParseResult parseResult, Result commandResult, Path tempFile) {
      String xmlFile = parseResult.getParamValueAsString(XML_FILE);
      String group = parseResult.getParamValueAsString(GROUP);
      // save the result to the file
      if (xmlFile != null && commandResult.getStatus() == Result.Status.OK) {
        File file = new File(xmlFile).getAbsoluteFile();
        try {
          FileUtils.write(file, commandResult.nextLine(), Charset.defaultCharset());
          return ResultBuilder
              .createInfoResult(group + " configuration exported to " + file.getAbsolutePath());
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      return commandResult;
    }

  }
}
