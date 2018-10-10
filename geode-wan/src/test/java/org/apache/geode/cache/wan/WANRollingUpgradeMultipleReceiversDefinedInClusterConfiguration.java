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
package org.apache.geode.cache.wan;

import static org.apache.geode.distributed.ConfigurationProperties.DISTRIBUTED_SYSTEM_ID;
import static org.apache.geode.distributed.ConfigurationProperties.ENABLE_CLUSTER_CONFIGURATION;
import static org.apache.geode.distributed.ConfigurationProperties.LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.LOG_LEVEL;
import static org.apache.geode.distributed.ConfigurationProperties.MCAST_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.REMOTE_LOCATORS;
import static org.apache.geode.distributed.ConfigurationProperties.USE_CLUSTER_CONFIGURATION;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.awaitility.Awaitility;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.Region;
import org.apache.geode.distributed.Locator;
import org.apache.geode.distributed.internal.InternalClusterConfigurationService;
import org.apache.geode.distributed.internal.InternalLocator;
import org.apache.geode.internal.AvailablePort;
import org.apache.geode.internal.cache.xmlcache.CacheCreation;
import org.apache.geode.internal.cache.xmlcache.CacheXmlGenerator;
import org.apache.geode.management.internal.configuration.domain.Configuration;
import org.apache.geode.management.internal.configuration.utils.XmlUtils;
import org.apache.geode.test.dunit.DistributedTestUtils;
import org.apache.geode.test.dunit.Host;
import org.apache.geode.test.dunit.NetworkUtils;
import org.apache.geode.test.dunit.VM;
import org.apache.geode.test.dunit.cache.internal.JUnit4CacheTestCase;
import org.apache.geode.test.dunit.standalone.DUnitLauncher;
import org.apache.geode.test.dunit.standalone.VersionManager;
import org.apache.geode.test.junit.categories.BackwardCompatibilityTest;
import org.apache.geode.test.junit.categories.DistributedTest;
import org.apache.geode.test.junit.categories.WanTest;
import org.apache.geode.test.junit.runners.CategoryWithParameterizedRunnerFactory;

@Category({DistributedTest.class, BackwardCompatibilityTest.class, WanTest.class})
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(CategoryWithParameterizedRunnerFactory.class)
public class WANRollingUpgradeMultipleReceiversDefinedInClusterConfiguration
    extends JUnit4CacheTestCase {

  @Parameterized.Parameter
  public String oldVersion;

  @Parameterized.Parameter(1)
  public List<Attribute> attributes;

  @Parameterized.Parameter(2)
  public int expectedReceiverElements;

  @Parameterized.Parameter(3)
  public int expectedReceivers;

  @Parameterized.Parameters(name = "from_v{0}; attributes={1}; expectedReceiverCount={2}")
  public static Collection data() {
    // Get initial versions to test against
    List<String> versions = getVersionsToTest();

    // Build up a list of version->attributes->expectedReceivers
    List<Object[]> result = new ArrayList<>();
    versions.forEach(version -> {
      // Add a case for hostname-for-senders
      addReceiversWithHostNameForSenders(result, version);

      // Add a case for bind-address
      addReceiversWithBindAddresses(result, version);

      // Add a case for multiple receivers with default attributes
      addMultipleReceiversWithDefaultAttributes(result, version);

      // Add a case for single receiver with default bind-address
      addSingleReceiverWithDefaultBindAddress(result, version);

      // Add a case for single receiver with default attributes
      addSingleReceiverWithDefaultAttributes(result, version);
    });

    System.out.println("running against these versions and attributes: "
        + result.stream().map(entry -> Arrays.toString(entry)).collect(Collectors.joining(", ")));
    return result;
  }

  private static List<String> getVersionsToTest() {
    // There is no need to test old versions beyond 130. Individual member configuration is not
    // saved in cluster configuration and multiple receivers are not supported starting in 140.
    // Note: This comparison works because '130' < '140'.
    List<String> result = VersionManager.getInstance().getVersionsWithoutCurrent();
    result.removeIf(version -> (version.compareTo(VersionManager.GEODE_140) >= 0));
    if (result.size() < 1) {
      throw new RuntimeException("No older versions of Geode were found to test against");
    }
    return result;
  }

  private static void addReceiversWithHostNameForSenders(List<Object[]> result, String version) {
    List<Attribute> attributes = new ArrayList<>();
    attributes.add(new Attribute("hostname-for-senders", "121.21.21.21"));
    attributes.add(new Attribute("hostname-for-senders", "121.21.21.22"));
    result.add(new Object[] {version, attributes, 2, 0});
  }

  private static void addReceiversWithBindAddresses(List<Object[]> result, String version) {
    List<Attribute> attributes = new ArrayList<>();
    attributes.add(new Attribute("bind-address", "121.21.21.21"));
    attributes.add(new Attribute("bind-address", "121.21.21.22"));
    result.add(new Object[] {version, attributes, 2, 0});
  }

  private static void addMultipleReceiversWithDefaultAttributes(List<Object[]> result,
      String version) {
    List<Attribute> attributes = new ArrayList<>();
    attributes.add(Attribute.DEFAULT);
    attributes.add(Attribute.DEFAULT);
    result.add(new Object[] {version, attributes, 2, 1});
  }

  private static void addSingleReceiverWithDefaultAttributes(List<Object[]> result,
      String version) {
    List<Attribute> attributes = new ArrayList<>();
    attributes.add(Attribute.DEFAULT);
    result.add(new Object[] {version, attributes, 1, 1});
  }

  private static void addSingleReceiverWithDefaultBindAddress(List<Object[]> result,
      String version) {
    List<Attribute> attributes = new ArrayList<>();
    attributes.add(new Attribute("bind-address", "0.0.0.0"));
    result.add(new Object[] {version, attributes, 1, 1});
  }

  @Test
  public void testMultipleReceiversRemovedDuringRoll() throws Exception {
    // Get old locator properties
    VM locator = Host.getHost(0).getVM(oldVersion, 0);
    String hostName = NetworkUtils.getServerHostName();
    final int locatorPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    DistributedTestUtils.deleteLocatorStateFile(locatorPort);
    final String locators = hostName + "[" + locatorPort + "]";

    // Start old locator
    locator.invoke(() -> startLocator(locatorPort, 0, locators, null, true));

    // Wait for configuration configuration to be ready.
    locator.invoke(
        () -> Awaitility.await().atMost(65, TimeUnit.SECONDS).pollInterval(1, TimeUnit.SECONDS)
            .until(() -> assertThat(InternalLocator.getLocator().isSharedConfigurationRunning())
                .isTrue()));

    // Add cluster configuration elements containing multiple receivers
    locator.invoke(() -> addMultipleGatewayReceiverElementsToClusterConfiguration());

    // Roll old locator to current
    rollLocatorToCurrent(locator, locatorPort, 0, locators, null, true);

    // Verify cluster configuration contains expected number of receivers
    locator.invoke(() -> verifyGatewayReceiverClusterConfigurationElements());

    // Start member in current version with cluster configuration enabled
    VM server = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, 1);
    server.invoke(() -> createCache(locators, true, true));

    // Verify member has expected number of receivers
    server.invoke(() -> verifyGatewayReceivers());
  }

  public void createCache(String locators, boolean enableClusterConfiguration,
      boolean useClusterConfiguration) {
    Properties props = new Properties();
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, String.valueOf(enableClusterConfiguration));
    props.setProperty(USE_CLUSTER_CONFIGURATION, String.valueOf(useClusterConfiguration));
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(LOCATORS, locators);
    props.setProperty(LOG_LEVEL, DUnitLauncher.logLevel);
    getCache(props);
  }

  void startLocator(int port, int distributedSystemId, String locators, String remoteLocators,
      boolean enableClusterConfiguration) throws IOException {
    Properties props = getLocatorProperties(distributedSystemId, locators, remoteLocators,
        enableClusterConfiguration);
    Locator.startLocatorAndDS(port, null, props);
  }

  private Properties getLocatorProperties(int distributedSystemId, String locators,
      String remoteLocators, boolean enableClusterConfiguration) {
    Properties props = new Properties();
    props.setProperty(MCAST_PORT, "0");
    props.setProperty(DISTRIBUTED_SYSTEM_ID, String.valueOf(distributedSystemId));
    props.setProperty(LOCATORS, locators);
    if (remoteLocators != null) {
      props.setProperty(REMOTE_LOCATORS, remoteLocators);
    }
    props.setProperty(LOG_LEVEL, DUnitLauncher.logLevel);
    props.setProperty(ENABLE_CLUSTER_CONFIGURATION, String.valueOf(enableClusterConfiguration));
    return props;
  }

  VM rollLocatorToCurrent(VM rollLocator, int port, int distributedSystemId, String locators,
      String remoteLocators, boolean enableClusterConfiguration) {
    rollLocator.invoke(() -> stopLocator());
    VM newLocator = Host.getHost(0).getVM(VersionManager.CURRENT_VERSION, rollLocator.getId());
    newLocator.invoke(() -> startLocator(port, distributedSystemId, locators, remoteLocators,
        enableClusterConfiguration));
    return newLocator;
  }

  private void stopLocator() throws Exception {
    InternalLocator.getLocator().stop();
  }

  private void addMultipleGatewayReceiverElementsToClusterConfiguration() throws Exception {
    // Create empty xml document
    CacheCreation creation = new CacheCreation();
    final StringWriter stringWriter = new StringWriter();
    final PrintWriter printWriter = new PrintWriter(stringWriter);
    CacheXmlGenerator.generate(creation, printWriter, true, false, false);
    printWriter.close();
    String baseXml = stringWriter.toString();
    Document document = XmlUtils.createDocumentFromXml(baseXml);

    // Add gateway-receiver for each attribute
    for (Attribute attribute : attributes) {
      Node rootNode = document.getDocumentElement();
      Element receiverElement = document.createElement("gateway-receiver");
      if (!attribute.name.equals("default")) {
        receiverElement.setAttribute(attribute.name, attribute.value);
      }
      rootNode.appendChild(receiverElement);
    }
    assertThat(document.getElementsByTagName("gateway-receiver").getLength())
        .isEqualTo(expectedReceiverElements);

    // Get configuration region
    Region<String, Configuration> configurationRegion = CacheFactory.getAnyInstance()
        .getRegion(InternalClusterConfigurationService.CONFIG_REGION_NAME);

    // Create a configuration and put into the configuration region
    Configuration configuration =
        new Configuration(InternalClusterConfigurationService.CLUSTER_CONFIG);
    configuration.setCacheXmlContent(XmlUtils.prettyXml(document));
    configurationRegion.put(InternalClusterConfigurationService.CLUSTER_CONFIG, configuration);
  }

  private void verifyGatewayReceiverClusterConfigurationElements() throws Exception {
    // Get configuration region
    Region<String, Configuration> configurationRegion = CacheFactory.getAnyInstance()
        .getRegion(InternalClusterConfigurationService.CONFIG_REGION_NAME);

    // Get the configuration from the region
    Configuration configuration =
        configurationRegion.get(InternalClusterConfigurationService.CLUSTER_CONFIG);

    // Verify the configuration contains no gateway-receiver elements
    Document document = XmlUtils.createDocumentFromXml(configuration.getCacheXmlContent());
    assertThat(document.getElementsByTagName("gateway-receiver").getLength())
        .isEqualTo(expectedReceivers);
  }

  private void verifyGatewayReceivers() {
    assertThat(CacheFactory.getAnyInstance().getGatewayReceivers().size())
        .isEqualTo(expectedReceivers);
  }

  private static class Attribute implements Serializable {

    private String name;

    private String value;

    private static final Attribute DEFAULT = new Attribute("default", "");

    Attribute(String name, String value) {
      this.name = name;
      this.value = value;
    }

    public String toString() {
      return new StringBuilder().append(this.name).append("=").append(this.value).toString();
    }
  }
}
