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

package org.apache.geode.tools.pulse;

import static org.apache.geode.distributed.ConfigurationProperties.HTTP_SERVICE_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.JMX_MANAGER_PORT;
import static org.apache.geode.distributed.ConfigurationProperties.SECURITY_MANAGER;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_CIPHERS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_ENABLED_COMPONENTS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_KEYSTORE_PASSWORD;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_PROTOCOLS;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE;
import static org.apache.geode.distributed.ConfigurationProperties.SSL_TRUSTSTORE_PASSWORD;
import static org.apache.geode.util.test.TestUtil.getResourcePath;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.geode.internal.AvailablePortHelper;
import org.apache.geode.security.SecurableCommunicationChannels;
import org.apache.geode.security.SimpleTestSecurityManager;
import org.apache.geode.test.dunit.rules.LocatorStarterRule;
import org.apache.geode.test.junit.categories.IntegrationTest;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.contrib.java.lang.system.RestoreSystemProperties;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;


@Category(IntegrationTest.class)
public class PulseSecurityWithSSLTest {

  private static File jks =
      new File(getResourcePath(PulseSecurityWithSSLTest.class, "/ssl/trusted.keystore"));

  @Rule
  public LocatorStarterRule locatorStarterRule = new LocatorStarterRule();

  @Rule
  public RestoreSystemProperties restoreSystemProperties = new RestoreSystemProperties();

  private int httpPort = AvailablePortHelper.getRandomAvailableTCPPort();
  private int jmxPort = AvailablePortHelper.getRandomAvailableTCPPort();

  private HttpHost host;
  private HttpClient httpClient;
  private HttpContext context;

  @Test
  public void loginWithIncorrectPassword() throws Exception {
    Properties properties = new Properties();
    properties.setProperty(HTTP_SERVICE_PORT, httpPort + "");
    properties.setProperty(SECURITY_MANAGER, SimpleTestSecurityManager.class.getName());
    properties.setProperty(JMX_MANAGER_PORT, jmxPort + "");

    properties.setProperty(SSL_ENABLED_COMPONENTS, SecurableCommunicationChannels.JMX);
    properties.setProperty(SSL_KEYSTORE, jks.getCanonicalPath());
    properties.setProperty(SSL_KEYSTORE_PASSWORD, "password");
    properties.setProperty(SSL_TRUSTSTORE, jks.getCanonicalPath());
    properties.setProperty(SSL_TRUSTSTORE_PASSWORD, "password");
    properties.setProperty(SSL_PROTOCOLS, "TLSv1.2");
    properties.setProperty(SSL_CIPHERS, "any");

    locatorStarterRule.startLocator(properties);


    host = new HttpHost("localhost", httpPort);
    httpClient = HttpClients.createDefault();
    context = new BasicHttpContext();

    HttpPost request = new HttpPost("/pulse/login");
    List<NameValuePair> nvps = new ArrayList<>();
    nvps.add(new BasicNameValuePair("username", "data"));
    nvps.add(new BasicNameValuePair("password", "wrongPassword"));
    request.setEntity(new UrlEncodedFormEntity(nvps));
    HttpResponse response = httpClient.execute(host, request, context);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/login.html?error=BAD_CREDS");

    // log in with correct password again
    nvps = new ArrayList<>();
    nvps.add(new BasicNameValuePair("username", "data"));
    nvps.add(new BasicNameValuePair("password", "data"));
    request.setEntity(new UrlEncodedFormEntity(nvps));
    response = httpClient.execute(host, request, context);
    assertThat(response.getStatusLine().getStatusCode()).isEqualTo(302);
    assertThat(response.getFirstHeader("Location").getValue())
        .contains("/pulse/clusterDetail.html");
  }
}