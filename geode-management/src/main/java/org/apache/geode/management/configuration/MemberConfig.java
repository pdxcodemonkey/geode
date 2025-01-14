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

package org.apache.geode.management.configuration;


import org.apache.geode.annotations.Experimental;
import org.apache.geode.management.api.CorrespondWith;
import org.apache.geode.management.api.RestfulEndpoint;
import org.apache.geode.management.runtime.MemberInformation;

@Experimental
public class MemberConfig extends AbstractConfiguration implements RestfulEndpoint,
    CorrespondWith<MemberInformation> {

  public static final String MEMBER_CONFIG_ENDPOINT = "/members";

  private String id;

  public void setId(String id) {
    this.id = id;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public String getEndpoint() {
    return MEMBER_CONFIG_ENDPOINT;
  }
}
