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
package org.apache.geode.internal.metrics;


import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.binder.jvm.JvmGcMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.core.instrument.binder.system.FileDescriptorMetrics;
import io.micrometer.core.instrument.binder.system.ProcessorMetrics;
import io.micrometer.core.instrument.binder.system.UptimeMetrics;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;

public class CacheMeterRegistryFactory implements CompositeMeterRegistryFactory {

  @Override
  public CompositeMeterRegistry create(int systemId, String memberName, String hostName) {
    JvmGcMetrics gcMetricsBinder = new JvmGcMetrics();
    GeodeCompositeMeterRegistry registry = new GeodeCompositeMeterRegistry(gcMetricsBinder);

    MeterRegistry.Config registryConfig = registry.config();
    registryConfig.commonTags("cluster", String.valueOf(systemId));
    registryConfig.commonTags("member", memberName == null ? "" : memberName);
    registryConfig.commonTags("host", hostName == null ? "" : hostName);

    gcMetricsBinder.bindTo(registry);
    new JvmMemoryMetrics().bindTo(registry);
    new JvmThreadMetrics().bindTo(registry);
    new ProcessorMetrics().bindTo(registry);
    new UptimeMetrics().bindTo(registry);
    new FileDescriptorMetrics().bindTo(registry);

    return registry;
  }
}
