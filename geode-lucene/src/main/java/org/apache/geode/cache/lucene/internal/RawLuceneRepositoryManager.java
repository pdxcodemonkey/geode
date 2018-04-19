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
package org.apache.geode.cache.lucene.internal;

import java.io.IOException;

import org.apache.geode.cache.lucene.LuceneSerializer;
import org.apache.geode.cache.lucene.internal.repository.IndexRepository;
import org.apache.geode.internal.cache.BucketNotFoundException;
import org.apache.geode.internal.cache.PartitionedRegion;

public class RawLuceneRepositoryManager extends PartitionedRepositoryManager {
  public static IndexRepositoryFactory indexRepositoryFactory = new RawIndexRepositoryFactory();

  public RawLuceneRepositoryManager(LuceneIndexImpl index, LuceneSerializer serializer) {
    super(index, serializer);
  }

  @Override
  protected IndexRepository getRepository(Integer bucketId) throws BucketNotFoundException {
    IndexRepository repo = indexRepositories.get(bucketId);
    if (repo != null && !repo.isClosed()) {
      return repo;
    }

    try {
      repo = computeRepository(bucketId, this.serializer, this.index, this.userRegion, repo);
      return repo;
    } catch (IOException e) {
      e.printStackTrace();
    }

    throw new BucketNotFoundException(
        "Colocated index buckets not found for bucket id " + bucketId);
  }

  @Override
  public IndexRepository computeRepository(Integer bucketId, LuceneSerializer serializer,
      InternalLuceneIndex index, PartitionedRegion userRegion, IndexRepository oldRepository)
      throws IOException {
    return indexRepositoryFactory.computeIndexRepository(bucketId, serializer, index, userRegion,
        oldRepository, this);
  }
}
