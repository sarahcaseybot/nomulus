// Copyright 2020 The Nomulus Authors. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package google.registry.schema.server;

import static com.google.common.flogger.util.Checks.checkArgument;
import static com.google.common.flogger.util.Checks.checkNotNull;
import static google.registry.persistence.transaction.TransactionManagerFactory.jpaTm;
import static google.registry.schema.server.Lock.GLOBAL;

import google.registry.schema.server.Lock.LockId;

/** Data access object class for {@link Lock}. */
public class LockDao {

  public static void save(Lock lock) {
    checkArgument(load(lock.resourceName, lock.tld) == null, "This lock already exists");
    jpaTm()
        .transact(
            () -> {
              jpaTm().getEntityManager().persist(lock);
            });
  }

  public static Lock load(String resourceName, String tld) {
    checkNotNull(resourceName, "The resource name of the lock to load cannot be null");
    checkNotNull(tld, "The tld of the lock to load cannot be null");
    return jpaTm()
        .transact(() -> jpaTm().getEntityManager().find(Lock.class, new LockId(resourceName, tld)));
  }

  public static Lock load(String resourceName) {
    checkNotNull(resourceName, "The resource name of the lock to load cannot be null");
    return jpaTm()
        .transact(
            () -> jpaTm().getEntityManager().find(Lock.class, new LockId(resourceName, GLOBAL)));
  }

  public static void delete(Lock lock) {
    checkArgument(load(lock.resourceName, lock.tld) != null, "The lock to delete does not exist");
    jpaTm()
        .transact(
            () -> {
              jpaTm()
                  .getEntityManager()
                  .remove(
                      jpaTm()
                          .getEntityManager()
                          .find(Lock.class, new LockId(lock.resourceName, lock.tld)));
            });
  }
}
