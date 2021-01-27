// Copyright 2021 The Nomulus Authors. All Rights Reserved.
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

package google.registry.model.common;

import static com.google.common.base.Preconditions.checkNotNull;
import static google.registry.config.RegistryConfig.getSingletonCacheRefreshDuration;
import static google.registry.model.common.EntityGroupRoot.getCrossTldKey;
import static google.registry.persistence.transaction.TransactionManagerFactory.ofyTm;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableSortedMap;
import com.googlecode.objectify.Key;
import com.googlecode.objectify.annotation.Embed;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Id;
import com.googlecode.objectify.annotation.Mapify;
import com.googlecode.objectify.annotation.Parent;
import google.registry.model.ImmutableObject;
import google.registry.model.common.TimedTransitionProperty.TimeMapper;
import google.registry.model.common.TimedTransitionProperty.TimedTransition;
import google.registry.persistence.VKey;
import google.registry.schema.replay.DatastoreOnlyEntity;
import java.util.Optional;
import javax.annotation.concurrent.Immutable;
import org.joda.time.DateTime;

@Entity
@Immutable
public class DatabaseTransitionSchedule extends ImmutableObject implements DatastoreOnlyEntity {

  /**
   * The name of the database to be treated as the primary database. The first entry in the schedule
   * will always be Datastore.
   */
  public enum PrimaryDatabase {
    DATASTORE,
    CLOUD_SQL
  }

  /**
   * The transition to a specified primary database at a specific point in time, for use in a
   * TimedTransitionProperty.
   */
  @Embed
  public static class PrimaryDatabaseTransition extends TimedTransition<PrimaryDatabase> {
    private PrimaryDatabase primaryDatabase;

    @Override
    protected PrimaryDatabase getValue() {
      return primaryDatabase;
    }

    @Override
    protected void setValue(PrimaryDatabase primaryDatabase) {
      this.primaryDatabase = primaryDatabase;
    }
  }

  @Parent Key<EntityGroupRoot> parent = getCrossTldKey();

  @Id String id;

  /** A property that tracks the primary database for a dual-read/dual-write database migration. */
  @Mapify(TimeMapper.class)
  TimedTransitionProperty<PrimaryDatabase, PrimaryDatabaseTransition> databaseTransitions =
      TimedTransitionProperty.forMapify(PrimaryDatabase.DATASTORE, PrimaryDatabaseTransition.class);

  /** A cache that loads the {@link DatabaseTransitionSchedule} for a given id. */
  private static final LoadingCache<String, Optional<DatabaseTransitionSchedule>> CACHE =
      CacheBuilder.newBuilder()
          .expireAfterWrite(
              java.time.Duration.ofMillis(getSingletonCacheRefreshDuration().getMillis()))
          .build(
              new CacheLoader<String, Optional<DatabaseTransitionSchedule>>() {
                @Override
                public Optional<DatabaseTransitionSchedule> load(String id) throws Exception {

                  VKey<DatabaseTransitionSchedule> key =
                      VKey.create(
                          DatabaseTransitionSchedule.class,
                          id,
                          Key.create(getCrossTldKey(), DatabaseTransitionSchedule.class, id));

                  return ofyTm().transact(() -> ofyTm().loadByKeyIfPresent(key));
                }
              });

  public static DatabaseTransitionSchedule create(
      String id,
      TimedTransitionProperty<PrimaryDatabase, PrimaryDatabaseTransition> databaseTransitions) {
    checkNotNull(id, "Id cannot be null");
    checkNotNull(databaseTransitions, "databaseTransitions cannot be null");
    databaseTransitions.checkValidity();
    DatabaseTransitionSchedule instance = new DatabaseTransitionSchedule();
    instance.id = id;
    instance.databaseTransitions = databaseTransitions;
    return instance;
  }

  /** Returns the database that is indicated as primary at the given time. */
  public PrimaryDatabase getPrimaryDatabase(DateTime now) {
    return databaseTransitions.getValueAtTime(now);
  }

  public ImmutableSortedMap<DateTime, PrimaryDatabase> getDatabaseTransitions() {
    return databaseTransitions.toValueMap();
  }

  public static Optional<DatabaseTransitionSchedule> get(String id) {
    return CACHE.getUnchecked(id);
  }
}
