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

import com.google.common.collect.ImmutableSortedMap;
import com.googlecode.objectify.annotation.Embed;
import com.googlecode.objectify.annotation.Entity;
import com.googlecode.objectify.annotation.Mapify;
import google.registry.model.Buildable;
import google.registry.model.common.TimedTransitionProperty.TimeMapper;
import google.registry.model.common.TimedTransitionProperty.TimedTransition;
import google.registry.schema.replay.DatastoreOnlyEntity;
import javax.annotation.concurrent.Immutable;
import org.joda.time.DateTime;

@Entity
@Immutable
public class DatabaseTransitionSchedule extends CrossTldSingleton implements DatastoreOnlyEntity {

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

  /** A property that tracks the primary database for a dual-read/dual-write database migration. */
  @Mapify(TimeMapper.class)
  TimedTransitionProperty<PrimaryDatabase, PrimaryDatabaseTransition> databaseTransitions =
      TimedTransitionProperty.forMapify(PrimaryDatabase.DATASTORE, PrimaryDatabaseTransition.class);

  /** Returns the database that is indicated as primary at the given time. */
  public PrimaryDatabase getPrimaryDatabase(DateTime now) {
    return databaseTransitions.getValueAtTime(now);
  }

  public ImmutableSortedMap<DateTime, PrimaryDatabase> getDatabaseTransitions() {
    return databaseTransitions.toValueMap();
  }

  public static class Builder extends Buildable.Builder<DatabaseTransitionSchedule> {
    public Builder() {}

    public Builder setDatabaseTransitions(
        ImmutableSortedMap<DateTime, PrimaryDatabase> databaseTransitionsMap) {
      checkNotNull(databaseTransitionsMap, "Database Transitions map cannot be null");

      getInstance().databaseTransitions =
          TimedTransitionProperty.fromValueMap(
              databaseTransitionsMap, PrimaryDatabaseTransition.class);

      return this;
    }

    @Override
    public DatabaseTransitionSchedule build() {
      final DatabaseTransitionSchedule instance = getInstance();

      // Check to ensure that there is a value for START_OF_TIME.
      instance.databaseTransitions.checkValidity();

      return super.build();
    }
  }
}
