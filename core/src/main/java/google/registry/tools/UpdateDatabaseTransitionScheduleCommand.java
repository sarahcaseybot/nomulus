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

package google.registry.tools;

import static google.registry.model.common.CrossTldSingleton.SINGLETON_ID;
import static google.registry.model.common.EntityGroupRoot.getCrossTldKey;
import static google.registry.persistence.transaction.TransactionManagerFactory.ofyTm;
import static google.registry.util.DateTimeUtils.START_OF_TIME;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Ordering;
import com.googlecode.objectify.Key;
import google.registry.model.common.DatabaseTransitionSchedule;
import google.registry.model.common.DatabaseTransitionSchedule.PrimaryDatabase;
import google.registry.persistence.VKey;
import java.util.Optional;
import org.joda.time.DateTime;

/** Command to update {@link DatabaseTransitionSchedule}. */
@Parameters(
    separators = " =",
    commandDescription = "Add a new entry to the database transition schedule.")
public class UpdateDatabaseTransitionScheduleCommand extends MutatingCommand {

  @Parameter(names = "--start_time", description = "Datetime for database to become primary.")
  DateTime start;

  @Parameter(
      names = "--database",
      description = "Database to become primary (DATASTORE or CLOUD_SQL)")
  private PrimaryDatabase primaryDatabase;

  @Override
  protected void init() throws Exception {
    // Add check arguments

    VKey<DatabaseTransitionSchedule> key =
        VKey.create(
            DatabaseTransitionSchedule.class,
            SINGLETON_ID,
            Key.create(getCrossTldKey(), DatabaseTransitionSchedule.class, SINGLETON_ID));

    Optional<DatabaseTransitionSchedule> currentSchedule =
        ofyTm().transact(() -> ofyTm().loadByKeyIfPresent(key));

    boolean scheduleWasNull = false;
    ImmutableSortedMap<DateTime, PrimaryDatabase> databaseTransitions;
    if (currentSchedule.isEmpty()) {
      scheduleWasNull = true;
      currentSchedule =
          Optional.of(
              new DatabaseTransitionSchedule.Builder()
                  .setDatabaseTransitions(
                      ImmutableSortedMap.of(START_OF_TIME, PrimaryDatabase.DATASTORE))
                  .build());
    }
    databaseTransitions = currentSchedule.get().getDatabaseTransitions();

    ImmutableSortedMap.Builder<DateTime, PrimaryDatabase> newDatabaseTransitions =
        new ImmutableSortedMap.Builder<>(Ordering.natural());
    newDatabaseTransitions.putAll(databaseTransitions);
    newDatabaseTransitions.put(start, primaryDatabase);

    DatabaseTransitionSchedule.Builder newSchedule = new DatabaseTransitionSchedule.Builder();
    newSchedule.setDatabaseTransitions(newDatabaseTransitions.build());
    stageEntityChange(scheduleWasNull ? null : currentSchedule.get(), newSchedule.build());
  }
}
