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

import static google.registry.model.common.EntityGroupRoot.getCrossTldKey;
import static google.registry.persistence.transaction.TransactionManagerFactory.ofyTm;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableSortedMap;
import com.googlecode.objectify.Key;
import google.registry.model.common.DatabaseTransitionSchedule;
import google.registry.model.common.DatabaseTransitionSchedule.PrimaryDatabase;
import google.registry.model.common.DatabaseTransitionSchedule.PrimaryDatabaseTransition;
import google.registry.model.common.TimedTransitionProperty;
import google.registry.persistence.VKey;
import google.registry.tools.params.TransitionListParameter.PrimaryDatabaseTransitions;
import java.util.Optional;
import org.joda.time.DateTime;

/** Command to update {@link DatabaseTransitionSchedule}. */
@Parameters(
    separators = " =",
    commandDescription = "Add a new entry to the database transition schedule.")
public class UpdateDatabaseTransitionScheduleCommand extends MutatingCommand {

  @Parameter(
      names = "--transition_schedule",
      converter = PrimaryDatabaseTransitions.class,
      validateWith = PrimaryDatabaseTransitions.class,
      description =
          "Comma-delimited list of database transitions, of the form"
              + " <time>=<primary-database>[,<time>=<primary-database>]*")
  ImmutableSortedMap<DateTime, PrimaryDatabase> transitionSchedule;

  @Parameter(names = "--schedule_id", description = "ID string for the schedule being updated")
  private String scheduleId;

  @Override
  protected void init() {
    VKey<DatabaseTransitionSchedule> key =
        VKey.create(
            DatabaseTransitionSchedule.class,
            scheduleId,
            Key.create(getCrossTldKey(), DatabaseTransitionSchedule.class, scheduleId));

    // Retrieve the existing schedule
    Optional<DatabaseTransitionSchedule> currentSchedule =
        ofyTm().transact(() -> ofyTm().loadByKeyIfPresent(key));

    DatabaseTransitionSchedule newSchedule =
        DatabaseTransitionSchedule.create(
            scheduleId,
            TimedTransitionProperty.fromValueMap(
                transitionSchedule, PrimaryDatabaseTransition.class));

    stageEntityChange(currentSchedule.orElse(null), newSchedule);
  }
}
