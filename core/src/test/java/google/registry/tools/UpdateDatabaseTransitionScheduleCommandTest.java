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

import static com.google.common.truth.Truth.assertThat;
import static google.registry.model.common.EntityGroupRoot.getCrossTldKey;
import static google.registry.model.ofy.ObjectifyService.ofy;
import static google.registry.testing.DatabaseHelper.persistResource;
import static google.registry.util.DateTimeUtils.START_OF_TIME;

import com.google.common.collect.ImmutableSortedMap;
import com.googlecode.objectify.Key;
import google.registry.model.common.DatabaseTransitionSchedule;
import google.registry.model.common.DatabaseTransitionSchedule.PrimaryDatabase;
import google.registry.model.common.DatabaseTransitionSchedule.PrimaryDatabaseTransition;
import google.registry.model.common.TimedTransitionProperty;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link UpdateDatabaseTransitionScheduleCommand}. */
public class UpdateDatabaseTransitionScheduleCommandTest
    extends CommandTestCase<UpdateDatabaseTransitionScheduleCommand> {

  Key<DatabaseTransitionSchedule> key;

  @BeforeEach
  void setup() {
    key = Key.create(getCrossTldKey(), DatabaseTransitionSchedule.class, "test");
    fakeClock.setTo(DateTime.parse("2020-12-01T00:00:00Z"));
  }

  @Test
  void testSuccess_currentScheduleIsEmpty() throws Exception {
    assertThat(ofy().load().key(key).now()).isNull();
    runCommandForced(
        "--schedule_id=test", String.format("--transition_schedule=%s=DATASTORE", START_OF_TIME));
    assertThat(DatabaseTransitionSchedule.get("test").getPrimaryDatabase(fakeClock.nowUtc()))
        .isEqualTo(PrimaryDatabase.DATASTORE);
    String changes = command.prompt();
    assertThat(changes).contains("Create DatabaseTransitionSchedule");
  }

  @Test
  void testSuccess() throws Exception {
    DatabaseTransitionSchedule schedule =
        DatabaseTransitionSchedule.create(
            "test",
            TimedTransitionProperty.fromValueMap(
                ImmutableSortedMap.of(
                    START_OF_TIME,
                    PrimaryDatabase.DATASTORE,
                    fakeClock.nowUtc().minusDays(1),
                    PrimaryDatabase.CLOUD_SQL),
                PrimaryDatabaseTransition.class));
    persistResource(schedule);
    assertThat(DatabaseTransitionSchedule.get("test").getDatabaseTransitions()).hasSize(2);
    runCommandForced(
        "--schedule_id=test",
        String.format(
            "--transition_schedule=%s=DATASTORE,%s=CLOUD_SQL,%s=DATASTORE",
            START_OF_TIME, fakeClock.nowUtc().minusDays(1), fakeClock.nowUtc().plusDays(5)));
    assertThat(DatabaseTransitionSchedule.get("test").getDatabaseTransitions()).hasSize(3);
    assertThat(
            DatabaseTransitionSchedule.get("test")
                .getPrimaryDatabase(fakeClock.nowUtc().plusDays(5)))
        .isEqualTo(PrimaryDatabase.DATASTORE);
    String changes = command.prompt();
    assertThat(changes).contains("Update DatabaseTransitionSchedule");
  }
}
