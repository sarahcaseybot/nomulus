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
import static google.registry.model.common.CrossTldSingleton.SINGLETON_ID;
import static google.registry.model.common.EntityGroupRoot.getCrossTldKey;
import static google.registry.model.ofy.ObjectifyService.ofy;
import static google.registry.testing.DatabaseHelper.persistResource;
import static google.registry.util.DateTimeUtils.START_OF_TIME;

import com.google.common.collect.ImmutableSortedMap;
import com.googlecode.objectify.Key;
import google.registry.model.common.DatabaseTransitionSchedule;
import google.registry.model.common.DatabaseTransitionSchedule.PrimaryDatabase;
import org.joda.time.DateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link UpdateDatabaseTransitionScheduleCommand}. */
public class UpdateDatabaseTransitionScheduleCommandTest
    extends CommandTestCase<UpdateDatabaseTransitionScheduleCommand> {

  Key<DatabaseTransitionSchedule> key;

  @BeforeEach
  void setup() {
    key = Key.create(getCrossTldKey(), DatabaseTransitionSchedule.class, SINGLETON_ID);
    fakeClock.setTo(DateTime.parse("2020-12-01T00:00:00Z"));
  }

  @Test
  void testSuccess_currentScheduleIsEmpty() throws Exception {
    assertThat(ofy().load().key(key).now()).isNull();
    runCommandForced("--start_time=2020-12-02T00:00:00Z", "--database=CLOUD_SQL");
    assertThat(ofy().load().key(key).now().getPrimaryDatabase(fakeClock.nowUtc()))
        .isEqualTo(PrimaryDatabase.DATASTORE);
    assertThat(ofy().load().key(key).now().getPrimaryDatabase(fakeClock.nowUtc().plusDays(5)))
        .isEqualTo(PrimaryDatabase.CLOUD_SQL);
    String changes = command.prompt();
    assertThat(changes).contains("Create DatabaseTransitionSchedule");
  }

  @Test
  void testSuccess() throws Exception {
    DatabaseTransitionSchedule.Builder builder = new DatabaseTransitionSchedule.Builder();
    builder.setDatabaseTransitions(
        ImmutableSortedMap.of(
            START_OF_TIME,
            PrimaryDatabase.DATASTORE,
            fakeClock.nowUtc().minusDays(1),
            PrimaryDatabase.CLOUD_SQL));
    persistResource(builder.build());
    assertThat(ofy().load().key(key).now().getDatabaseTransitions()).hasSize(2);
    runCommandForced("--start_time=2020-12-02T00:00:00Z", "--database=DATASTORE");
    assertThat(ofy().load().key(key).now().getDatabaseTransitions()).hasSize(3);
    assertThat(ofy().load().key(key).now().getPrimaryDatabase(fakeClock.nowUtc().plusDays(5)))
        .isEqualTo(PrimaryDatabase.DATASTORE);
    String changes = command.prompt();
    assertThat(changes).contains("Update DatabaseTransitionSchedule");
  }
}
