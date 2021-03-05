// Copyright 2017 The Nomulus Authors. All Rights Reserved.
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

package google.registry.export;

import static com.google.common.truth.Truth.assertThat;
import static google.registry.testing.DatabaseHelper.createTld;
import static google.registry.testing.DatabaseHelper.persistReservedList;
import static google.registry.testing.DatabaseHelper.persistResource;

import com.google.common.collect.ImmutableList;
import google.registry.model.registry.Registry;
import google.registry.model.registry.label.ReservedList;
import google.registry.testing.AppEngineExtension;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/** Unit tests for {@link ExportUtils}. */
class ExportUtilsTest {

  @RegisterExtension
  public final AppEngineExtension appEngine =
      AppEngineExtension.builder().withDatastoreAndCloudSql().build();

  @Test
  void test_exportReservedTerms() {
    ReservedList rl1 =
        persistReservedList(
            new ReservedList.Builder()
                .setName("tld-reserved1")
                .setReservedListMapFromLines(
                    ImmutableList.of("lol,FULLY_BLOCKED", "cat,FULLY_BLOCKED"))
                .setShouldPublish(true)
                .setLastUpdateTime(DateTime.now(DateTimeZone.UTC))
                .build());
    ReservedList rl2 =
        persistReservedList(
            new ReservedList.Builder()
                .setName("tld-reserved2")
                .setReservedListMapFromLines(
                    ImmutableList.of("lol,NAME_COLLISION", "snow,FULLY_BLOCKED"))
                .setShouldPublish(true)
                .setLastUpdateTime(DateTime.now(DateTimeZone.UTC))
                .build());
    ReservedList rl3 =
        new ReservedList.Builder()
            .setName("tld-reserved3")
            .setReservedListMapFromLines(ImmutableList.of("tine,FULLY_BLOCKED"))
            .setShouldPublish(false)
            .setLastUpdateTime(DateTime.now(DateTimeZone.UTC))
            .build();
    persistReservedList(rl3);
    // persistResource(rl3);
    // ReservedListSqlDao.save(rl3);
    createTld("tld");
    persistResource(Registry.get("tld").asBuilder().setReservedLists(rl1, rl2, rl3).build());
    // Should not contain jimmy, tine, or oval.
    assertThat(new ExportUtils("# This is a disclaimer.").exportReservedTerms(Registry.get("tld")))
        .isEqualTo("# This is a disclaimer.\ncat\nlol\nsnow\n");
  }
}
