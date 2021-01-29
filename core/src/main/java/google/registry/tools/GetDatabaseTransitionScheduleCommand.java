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

import static google.registry.util.PreconditionsUtils.checkArgumentPresent;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import google.registry.model.common.DatabaseTransitionSchedule;
import java.util.List;

/** Command to show the {@link DatabaseTransitionSchedule} for an entity. */
@Parameters(separators = " =", commandDescription = "Show database transition schedule")
final class GetDatabaseTransitionScheduleCommand implements CommandWithRemoteApi {

  @Parameter(description = "ID string for the schedule to get", required = true)
  private List<String> mainParameters;

  @Override
  public void run() {
    for (String id : mainParameters) {
      DatabaseTransitionSchedule schedule =
          checkArgumentPresent(
              DatabaseTransitionSchedule.get(id),
              "A database transition schedule for %s does not exist",
              id);

      System.out.println(schedule.toString());
    }
  }
}
