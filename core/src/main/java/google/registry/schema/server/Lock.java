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

import static com.google.common.flogger.util.Checks.checkNotNull;

import google.registry.model.ImmutableObject;
import google.registry.schema.server.Lock.LockId;
import google.registry.util.DateTimeUtils;
import java.io.Serializable;
import java.time.ZonedDateTime;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.IdClass;
import javax.persistence.Table;
import org.joda.time.DateTime;
import org.joda.time.Duration;

/**
 * A lock on some shared resource.
 *
 * <p>Locks are either specific to a tld or global to the entire system, in which case a tld of
 * {@link GLOBAL} is used.
 *
 * <p>This uses a compound primary key as defined in {@link LockId}.
 */
@Entity
@Table
@IdClass(LockId.class)
public class Lock {

  @Column(nullable = false)
  @Id
  String resourceName;

  @Column(nullable = false)
  @Id
  String tld;

  @Column(nullable = false)
  String requestLogId;

  @Column(nullable = false)
  ZonedDateTime acquiredTime;

  @Column(nullable = false)
  ZonedDateTime expirationTime;

  /** The scope of a lock that is not specific to a single tld. */
  public static final String GLOBAL = "GLOBAL";

  private Lock(
      String resourceName,
      String tld,
      String requestLogId,
      DateTime acquiredTime,
      DateTime expirationTime) {
    this.resourceName = resourceName;
    this.tld = tld;
    this.requestLogId = requestLogId;
    this.acquiredTime = DateTimeUtils.toZonedDateTime(acquiredTime);
    this.expirationTime = DateTimeUtils.toZonedDateTime(expirationTime);
  }

  // Hibernate requires a default constructor.
  private Lock() {}

  /** Constructs a {@link Lock} object. */
  public static Lock create(
      String resourceName,
      String tld,
      String requestLogId,
      DateTime acquiredTime,
      Duration leaseLength) {
    checkNotNull(
        tld, "The tld cannot be null. To create a global lock, use the createGlobal method");
    DateTime expireTime = acquiredTime.plus(leaseLength);
    return new Lock(resourceName, tld, requestLogId, acquiredTime, expireTime);
  }

  /** Constructs a {@link Lock} object with a {@link GLOBAL} scope. */
  public static Lock createGlobal(
      String resourceName, String requestLogId, DateTime acquiredTime, Duration leaseLength) {
    DateTime expireTime = acquiredTime.plus(leaseLength);
    return new Lock(resourceName, GLOBAL, requestLogId, acquiredTime, expireTime);
  }

  static class LockId extends ImmutableObject implements Serializable {

    public String resourceName;

    public String tld;

    private LockId() {}

    public LockId(String resourceName, String tld) {
      this.resourceName = resourceName;
      this.tld = tld;
    }
  }
}
