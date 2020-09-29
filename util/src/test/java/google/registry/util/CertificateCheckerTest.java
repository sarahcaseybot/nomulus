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

package google.registry.util;

import static com.google.common.truth.Truth.assertThat;
import static org.joda.time.DateTimeZone.UTC;

import com.google.common.collect.ImmutableSet;
import java.security.KeyPairGenerator;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.joda.time.DateTime;
import org.junit.jupiter.api.Test;

/** Unit tests for {@link CertificateChecker} */
public class CertificateCheckerTest {

  private static final String SSL_HOST = "www.example.tld";

  private static CertificateChecker certificateChecker = new CertificateChecker(825, 30, 2048);

  @Test
  void test_compliantCertificate() throws Exception {
    X509Certificate certificate =
        SelfSignedCaCertificate.create(
                SSL_HOST,
                DateTime.now(UTC).minusDays(5).toDate(),
                DateTime.now(UTC).plusDays(80).toDate())
            .cert();
    assertThat(certificateChecker.checkCertificate(certificate, DateTime.now(UTC).toDate()))
        .isEqualTo(ImmutableSet.of());
  }

  @Test
  void test_certificateWithSeveralIssues() throws Exception {
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA", new BouncyCastleProvider());
    keyGen.initialize(1024, new SecureRandom());

    X509Certificate certificate =
        SelfSignedCaCertificate.create(
                keyGen.generateKeyPair(),
                SSL_HOST,
                DateTime.now(UTC).plusDays(5).toDate(),
                DateTime.now(UTC).plusDays(1000).toDate())
            .cert();

    ImmutableSet<CertificateViolation> violations =
        certificateChecker.checkCertificate(certificate, DateTime.now(UTC).toDate());
    assertThat(violations).hasSize(3);
    assertThat(violations)
        .isEqualTo(
            ImmutableSet.of(
                CertificateViolation.create(
                    "Not Yet Valid", "This certificate's start date has not yet passed."),
                CertificateViolation.create(
                    "Validity Period Too Long",
                    "The certificate must have a validity length of less than 825 days."),
                CertificateViolation.create(
                    "RSA Key Length Too Long",
                    String.format("The minimum RSA key length is %d.", 2048))));
  }

  @Test
  void test_expiredCertificate() throws Exception {
    X509Certificate certificate =
        SelfSignedCaCertificate.create(
                SSL_HOST,
                DateTime.now(UTC).minusDays(50).toDate(),
                DateTime.now(UTC).minusDays(10).toDate())
            .cert();
    ImmutableSet<CertificateViolation> violations =
        certificateChecker.checkCertificate(certificate, DateTime.now(UTC).toDate());
    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().name()).isEqualTo("Expired Certificate");
  }

  @Test
  void test_notYetValid() throws Exception {
    X509Certificate certificate =
        SelfSignedCaCertificate.create(
                SSL_HOST,
                DateTime.now(UTC).plusDays(10).toDate(),
                DateTime.now(UTC).plusDays(50).toDate())
            .cert();
    ImmutableSet<CertificateViolation> violations =
        certificateChecker.checkCertificate(certificate, DateTime.now(UTC).toDate());
    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().name()).isEqualTo("Not Yet Valid");
  }

  @Test
  void test_checkValidityLength() throws Exception {
    X509Certificate certificate =
        SelfSignedCaCertificate.create(
                SSL_HOST,
                DateTime.now(UTC).minusDays(10).toDate(),
                DateTime.now(UTC).plusDays(1000).toDate())
            .cert();
    ImmutableSet<CertificateViolation> violations =
        certificateChecker.checkCertificate(certificate, DateTime.now(UTC).toDate());
    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().name()).isEqualTo("Validity Period Too Long");
    assertThat(violations.iterator().next().displayMessage())
        .isEqualTo("The certificate must have a validity length of less than 825 days.");
  }

  @Test
  void test_nearingExpiration() throws Exception {
    X509Certificate certificate =
        SelfSignedCaCertificate.create(
                SSL_HOST,
                DateTime.now(UTC).minusDays(50).toDate(),
                DateTime.now(UTC).plusDays(10).toDate())
            .cert();
    assertThat(certificateChecker.isNearingExpiration(certificate, DateTime.now(UTC).toDate()))
        .isTrue();

    certificate =
        SelfSignedCaCertificate.create(
                SSL_HOST,
                DateTime.now(UTC).minusDays(50).toDate(),
                DateTime.now(UTC).plusDays(100).toDate())
            .cert();
    assertThat(certificateChecker.isNearingExpiration(certificate, DateTime.now(UTC).toDate()))
        .isFalse();
  }

  @Test
  void test_checkRsaKeyLength() throws Exception {
    // Key length too low
    KeyPairGenerator keyGen = KeyPairGenerator.getInstance("RSA", new BouncyCastleProvider());
    keyGen.initialize(1024, new SecureRandom());

    X509Certificate certificate =
        SelfSignedCaCertificate.create(
                keyGen.generateKeyPair(),
                SSL_HOST,
                DateTime.now(UTC).minusDays(5).toDate(),
                DateTime.now(UTC).plusDays(100).toDate())
            .cert();

    ImmutableSet<CertificateViolation> violations =
        certificateChecker.checkCertificate(certificate, DateTime.now(UTC).toDate());
    assertThat(violations).hasSize(1);
    assertThat(violations.iterator().next().name()).isEqualTo("RSA Key Length Too Long");

    // Key length higher than required
    keyGen = KeyPairGenerator.getInstance("RSA", new BouncyCastleProvider());
    keyGen.initialize(4096, new SecureRandom());

    certificate =
        SelfSignedCaCertificate.create(
                keyGen.generateKeyPair(),
                SSL_HOST,
                DateTime.now(UTC).minusDays(5).toDate(),
                DateTime.now(UTC).plusDays(100).toDate())
            .cert();

    assertThat(certificateChecker.checkCertificate(certificate, DateTime.now(UTC).toDate()))
        .isEqualTo(ImmutableSet.of());
  }
}
