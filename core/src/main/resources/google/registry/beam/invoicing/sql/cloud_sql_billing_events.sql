--Copyright 2021 The Nomulus Authors. All Rights Reserved.
--
--Licensed under the Apache License, Version 2.0 (the "License");
--you may not use this file except in compliance with the License.
--You may obtain a copy of the License at
--
--    http://www.apache.org/licenses/LICENSE-2.0
--
--Unless required by applicable law or agreed to in writing, software
--distributed under the License is distributed on an "AS IS" BASIS,
--WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
--See the License for the specific language governing permissions and
--limitations under the License.

-- This query gathers all non-canceled billing events for a given
-- YEAR_MONTH in yyyy-MM format.

 SELECT b, r FROM BillingEvent b
 JOIN Registrar r ON b.clientId = r.clientIdentifier
 JOIN Domain d ON b.domainRepoId = d.repoId
 JOIN Tld t ON t.tldStrId = d.tld
 LEFT JOIN BillingCancellation c ON b.id = c.refOneTime.billingId
 LEFT JOIN BillingCancellation cr ON b.cancellationMatchingBillingEvent = cr.refRecurring.billingId
 WHERE r.billingIdentifier IS NOT NULL
 AND r.type = 'REAL'
 AND t.invoicingEnabled IS TRUE
 AND b.billingTime BETWEEN CAST('%FIRST_TIMESTAMP_OF_MONTH%' AS timestamp) AND CAST('%LAST_TIMESTAMP_OF_MONTH%' AS timestamp)
 AND c.id IS NULL
 AND cr.id IS NULL
