/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.nifi.registry.web.api;

import static org.apache.nifi.registry.web.api.IntegrationTestUtils.assertBucketsEqual;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.UUID;
import javax.ws.rs.client.Entity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import org.apache.nifi.registry.bucket.Bucket;
import org.apache.nifi.registry.revision.entity.RevisionInfo;
import org.junit.jupiter.api.Test;
import org.skyscreamer.jsonassert.JSONAssert;
import org.springframework.test.context.jdbc.Sql;

public class BucketsIT extends UnsecuredITBase {

    @Test
    public void testGetBucketsEmpty() {

        // Given: a fresh context server with an empty DB
        // When: the /buckets endpoint is queried

        final Bucket[] buckets = client
                .target(createURL("buckets"))
                .request()
                .get(Bucket[].class);

        // Then: an empty array is returned

        assertNotNull(buckets);
        assertEquals(0, buckets.length);
    }

    // NOTE: The tests that seed the DB directly from SQL end up with different results for the timestamp depending on
    // which DB is used, so for now these types of tests only run against H2.
    @Test
//    @EnabledIf(value = "${current.database.is.h2}", loadContext = true)
    @Sql(executionPhase = Sql.ExecutionPhase.BEFORE_TEST_METHOD, scripts = {"classpath:db/clearDB.sql", "classpath:db/BucketsIT.sql"})
    public void testGetBuckets() throws Exception {

        // Given: these buckets have been populated in the DB (see BucketsIT.sql)

        final String expected = "[" +
                "{\"identifier\":\"1\"," +
                "\"name\":\"Bucket 1\"," +
                "\"createdTimestamp\":1505134260000," +
                "\"description\":\"This is test bucket 1\"," +
                "\"permissions\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"link\":{\"params\":{\"rel\":\"self\"},\"href\":\"buckets/1\"}}," +
                "{\"identifier\":\"2\"," +
                "\"name\":\"Bucket 2\"," +
                "\"createdTimestamp\":1505134320000," +
                "\"description\":\"This is test bucket 2\"," +
                "\"permissions\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"link\":{\"params\":{\"rel\":\"self\"},\"href\":\"buckets/2\"}}," +
                "{\"identifier\":\"3\"," +
                "\"name\":\"Bucket 3\"," +
                "\"createdTimestamp\":1505134380000," +
                "\"description\":\"This is test bucket 3\"," +
                "\"permissions\":{\"canRead\":true,\"canWrite\":true,\"canDelete\":true}," +
                "\"link\":{\"params\":{\"rel\":\"self\"},\"href\":\"buckets/3\"}}" +
                "]";

        // When: the /buckets endpoint is queried

        final String bucketsJson = client
                .target(createURL("buckets"))
                .request()
                .get(String.class);

        // Then: the pre-populated list of buckets is returned

        JSONAssert.assertEquals(expected, bucketsJson, false);
        assertFalse(bucketsJson.contains("null")); // JSON serialization from the server should not include null fields, such as "versionedFlows": null
    }

    @Test
    public void testGetNonexistentBucket() {
        // Given: a fresh context server with an empty DB
        // When: any /buckets/{id} endpoint is queried
        final Response response = client.target(createURL("buckets/a-nonexistent-identifier")).request().get();

        // Then: a 404 response status is returned
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testCreateBucketGetBucket() {
        final String clientId = UUID.randomUUID().toString();
        final RevisionInfo initialRevision = new RevisionInfo(clientId, 0L);

        // Given:

        final long testStartTime = System.currentTimeMillis();
        final Bucket bucket = new Bucket();
        bucket.setName("Integration Test Bucket");
        bucket.setDescription("A bucket created by an integration test.");
        bucket.setRevision(initialRevision);

        // When: a bucket is created on the server

        final Bucket createdBucket = client
                .target(createURL("buckets"))
                .request()
                .post(Entity.entity(bucket, MediaType.APPLICATION_JSON), Bucket.class);

        // Then: the server returns the created bucket, with server-set fields populated correctly

        assertBucketsEqual(bucket, createdBucket, false);
        assertNotNull(createdBucket.getIdentifier());
        assertTrue(createdBucket.getCreatedTimestamp() - testStartTime > 0L); // both server and client in same JVM, so there shouldn't be skew
        assertNotNull(createdBucket.getLink());
        assertNotNull(createdBucket.getLink().getUri());
        assertNotNull(createdBucket.getRevision());
        assertEquals(initialRevision.getVersion() + 1, createdBucket.getRevision().getVersion().longValue());
        assertEquals(initialRevision.getClientId(), createdBucket.getRevision().getClientId());

        // And when /buckets is queried, then the newly created bucket is returned in the list

        final Bucket[] buckets = client
                .target(createURL("buckets"))
                .request()
                .get(Bucket[].class);
        assertNotNull(buckets);
        assertEquals(1, buckets.length);
        assertBucketsEqual(createdBucket, buckets[0], true);

        // And when the link URI is queried, then the newly created bucket is returned

        final Bucket bucketByLink = client
                .target(createURL(buckets[0].getLink().getUri().toString()))
                .request()
                .get(Bucket.class);
        assertBucketsEqual(createdBucket, bucketByLink, true);

        // And when the bucket is queried by /buckets/ID, then the newly created bucket is returned

        final Bucket bucketById = client
                .target(createURL("buckets/" + createdBucket.getIdentifier()))
                .request()
                .get(Bucket.class);
        assertBucketsEqual(createdBucket, bucketById, true);
        assertNotNull(bucketById.getRevision());
        assertEquals(initialRevision.getVersion() + 1, bucketById.getRevision().getVersion().longValue());
        assertEquals(initialRevision.getClientId(), bucketById.getRevision().getClientId());
    }

    @Test
    public void testUpdateBucket() {
        final String clientId = UUID.randomUUID().toString();
        final RevisionInfo initialRevision = new RevisionInfo(clientId, 0L);

        // Given: a bucket exists on the server

        final Bucket bucket = new Bucket();
        bucket.setName("Integration Test Bucket");
        bucket.setDescription("A bucket created by an integration test.");
        bucket.setRevision(initialRevision);

        final Bucket createdBucket = client
                .target(createURL("buckets"))
                .request()
                .post(Entity.entity(bucket, MediaType.APPLICATION_JSON), Bucket.class);

        // When: the bucket is modified by the client and updated on the server

        createdBucket.setName("Renamed Bucket");
        createdBucket.setDescription("This bucket has been updated by an integration test.");

        final Bucket updatedBucket = client
                .target(createURL("buckets/" + createdBucket.getIdentifier()))
                .request()
                .put(Entity.entity(createdBucket, MediaType.APPLICATION_JSON), Bucket.class);

        // Then: the server returns the updated bucket

        assertBucketsEqual(createdBucket, updatedBucket, true);
    }

    @Test
    public void testUpdateBucketWithIncorrectRevision() {
        final String clientId = UUID.randomUUID().toString();
        final RevisionInfo initialRevision = new RevisionInfo(clientId, 0L);

        // Given: a bucket exists on the server

        final Bucket bucket = new Bucket();
        bucket.setName("Integration Test Bucket");
        bucket.setDescription("A bucket created by an integration test.");
        bucket.setRevision(initialRevision);

        Bucket createdBucket = client
                .target(createURL("buckets"))
                .request()
                .post(Entity.entity(bucket, MediaType.APPLICATION_JSON), Bucket.class);

        // When: the bucket is modified by the client and updated on the server

        createdBucket.setName("Renamed Bucket");
        createdBucket.setDescription("This bucket has been updated by an integration test.");

        // Change version to incorrect number and don't send a client id
        createdBucket.getRevision().setClientId(null);
        createdBucket.getRevision().setVersion(99L);

        try (final Response response = client
                .target(createURL("buckets/" + createdBucket.getIdentifier()))
                .request()
                .put(Entity.entity(createdBucket, MediaType.APPLICATION_JSON))) {

            // Then: we get a bad request for sending a wrong revision

            assertEquals(400, response.getStatus());
        }
    }

    @Test
    public void testDeleteBucket() {
        final String clientId = UUID.randomUUID().toString();
        final RevisionInfo initialRevision = new RevisionInfo(clientId, 0L);

        // Given: a bucket has been created

        final Bucket bucket = new Bucket();
        bucket.setName("Integration Test Bucket");
        bucket.setDescription("A bucket created by an integration test.");
        bucket.setRevision(initialRevision);

        final Bucket createdBucket = client
                .target(createURL("buckets"))
                .request()
                .post(Entity.entity(bucket, MediaType.APPLICATION_JSON), Bucket.class);

        // When: that bucket deleted

        final Bucket deletedBucket = client
                .target(createURL("buckets/" + createdBucket.getIdentifier()))
                .queryParam("version", createdBucket.getRevision().getVersion())
                .request()
                .delete(Bucket.class);

        // Then: the body of the server response matches the bucket that was deleted
        //  and: the bucket is no longer accessible (resource not found)

        createdBucket.setPermissions(null); // authorizedActions will not be present in deletedBucket
        createdBucket.setLink(null); // links will not be present in deletedBucket
        assertBucketsEqual(createdBucket, deletedBucket, true);

        final Response response = client
                .target(createURL("buckets/" + createdBucket.getIdentifier()))
                .request()
                .get();
        assertEquals(404, response.getStatus());
    }

    @Test
    public void testDeleteBucketWithIncorrectRevision() {
        final String clientId = UUID.randomUUID().toString();
        final RevisionInfo initialRevision = new RevisionInfo(clientId, 0L);

        // Given: a bucket has been created

        final Bucket bucket = new Bucket();
        bucket.setName("Integration Test Bucket");
        bucket.setDescription("A bucket created by an integration test.");
        bucket.setRevision(initialRevision);

        Bucket createdBucket = client
                .target(createURL("buckets"))
                .request()
                .post(Entity.entity(bucket, MediaType.APPLICATION_JSON), Bucket.class);

        // When: that bucket deleted

        try (final Response response = client
                .target(createURL("buckets/" + createdBucket.getIdentifier()))
                .queryParam("version", 99L)
                .request()
                .delete()) {

            // Then: we get a bad request for sending the wrong revision version

            assertEquals(400, response.getStatus());
        }
    }

    @Test
    public void getBucketFields() throws Exception {

        // Given: the server is configured to return this fixed response

        final String expected = "{\"fields\":[\"ID\",\"NAME\",\"DESCRIPTION\",\"CREATED\"]}";

        // When: the server is queried

        final String bucketFieldsJson = client
                .target(createURL("buckets/fields"))
                .request()
                .get(String.class);

        // Then: the fixed response is returned to the client

        JSONAssert.assertEquals(expected, bucketFieldsJson, false);

    }

}
