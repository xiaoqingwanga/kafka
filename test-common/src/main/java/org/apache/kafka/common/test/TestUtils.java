/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.common.test;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.utils.Utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;

import static java.lang.String.format;

/**
 * Helper functions for writing unit tests
 */
public class TestUtils {
    private static final Logger log = LoggerFactory.getLogger(TestUtils.class);

    private static final long DEFAULT_POLL_INTERVAL_MS = 100;
    private static final long DEFAULT_MAX_WAIT_MS = 15_000;
    private static final long DEFAULT_TIMEOUT_MS = 60_000;

    /**
     * Create an empty file in the default temporary-file directory, using `kafka` as the prefix and `tmp` as the
     * suffix to generate its name.
     */
    public static File tempFile() throws IOException {
        final File file = Files.createTempFile("kafka", ".tmp").toFile();
        file.deleteOnExit();
        return file;
    }

    /**
     * Create a temporary relative directory in the specified parent directory with the given prefix.
     *
     */
    static File tempDirectory() {
        final File file;
        String prefix = "kafka-";
        try {
            file = Files.createTempDirectory(prefix).toFile();
        } catch (final IOException ex) {
            throw new RuntimeException("Failed to create a temp dir", ex);
        }

        Exit.addShutdownHook("delete-temp-file-shutdown-hook", () -> {
            try {
                Utils.delete(file);
            } catch (IOException e) {
                log.error("Error deleting {}", file.getAbsolutePath(), e);
            }
        });

        return file;
    }

    /**
     * uses default value of 15 seconds for timeout
     */
    public static void waitForCondition(final Supplier<Boolean> testCondition, final String conditionDetails) throws InterruptedException {
        waitForCondition(testCondition, DEFAULT_MAX_WAIT_MS, conditionDetails);
    }

    /**
     * Wait for condition to be met for at most {@code maxWaitMs} and throw assertion failure otherwise.
     * This should be used instead of {@code Thread.sleep} whenever possible as it allows a longer timeout to be used
     * without unnecessarily increasing test time (as the condition is checked frequently). The longer timeout is needed to
     * avoid transient failures due to slow or overloaded machines.
     */
    public static void waitForCondition(final Supplier<Boolean> testCondition, 
                                        final long maxWaitMs, 
                                        String conditionDetails
    ) throws InterruptedException {
        retryOnExceptionWithTimeout(() -> {
            String conditionDetail = conditionDetails == null ? "" : conditionDetails;
            if (!testCondition.get())
                throw new TimeoutException("Condition not met within timeout " + maxWaitMs + ". " + conditionDetail);
        });
    }

    /**
     * Wait for the given runnable to complete successfully, i.e. throw now {@link Exception}s or
     * {@link AssertionError}s, or for the given timeout to expire. If the timeout expires then the
     * last exception or assertion failure will be thrown thus providing context for the failure.
     *
     * @param runnable the code to attempt to execute successfully.
     * @throws InterruptedException if the current thread is interrupted while waiting for {@code runnable} to complete successfully.
     */
    static void retryOnExceptionWithTimeout(final Runnable runnable) throws InterruptedException {
        final long expectedEnd = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        while (true) {
            try {
                runnable.run();
                return;
            } catch (final AssertionError t) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw t;
                }
            } catch (final Exception e) {
                if (expectedEnd <= System.currentTimeMillis()) {
                    throw new AssertionError(format("Assertion failed with an exception after %s ms", DEFAULT_TIMEOUT_MS), e);
                }
            }
            Thread.sleep(DEFAULT_POLL_INTERVAL_MS);
        }
    }

    public static int waitUntilLeaderIsElectedOrChangedWithAdmin(Admin admin,
                                                                 String topic,
                                                                 int partitionNumber,
                                                                 long timeoutMs) throws Exception {
        BiFunction<String, Integer, Optional<Integer>> getPartitionLeader = (t, p) -> {
            try {
                return Optional.ofNullable(getLeaderFromAdmin(admin, t, p));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        };
        return doWaitUntilLeaderIsElectedOrChanged(getPartitionLeader, topic, partitionNumber, timeoutMs);
    }

    private static Integer getLeaderFromAdmin(Admin admin, String topic, int partition) throws Exception {
        TopicDescription topicDescription = admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get().get(topic);
        return topicDescription.partitions().stream()
            .filter(partitionInfo -> partitionInfo.partition() == partition)
            .findFirst()
            .map(partitionInfo -> partitionInfo.leader().id() == Node.noNode().id() ? null : partitionInfo.leader().id())
            .orElse(null);
    }

    private static int doWaitUntilLeaderIsElectedOrChanged(BiFunction<String, Integer, Optional<Integer>> getPartitionLeader,
                                                           String topic,
                                                           int partition,
                                                           long timeoutMs) throws Exception {
        long startTime = System.currentTimeMillis();
        TopicPartition topicPartition = new TopicPartition(topic, partition);
        Optional<Integer> electedLeader = Optional.empty();

        while (electedLeader.isEmpty() && System.currentTimeMillis() < startTime + timeoutMs) {
            Optional<Integer> leader = getPartitionLeader.apply(topic, partition);
            if (leader.isPresent()) {
                log.trace("Leader {} is elected for partition {}", leader.get(), topicPartition);
                electedLeader = leader;
            } else {
                log.trace("Leader for partition {} is not elected yet", topicPartition);
            }
            Thread.sleep(Math.min(timeoutMs, 100L));
        }

        Optional<Integer> finalLeader = electedLeader;
        return electedLeader.orElseThrow(() -> new AssertionError("Timing out after " + timeoutMs
            + " ms since a leader was not elected for partition " + topicPartition + ", leader is " + finalLeader));
    }
}
