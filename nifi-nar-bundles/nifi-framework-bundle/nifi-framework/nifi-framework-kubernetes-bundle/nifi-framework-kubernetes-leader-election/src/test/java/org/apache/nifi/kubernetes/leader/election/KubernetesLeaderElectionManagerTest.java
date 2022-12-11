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
package org.apache.nifi.kubernetes.leader.election;

import org.apache.nifi.controller.leader.election.LeaderElectionRole;
import org.apache.nifi.controller.leader.election.LeaderElectionStateChangeListener;
import org.apache.nifi.kubernetes.leader.election.command.LeaderElectionCommandProvider;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class KubernetesLeaderElectionManagerTest {

    private static final LeaderElectionRole LEADER_ELECTION_ROLE = LeaderElectionRole.CLUSTER_COORDINATOR;

    private static final String ROLE = LEADER_ELECTION_ROLE.getRoleName();

    private static final String PARTICIPANT_ID = "Node-0";

    @Mock
    LeaderElectionStateChangeListener changeListener;

    @Mock
    ExecutorService executorService;

    @Mock
    Future<?> future;

    @Captor
    ArgumentCaptor<Runnable> commandCaptor;

    ManagedLeaderElectionCommandProvider leaderElectionCommandFactory;

    KubernetesLeaderElectionManager manager;

    @BeforeEach
    void setManager() {
        leaderElectionCommandFactory = new ManagedLeaderElectionCommandProvider();
        manager = new MockKubernetesLeaderElectionManager();
    }

    @Test
    void testStartStartStop() {
        manager.start();
        manager.start();
        manager.stop();

        assertTrue(leaderElectionCommandFactory.closed);
    }

    @Test
    void testStartIsLeaderFalseStop() {
        manager.start();

        final boolean leader = manager.isLeader(ROLE);
        assertFalse(leader);

        manager.stop();

        assertTrue(leaderElectionCommandFactory.closed);
    }

    @Test
    void testStartRegisterParticipatingStartLeading() {
        manager.start();

        setSubmitStartLeading();

        manager.register(ROLE, changeListener, PARTICIPANT_ID);

        captureRunCommand();
        assertActiveParticipantLeader();
    }

    @Test
    void testRegisterParticipatingStartLeading() {
        manager.register(ROLE, changeListener, PARTICIPANT_ID);

        setSubmitStartLeading();

        manager.start();

        captureRunCommand();
        assertActiveParticipantLeader();
    }

    @Test
    void testRegisterParticipatingStartLeadingUnregister() {
        manager.register(ROLE, changeListener, PARTICIPANT_ID);

        setSubmitStartLeading();

        manager.start();

        captureRunCommand();
        assertActiveParticipantLeader();

        manager.unregister(ROLE);

        assertNotActiveParticipantNotLeader();
    }

    @Test
    void testIsLeaderNotRegistered() {
        final boolean leader = manager.isLeader(ROLE);

        assertFalse(leader);
    }

    @Test
    void testRegisterParticipatingIsActiveParticipantTrue() {
        manager.register(ROLE, changeListener, PARTICIPANT_ID);

        final boolean activeParticipant = manager.isActiveParticipant(ROLE);
        assertTrue(activeParticipant);
    }

    @Test
    void testRegisterParticipatingIsActiveParticipantTrueUnregister() {
        manager.register(ROLE, changeListener, PARTICIPANT_ID);

        final boolean registeredActiveParticipant = manager.isActiveParticipant(ROLE);
        assertTrue(registeredActiveParticipant);

        manager.unregister(ROLE);

        final boolean unregisteredActiveParticipant = manager.isActiveParticipant(ROLE);
        assertFalse(unregisteredActiveParticipant);
    }

    @Test
    void testRegisterNotParticipatingIsActiveParticipantFalse() {
        manager.register(ROLE, changeListener);

        final boolean activeParticipant = manager.isActiveParticipant(ROLE);
        assertFalse(activeParticipant);
    }

    @Test
    void testUnregisterRoleNameRequired() {
        assertThrows(IllegalArgumentException.class, () -> manager.unregister(null));
    }

    @Test
    void testUnregisterNotRegistered() {
        manager.unregister(ROLE);

        final boolean unregisteredActiveParticipant = manager.isActiveParticipant(ROLE);
        assertFalse(unregisteredActiveParticipant);
    }

    private void setSubmitStartLeading() {
        doReturn(future).when(executorService).submit(isA(Runnable.class));
        leaderElectionCommandFactory.runStartLeading = true;
        leaderElectionCommandFactory.runNewLeader = true;
        leaderElectionCommandFactory.runStopLeading = true;
    }

    private void captureRunCommand() {
        verify(executorService).submit(commandCaptor.capture());
        commandCaptor.getValue().run();
    }

    private void assertActiveParticipantLeader() {
        final boolean activeParticipant = manager.isActiveParticipant(ROLE);
        assertTrue(activeParticipant);

        final boolean leader = manager.isLeader(ROLE);
        assertTrue(leader);

        final String leaderId = manager.getLeader(ROLE);
        assertEquals(PARTICIPANT_ID, leaderId);

        assertEquals(LEADER_ELECTION_ROLE.getRoleId(), leaderElectionCommandFactory.name);
    }

    private void assertNotActiveParticipantNotLeader() {
        final boolean activeParticipant = manager.isActiveParticipant(ROLE);
        assertFalse(activeParticipant);

        final boolean leader = manager.isLeader(ROLE);
        assertFalse(leader);
    }

    private class MockKubernetesLeaderElectionManager extends KubernetesLeaderElectionManager {
        @Override
        protected ExecutorService createExecutorService() {
            return executorService;
        }

        @Override
        protected LeaderElectionCommandProvider createLeaderElectionCommandProvider() {
            return leaderElectionCommandFactory;
        }
    }

    private static class ManagedLeaderElectionCommandProvider implements LeaderElectionCommandProvider {

        private String name;

        private boolean runStartLeading;

        private boolean runStopLeading;

        private boolean runNewLeader;

        private boolean closed;

        @Override
        public Runnable getCommand(
                final String name,
                final String identity,
                final Runnable onStartLeading,
                final Runnable onStopLeading,
                final Consumer<String> onNewLeader
        ) {
            this.name = name;
            return () -> {
                if (runStartLeading) {
                    onStartLeading.run();
                }
                if (runNewLeader) {
                    onNewLeader.accept(identity);
                }
                if (runStopLeading) {
                    onStopLeading.run();
                }
            };
        }

        @Override
        public String findLeader(final String name) {
            return null;
        }

        @Override
        public void close() {
            closed = true;
        }
    }
}
