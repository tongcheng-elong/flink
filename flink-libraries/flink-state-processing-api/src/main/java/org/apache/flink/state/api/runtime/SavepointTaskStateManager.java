/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.api.runtime;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointMetrics;
import org.apache.flink.runtime.checkpoint.InflightDataRescalingDescriptor;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.PrioritizedOperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.SubTaskInitializationMetrics;
import org.apache.flink.runtime.checkpoint.TaskStateSnapshot;
import org.apache.flink.runtime.checkpoint.channel.SequentialChannelStateReader;
import org.apache.flink.runtime.checkpoint.filemerging.FileMergingSnapshotManager;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.LocalRecoveryConfig;
import org.apache.flink.runtime.state.TaskStateManager;
import org.apache.flink.runtime.state.changelog.ChangelogStateHandle;
import org.apache.flink.runtime.state.changelog.StateChangelogStorage;
import org.apache.flink.runtime.state.changelog.StateChangelogStorageView;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.Optional;

/**
 * A minimally implemented {@link TaskStateManager} that provides the functionality required to run
 * the {@code state-processor-api}.
 */
final class SavepointTaskStateManager implements TaskStateManager {
    private static final String MSG = "This method should never be called";

    @Nonnull private final PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskState;

    SavepointTaskStateManager(PrioritizedOperatorSubtaskState prioritizedOperatorSubtaskState) {
        Preconditions.checkNotNull(
                prioritizedOperatorSubtaskState, "Operator subtask state must not be null");
        this.prioritizedOperatorSubtaskState = prioritizedOperatorSubtaskState;
    }

    @Override
    public void reportInitializationMetrics(
            SubTaskInitializationMetrics subTaskInitializationMetrics) {}

    @Override
    public void reportTaskStateSnapshots(
            @Nonnull CheckpointMetaData checkpointMetaData,
            @Nonnull CheckpointMetrics checkpointMetrics,
            @Nullable TaskStateSnapshot acknowledgedState,
            @Nullable TaskStateSnapshot localState) {}

    @Override
    public void reportIncompleteTaskStateSnapshots(
            CheckpointMetaData checkpointMetaData, CheckpointMetrics checkpointMetrics) {}

    @Override
    public boolean isTaskDeployedAsFinished() {
        return false;
    }

    @Override
    public Optional<Long> getRestoreCheckpointId() {
        return Optional.empty();
    }

    @Nonnull
    @Override
    public PrioritizedOperatorSubtaskState prioritizedOperatorState(OperatorID operatorID) {
        return prioritizedOperatorSubtaskState;
    }

    @Override
    public Optional<OperatorSubtaskState> getSubtaskJobManagerRestoredState(OperatorID operatorID) {
        throw new UnsupportedOperationException(
                "Unsupported method for SavepointTaskStateManager.");
    }

    @Nonnull
    @Override
    public LocalRecoveryConfig createLocalRecoveryConfig() {
        return LocalRecoveryConfig.BACKUP_AND_RECOVERY_DISABLED;
    }

    @Override
    public SequentialChannelStateReader getSequentialChannelStateReader() {
        return SequentialChannelStateReader.NO_OP;
    }

    @Override
    public InflightDataRescalingDescriptor getInputRescalingDescriptor() {
        return InflightDataRescalingDescriptor.NO_RESCALE;
    }

    @Override
    public InflightDataRescalingDescriptor getOutputRescalingDescriptor() {
        return InflightDataRescalingDescriptor.NO_RESCALE;
    }

    @Nullable
    @Override
    public StateChangelogStorage<?> getStateChangelogStorage() {
        return null;
    }

    @Nullable
    @Override
    public StateChangelogStorageView<?> getStateChangelogStorageView(
            Configuration configuration, ChangelogStateHandle changelogStateHandle) {
        throw new UnsupportedOperationException("State processor api does not support changelog.");
    }

    @Nullable
    @Override
    public FileMergingSnapshotManager getFileMergingSnapshotManager() {
        return null;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        throw new UnsupportedOperationException(MSG);
    }

    @Override
    public void notifyCheckpointAborted(long checkpointId) {
        throw new UnsupportedOperationException(MSG);
    }

    @Override
    public void close() {}
}
