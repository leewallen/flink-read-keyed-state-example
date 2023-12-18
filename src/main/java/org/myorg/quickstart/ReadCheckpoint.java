/**
 * Something profound.
 */
package org.myorg.quickstart;


import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.runtime.checkpoint.StateObjectCollection;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.state.api.OperatorIdentifier;
import org.apache.flink.state.api.SavepointReader;

import org.apache.flink.state.api.runtime.SavepointLoader;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.nio.file.Paths;

/**
 * Starting point for a Flink streaming job using the DataStream API.
 */
public final class ReadCheckpoint {

    private static final Logger log = LoggerFactory.getLogger(ReadCheckpoint.class);
    public static void main(final String[] args) throws Exception {
        final var job = new ReadCheckpoint();
        job.run();
    }

    public void run() throws Exception {
        URL resource = ReadCheckpoint.class.getResource("/chk-6/_metadata");
        final String path = Paths.get(resource.toURI()).toFile().getPath();
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        final EmbeddedRocksDBStateBackend stateBackend = new EmbeddedRocksDBStateBackend();
        log.info("path = " + path);

        CheckpointMetadata metadata = SavepointLoader.loadSavepointMetadata(path);

        metadata.getOperatorStates().stream().forEach(s -> {
            s.getSubtaskStates().forEach((key, value) -> {
                if (value != null && value.hasState()) {
                    StateObjectCollection<KeyedStateHandle> rawKeyedState = value.getManagedKeyedState();
                    if (rawKeyedState.hasState()) {
                        log.info("ManagedKeyedState:");
                        rawKeyedState.forEach(rks ->
                                log.info("StateHandleId={}, startKeyGroup={}, endKeyGroup={}, num key groups={}",
                                        rks.getStateHandleId(),
                                        rks.getKeyGroupRange().getStartKeyGroup(),
                                        rks.getKeyGroupRange().getEndKeyGroup(),
                                        rks.getKeyGroupRange().getNumberOfKeyGroups()));
                    }
                }
            });
        });

        var checkpointProperties = metadata.getCheckpointProperties();
        log.info("checkpointId = {}, snapshotType = {}, isSavepoint = {}, isSynchronous = {}, isUnclaimed = {}",
                metadata.getCheckpointId(),
                checkpointProperties.getCheckpointType().getName(),
                checkpointProperties.isSavepoint(),
                checkpointProperties.isSynchronous(),
                checkpointProperties.isUnclaimed());

        final SavepointReader savepoint = SavepointReader
                .read(env, "file://" + path, stateBackend);

        final DataStream<CountWithTimestamp> results = savepoint
                .readKeyedState(OperatorIdentifier.forUid("Upper"), new KeyedStateReader());

        var stateResults = results
                .executeAndCollect("Kafka Experiment State Reader");
        stateResults.forEachRemaining(r -> log.info("State: {}", r));

    }
}
