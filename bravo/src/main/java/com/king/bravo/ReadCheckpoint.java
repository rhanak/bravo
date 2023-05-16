package com.king.bravo;

import com.king.bravo.reader.KeyedStateReader;
import com.king.bravo.reader.OperatorStateReader;
import com.king.bravo.utils.StateMetadataUtils;
import java.io.IOException;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;

public class ReadCheckpoint {
    public static void main(String[] args) throws Exception {
        // First we start by taking a savepoint/checkpoint of our running job...
        // Now it's time to load the metadata
        String savepointPath = args[0];
        CheckpointMetadata savepoint = StateMetadataUtils.loadSavepoint(savepointPath);

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // We create a KeyedStateReader for accessing the state of the operator with the UID "CountPerKey"
        OperatorStateReader reader = new OperatorStateReader(env, savepoint, "CountPerKey");

        // The reader now has access to all keyed states of the "CountPerKey" operator
        // We are going to read one specific value state named "Count"
        // The DataSet contains the key-value tuples from our state
        DataSet<Tuple2<Integer, Integer>> countState =
            reader.readKeyedStates(KeyedStateReader.forValueStateKVPairs("Count", new TypeHint<>() {
            }));

    }
}
