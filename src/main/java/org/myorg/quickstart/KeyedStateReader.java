package org.myorg.quickstart;


import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.state.api.functions.KeyedStateReaderFunction;
import org.apache.flink.util.Collector;

public class KeyedStateReader extends KeyedStateReaderFunction<String, CountWithTimestamp> {
    public static final String ID = "nullEmptyCounts";
    ValueState<CountWithTimestamp> state;

    @Override
    public void open(Configuration parameters) {
        ValueStateDescriptor<CountWithTimestamp> stateDescriptor = new ValueStateDescriptor<>(ID,
                TypeInformation.of(CountWithTimestamp.class));
        state = getRuntimeContext().getState(stateDescriptor);
    }

    @Override
    public void readKey(String key, Context ctx, Collector<CountWithTimestamp> out) throws Exception {
        CountWithTimestamp data = state.value();
        out.collect(data);
    }
}