package com.tonyzampogna.streams.streams;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.beans.factory.annotation.Autowired;

import static com.tonyzampogna.streams.config.KafkaConfig.REQUEST_STATE_STORE_NAME;


/**
 * This class is added during Kafka Streams StateStoreProcessor API setup.
 *
 * The benefit of using the class is the {@link org.apache.kafka.streams.processor.ProcessorContext}
 * gives us a read/write state store, instead of just a read-only one.
 */
@Slf4j
public class StateStoreProcessor implements Processor<String, String> {

    @Autowired
    private StateStore stateStore;


    public void init(ProcessorContext context) {
        log.info("Intializing the processor context.");
        KeyValueStore<String, String> stateStore
                = (KeyValueStore<String, String>) context.getStateStore(REQUEST_STATE_STORE_NAME);
        this.stateStore.setStateStore(stateStore);
    }

    public void process(String key, String message) {
        // Do nothing
    }

    public void close() {
        // Do nothing
    }

}
