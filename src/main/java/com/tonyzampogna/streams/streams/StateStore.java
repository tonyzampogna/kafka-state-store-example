package com.tonyzampogna.streams.streams;

import com.tonyzampogna.streams.model.RequestState;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.util.StringUtils;


@Slf4j
public class StateStore {

    private KeyValueStore<String, String> stateStore;


    /**
     * Get the value of the request in the state store for the given key.
     *
     * @param key The key of the value.
     * @return The value in the state store.
     */
    public RequestState getState(String key) {
        log.info("Get value in StateStore for key: {}", key);
        stateStore.all().forEachRemaining(keyValue -> log.debug("Keys in the group state store: {}: {}", keyValue.key, keyValue.value));
        String value = stateStore.get(key);
        if (StringUtils.isEmpty(value)) {
            return null;
        }
        return RequestState.deserialize(value);
    }

    /**
     * Set a value for a request in the state store.
     *
     * @param key The key to set the value with.
     * @param requestState The value to put in the state store.
     */
    public void setState(String key, RequestState requestState) {
        log.info("Setting the group state to {} for key {}", requestState, key);
        stateStore.put(key, requestState.toString());
    }

    /**
     * Delete the state for the given request key.
     *
     * @param key The key the request is stored as.
     */
    public void deleteState(String key) {
        stateStore.delete(key);
    }

    /**
     * This is done on application initialization so the class has a "store"
     * to add/remove values from.
     *
     * @param stateStore
     */
    public void setStateStore(KeyValueStore<String, String> stateStore) {
        this.stateStore = stateStore;
    }

}
