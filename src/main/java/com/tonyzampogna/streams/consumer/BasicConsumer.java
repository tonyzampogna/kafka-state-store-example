package com.tonyzampogna.streams.consumer;

import com.tonyzampogna.streams.config.KafkaConfig;
import com.tonyzampogna.streams.model.Message;
import com.tonyzampogna.streams.model.RequestState;
import com.tonyzampogna.streams.streams.StateStore;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;


@Component
@Slf4j
@KafkaListener(
        topics = KafkaConfig.EXAMPLE_TOPIC,
        containerFactory = KafkaConfig.CONTAINER_FACTORY)
public class BasicConsumer {

    @Autowired
    private StateStore stateStore;


    @KafkaHandler
    public void handleGroupMessage(@Payload Message message) {
        log.info("Topic {} received message with key: {}", KafkaConfig.EXAMPLE_TOPIC, message.getKey());
        try {
            if (message.getKey().equals("1")) {
                log.info("First message");
            }
            stateStore.setState(message.getKey(), createSuccessRequestState());
            log.info("New group successfully created!");
        }
        catch (Exception e) {
            log.error("An exception happened processing message with key: {}", message.getKey(), e);
            stateStore.setState(message.getKey(), createErrorRequestState(e));
        }
    }


    private RequestState createSuccessRequestState() {
        RequestState requestState = new RequestState();
        requestState.setStatus(RequestState.Status.SUCCESS);
        requestState.setMessage("SUCCESS");
        return requestState;
    }

    private RequestState createErrorRequestState(Exception e) {
        RequestState requestState = new RequestState();
        requestState.setStatus(RequestState.Status.ERROR);
        requestState.setMessage(e.getMessage());
        return requestState;
    }

}

