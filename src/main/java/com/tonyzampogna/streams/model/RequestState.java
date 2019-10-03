package com.tonyzampogna.streams.model;

import lombok.Data;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;


@Data
public class RequestState {

    public enum Status {
        SUCCESS, ERROR
    }

    private Status status;
    private String message;


    public RequestState() {
        super();
    }

    public static RequestState deserialize(String value) {
        String[] valueParts = StringUtils.split(value, ",");
        RequestState requestState = new RequestState();
        requestState.setStatus(Status.valueOf(valueParts[0]));
        requestState.setMessage(valueParts[1]);
        return requestState;
    }

    public String toString() {
        return this.status.toString() + "," + this.message;
    }

}
