package edu.vanderbilt.cs6381;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

public class Wrapper {
    @JsonProperty("value")
    private OHLC value;

    public OHLC getValue() {
        return value;
    }

    public void setValue(OHLC value) {
        this.value = value;
    }
}
