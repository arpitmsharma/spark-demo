package com.github.arpitmsharma;


public class TemperatureEvent {
    private Integer devId;
    private Integer value;

    public TemperatureEvent(Integer devId, Integer value) {
        this.devId = devId;
        this.value = value;
    }

    public Integer getDevId() {
        return devId;
    }

    public void setDevId(Integer devId) {
        this.devId = devId;
    }

    public Integer getValue() {
        return value;
    }

    public void setValue(Integer value) {
        this.value = value;
    }
}