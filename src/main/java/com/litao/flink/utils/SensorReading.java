package com.litao.flink.utils;

import java.util.Objects;

public class SensorReading {
    public String id;
    public Long ts;
    public Long temperature;

    public SensorReading() {
    }

    public SensorReading(String id, Long ts, Long temperature) {
        this.id = id;
        this.ts = ts;
        this.temperature = temperature;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Long getTemperature() {
        return temperature;
    }

    public void setTemperature(Long temperature) {
        this.temperature = temperature;
    }

    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", temperature=" + temperature +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SensorReading that = (SensorReading) o;
        return Objects.equals(id, that.id) &&
                Objects.equals(ts, that.ts) &&
                Objects.equals(temperature, that.temperature);
    }

    @Override
    public int hashCode() {

        return Objects.hash(id, ts, temperature);
    }
}
