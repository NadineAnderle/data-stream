package com.example.model;

import com.fasterxml.jackson.annotation.JsonProperty;

public class HealthData {
    @JsonProperty("patient_id")
    private String patientId;
    
    @JsonProperty("heart_rate")
    private double heartRate;
    
    @JsonProperty("temperature")
    private double temperature;
    
    @JsonProperty("oxygen_saturation")
    private double oxygenSaturation;

    @JsonProperty("large_data")
    private String largeData;
    
    private long timestamp;

    // Getters e Setters
    public String getPatientId() { return patientId; }
    public void setPatientId(String patientId) { this.patientId = patientId; }
    
    public double getHeartRate() { return heartRate; }
    public void setHeartRate(double heartRate) { this.heartRate = heartRate; }
    
    public double getTemperature() { return temperature; }
    public void setTemperature(double temperature) { this.temperature = temperature; }
    
    public double getOxygenSaturation() { return oxygenSaturation; }
    public void setOxygenSaturation(double oxygenSaturation) { this.oxygenSaturation = oxygenSaturation; }
    
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public String getLargeData() { return largeData; }
    public void setLargeData(String largeData) { this.largeData = largeData; }
}
