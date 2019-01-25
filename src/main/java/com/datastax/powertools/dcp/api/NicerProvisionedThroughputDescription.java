package com.datastax.powertools.dcp.api;

/*
 *
 * @author Sebastián Estévez on 1/24/19.
 *
 */


import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;

public class NicerProvisionedThroughputDescription {

    @JsonProperty("LastIncreaseDateTime")
    private Date lastIncreaseDateTime;
    @JsonProperty("LastDecreaseDateTime")
    private Date lastDecreaseDateTime;
    @JsonProperty("NumberOfDecreasesToday")
    private Long numberOfDecreasesToday;
    @JsonProperty("ReadCapacityUnits")
    private Long readCapacityUnits;
    @JsonProperty("WriteCapacityUnits")
    private Long writeCapacityUnits;

    public NicerProvisionedThroughputDescription() {
    }

    public Date getLastIncreaseDateTime() {
        return lastIncreaseDateTime;
    }

    public void setLastIncreaseDateTime(Date lastIncreaseDateTime) {
        this.lastIncreaseDateTime = lastIncreaseDateTime;
    }

    public Date getLastDecreaseDateTime() {
        return lastDecreaseDateTime;
    }

    public void setLastDecreaseDateTime(Date lastDecreaseDateTime) {
        this.lastDecreaseDateTime = lastDecreaseDateTime;
    }

    public Long getNumberOfDecreasesToday() {
        return numberOfDecreasesToday;
    }

    public void setNumberOfDecreasesToday(Long numberOfDecreasesToday) {
        this.numberOfDecreasesToday = numberOfDecreasesToday;
    }

    public Long getReadCapacityUnits() {
        return readCapacityUnits;
    }

    public void setReadCapacityUnits(Long readCapacityUnits) {
        this.readCapacityUnits = readCapacityUnits;
    }

    public Long getWriteCapacityUnits() {
        return writeCapacityUnits;
    }

    public void setWriteCapacityUnits(Long writeCapacityUnits) {
        this.writeCapacityUnits = writeCapacityUnits;
    }
}
