package edu.iris.miniseedutils.steim;

import java.lang.System;
import java.sql.Timestamp;
import java.util.Date;

public class MiniSeedRecordHeader
        implements Comparable
{
    protected String network;
    protected String station;
    protected String locID;
    protected String channel;
    protected double startTime;
    protected Timestamp startTimes;
    protected double endTime;
    protected Timestamp endTimes;

    protected int numSamples;
    protected double sampleRate;
    protected String sequenceNumber;
    protected int activeFlag;
    protected int recordLength;
    protected int caliFlag;

    public String getChannelLocator()
    {
        return this.network + "/" + this.station + "/" + this.locID + "/" + this.channel;
    }

    protected static void build(GenericMiniSeedRecord g, MiniSeedRecordHeader msr) {
        //System.out.print(g.getStartTime());
        //System.out.print("hhhh");
        //System.out.print(g.getEndTime());

        msr.setNetwork(g.getNetwork());
        msr.setStation(g.getStation());
        msr.setLocID(g.getLocID());
        msr.setChannel(g.getChannel());

        long ms = g.getStartTime().getTime() / 1000L * 1000L;
        msr.setStartTime(ms / 1000L + g.getStartTime().getNanos() / 1000000000.0D);
        msr.setStartTimes(g.getStartTime());
        long ed_ms = g.getEndTime().getTime() / 1000L * 1000L;
        msr.setEndTime(ed_ms / 1000L + g.getEndTime().getNanos() / 1000000000.0D);
        msr.setEndTimes(g.getEndTime());
        msr.setSampleRate(g.getSampleRate());
        msr.setNumSamples(g.getNumSamples());

        msr.sequenceNumber = g.getSequenceNumber();
        msr.activeFlag = g.getActiveFlag();
        msr.caliFlag = g.getCaliFlag();
        msr.recordLength = g.getRecordLength();
    }

    public String getChannel()
    {
        return this.channel;
    }
    public void setChannel(String chan) {
        this.channel = chan;
    }
    public String getLocID() {
        return this.locID;
    }
    public void setLocID(String locId) {
        this.locID = locId;
    }
    public String getNetwork() {
        return this.network;
    }
    public void setNetwork(String net) {
        this.network = net;
    }
    public int getNumSamples() {
        return this.numSamples;
    }
    public void setNumSamples(int sampNum) {
        this.numSamples = sampNum;
    }
    public double getSampleRate() {
        return this.sampleRate;
    }
    public void setSampleRate(double sampRate) {
        this.sampleRate = sampRate;
    }
    public String getStation() {
        return this.station;
    }
    public void setStation(String sta) {
        this.station = sta;
    }

    public int compareTo(Object obj) {
        MiniSeedRecordHeader msc = (MiniSeedRecordHeader)obj;
        return (int)Math.signum(this.startTime - msc.startTime);
    }

    public int getRecordLength()
    {
        return this.recordLength;
    }

    public void setRecordLength(int recLength) {
        this.recordLength = recLength;
    }

    public String toString()
    {
        return getSequenceNumber() + "" + getNetwork() + "/" + getStation() +
                "/" + getLocID() + "/" + getChannel() +
                " "  + getNumSamples() + " sps " +
            getSampleRate() + " ActiveFlag:" + getActiveFlag() + " CaliFlag:" + getCaliFlag();
    }

    public int getActiveFlag() {
        return this.activeFlag;
    }

    public void setActiveFlag(int activeFlag) {
        this.activeFlag = activeFlag;
    }

    public int getCaliFlag() {
        return this.caliFlag;
    }

    public void setCaliFlag(int caliFlag) {
        this.caliFlag = caliFlag;
    }

    public String getSequenceNumber() {
        return this.sequenceNumber;
    }

    public void setSequenceNumber(String sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    public double getEndTime()
    {
        return this.endTime;
    }
    public Timestamp getEndTimes()
    {
        return this.endTimes;
    }

    public void setEndTime(double endTime)
    {
        this.endTime = endTime;
    }
    public void setEndTimes(Timestamp endTime)
    {
        this.endTimes = endTime;
    }

    public double getStartTime()
    {
        return this.startTime;
    }
    public Timestamp getStartTimes()
    {
        return this.startTimes;
    }

    public void setStartTime(double startTime)
    {
        this.startTime = startTime;
    }
    public void setStartTimes(Timestamp startTime)
    {
        this.startTimes = startTime;
    }
}