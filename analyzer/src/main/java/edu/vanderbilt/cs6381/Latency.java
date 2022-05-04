package edu.vanderbilt.cs6381;

public class Latency {
    private long sendTime;
    private long recvTime;

    public Latency() {
    }

    public Latency(final long sendTime, final long recvTime) {
        this.sendTime = sendTime;
        this.recvTime = recvTime;
    }

    public long getSendTime() {
        return sendTime;
    }

    public void setSendTime(long sendTime) {
        this.sendTime = sendTime;
    }

    public long getRecvTime() {
        return recvTime;
    }

    public void setRecvTime(long recvTime) {
        this.recvTime = recvTime;
    }

    public long getLatency() {
        return recvTime - sendTime;
    }

    @Override
    public String toString() {
        return "Latency{" +
                "sendTime=" + sendTime +
                ", recvTime=" + recvTime +
                ", latency=" + getLatency() +
                '}';
    }
}
