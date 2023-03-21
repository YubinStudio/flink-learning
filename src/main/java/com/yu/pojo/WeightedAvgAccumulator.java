package com.yu.pojo;

public class WeightedAvgAccumulator {
    public long sum;
    public int count;

    public WeightedAvgAccumulator() {
    }

    public WeightedAvgAccumulator(long sum, int count) {
        this.sum = sum;
        this.count = count;
    }

    public long getSum() {
        return sum;
    }

    public void setSum(long sum) {
        this.sum = sum;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "WeightedAvgAccumulator{" +
                "sum=" + sum +
                ", count=" + count +
                '}';
    }
}
