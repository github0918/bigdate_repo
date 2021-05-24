package com.qianfeng.util;

/**
 * @Description: 样本点的实体类，记录每一个样本数据X就是特征，Y就是标签
 * @Author: QF
 * @Date: 2020/6/24 10:22 PM
 * @Version V1.0
 */
public class Sample {
    double X;
    double Y;

    public Sample(double x, double y) {
        X = x;
        Y = y;
    }

    public Sample() {
    }

    public double getX() {
        return X;
    }

    public void setX(double x) {
        X = x;
    }

    public double getY() {
        return Y;
    }

    public void setY(double y) {
        Y = y;
    }
}
