package com.qianfeng.bean;


import com.qianfeng.util.Sample;

import java.util.LinkedHashMap;
import java.util.List;

/**
 * @Description: 拟合的留存曲线信息
 * @Author: QF
 * @Date: 2020/6/25 12:14 PM
 * @Version V1.0
 */
public class RetentionCurveInfo {
    // y= a * x^b
    // theta0 = a
    // theta1 = b
    private Double theta0;
    private Double theta1;
    // 方程式
    private String equation;

    // 原始样本点
    private List<Sample> samples;

    // 留存率
    private LinkedHashMap<String, Double> rrMap;

    // 预测的留存率
    public LinkedHashMap<String, Double> getRrMap() {
        return rrMap;
    }

    public void setRrMap(LinkedHashMap<String, Double> rrMap) {
        this.rrMap = rrMap;
    }



    public RetentionCurveInfo() {
    }

    public List<Sample> getSamples() {
        return samples;
    }

    public void setSamples(List<Sample> samples) {
        this.samples = samples;
    }

    public Double getTheta0() {
        return theta0;
    }

    public void setTheta0(Double theta0) {
        this.theta0 = theta0;
    }

    public Double getTheta1() {
        return theta1;
    }

    public void setTheta1(Double theta1) {
        this.theta1 = theta1;
    }

    public String getEquation() {
        return equation;
    }

    public void setEquation(String equation) {
        this.equation = equation;
    }
}
