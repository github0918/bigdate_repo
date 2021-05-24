package com.qianfeng.bean;

import java.util.LinkedHashMap;

/**
 * @Description: Dau预测结果的实体类
 * @Author: QF
 * @Date: 2020/6/25 5:21 PM
 * @Version V1.0
 */
public class DauPredictInfo {
    //留存信息
    private RetentionCurveInfo rci;

    private int code;
    private String msg;

    private LinkedHashMap<String, Integer> predictDAU;  //是用于存储day-预测的dau值


    public DauPredictInfo() {
    }

    public LinkedHashMap<String, Integer> getPredictDAU() {
        return predictDAU;
    }

    public void setPredictDAU(LinkedHashMap<String, Integer> predictDAU) {
        this.predictDAU = predictDAU;
    }

    public RetentionCurveInfo getRci() {
        return rci;
    }

    public void setRci(RetentionCurveInfo rci) {
        this.rci = rci;
    }


    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }
}
