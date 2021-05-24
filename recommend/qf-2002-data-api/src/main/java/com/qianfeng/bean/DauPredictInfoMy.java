package com.qianfeng.bean;

import java.util.ArrayList;
import java.util.Map;

/**
 * @Description: Dau预测结果的实体类
 * @Author: QF
 * @Date: 2020/6/25 5:21 PM
 * @Version V1.0
 */
public class DauPredictInfoMy {

    private int code;
    private String msg;

    //data:{"category":["2020-09-14",""],"dau":[5536,6889,]}
    /*private ArrayList<String> category;
    private ArrayList<Integer> dau;*/
    private Map<String, ArrayList<String>> categoryDAU;  //是用于存储day-预测的dau值


    public DauPredictInfoMy() {
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

    public Map<String, ArrayList<String>> getCategoryDAU() {
        return categoryDAU;
    }

    public void setCategoryDAU(Map<String, ArrayList<String>> categoryDAU) {
        this.categoryDAU = categoryDAU;
    }
}
