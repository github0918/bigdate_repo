package com.qianfeng.bean;

import java.util.List;
import java.util.Map;

/**
 * @Description: raw sql 查询返回的结果
 * @Author: QF
 * @Date: 2020/6/26 10:57 PM
 * @Version V1.0
 */
public class RawSQLResult {

    private int code; // 返回状态吗
    private String msg; // 与状态对应的信息
    private List<Map<String, Object>> data; // 结果数据

    public RawSQLResult() {
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

    public List<Map<String, Object>> getData() {
        return data;
    }

    public void setData(List<Map<String, Object>> data) {
        this.data = data;
    }


}
