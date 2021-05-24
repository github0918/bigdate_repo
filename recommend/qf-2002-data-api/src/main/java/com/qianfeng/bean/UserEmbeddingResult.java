package com.qianfeng.bean;

import java.util.List;

/**
 * @Description: user embedding 查询返回的结果
 * @Author: QF
 * @Date: 2020/6/26 10:57 PM
 * @Version V1.0
 */
public class UserEmbeddingResult {

    private int code; // 返回状态吗
    private String msg; // 与状态对应的信息

    private List<UserEmbeddingInfo> data;

    public UserEmbeddingResult() {
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

    public List<UserEmbeddingInfo> getData() {
        return data;
    }

    public void setData(List<UserEmbeddingInfo> data) {
        this.data = data;
    }
}
