package com.qianfeng.bean;

import java.util.List;

/**
 * @Description: user recommend 查询返回的结果
 * @Author: QF
 * @Date: 2020/6/26 10:57 PM
 * @Version V1.0
 * {
 *     "code":200,
 *     "msg":"推荐成功",
 *     "data":[{"aid",123,"probability":0.9232},{"aid",234,"probability":0.9201},,,]
 * }
 */
public class RecommendResult {

    private int code; // 返回状态吗
    private String msg; // 与状态对应的信息

    private List<RecommendInfo> data;

    public RecommendResult() {
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

    public List<RecommendInfo> getData() {
        return data;
    }

    public void setData(List<RecommendInfo> data) {
        this.data = data;
    }
}
