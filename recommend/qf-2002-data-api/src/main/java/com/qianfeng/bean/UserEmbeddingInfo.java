package com.qianfeng.bean;

/**
 * @Description: 查询结果封装，表示了用户ID及该ID与查询ID的相似度
 * @Author: QF
 * @Date: 2020/7/10 12:07 PM
 * @Version V1.0
 */
public class UserEmbeddingInfo {
    private String uid;
    private Float distance;

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public Float getDistance() {
        return distance;
    }

    public void setDistance(Float distance) {
        this.distance = distance;
    }
}
