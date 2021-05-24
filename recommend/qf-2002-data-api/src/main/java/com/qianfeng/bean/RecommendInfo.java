package com.qianfeng.bean;

/**
 * @Description: 查询结果封装，表是推荐的物品ID和ID对应排序分数
 * @Author: QF
 * @Date: 2020/7/10 12:07 PM
 * @Version V1.0
 */
public class RecommendInfo {
    private int aid;
    private Double probability;

    public int getAid() {
        return aid;
    }

    public void setAid(int aid) {
        this.aid = aid;
    }

    public Double getProbability() {
        return probability;
    }

    public void setProbability(Double probability) {
        this.probability = probability;
    }
}
