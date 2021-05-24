package com.qianfeng.service;


import com.qianfeng.bean.RecommendResult;


/**
 * @Description:  从clickhouse 查询用户标签查询
 * @Author: QF
 * @Date: 2020/6/25 12:09 PM
 * @Version V1.0
 */
public interface RecommendService {
    // 用户推荐接口
    RecommendResult recommend(String uid);
}
