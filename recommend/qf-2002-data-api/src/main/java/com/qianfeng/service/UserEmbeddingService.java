package com.qianfeng.service;


import com.qianfeng.bean.UserEmbeddingResult;

/**
 * @Description: 用户向量载入与相似向量搜索接口
 * @Author: QF
 * @Date: 2020/6/25 12:09 PM
 * @Version V1.0
 */
public interface UserEmbeddingService {

    public UserEmbeddingResult loadEmbedding();

    public UserEmbeddingResult searchSimUserById(Long vectorId, long topK);
}
