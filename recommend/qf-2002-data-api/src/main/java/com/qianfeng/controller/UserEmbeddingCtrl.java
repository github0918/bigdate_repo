package com.qianfeng.controller;

import com.qianfeng.bean.UserEmbeddingResult;
import com.qianfeng.service.UserEmbeddingService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

/**
 * @Description: 用户向量加载与搜索API
 * @Author: QF
 * @Date: 2020/6/25 8:09 PM
 * @Version V1.0
 */

@RestController
@SpringBootApplication
@RequestMapping(value = "api/v1/")
public class UserEmbeddingCtrl {

    @Autowired
    private UserEmbeddingService ues;


    /**
     * 从Presto中加载用户向量到milvus
     *
     * @return
     */
    @RequestMapping(value = "user/embedding/load", method = {RequestMethod.POST})
    @ResponseBody
    public UserEmbeddingResult loadEmbedding() {
        return ues.loadEmbedding();
    }


    /**
     * 从milvus搜索topK相似用户
     *
     * @param uid  要查询的用户ID
     * @param topk 与查询用户相似的topK用户ID   ?uid=1&uname=zs    /1/zs
     * @return
     */
    @RequestMapping(value = "user/embedding/search/{uid}/{topk}", method = {RequestMethod.GET})
    @ResponseBody
    public UserEmbeddingResult searchSimUser(@PathVariable String uid, @PathVariable String topk) {
        // 这里topK+1,是因为与查询向量相似的向量，还包含他本身，因此我们会去掉这个向量，所有查询结果上再补充一个
        return ues.searchSimUserById(Long.parseLong(uid), Long.parseLong(topk)+1);
    }

}
