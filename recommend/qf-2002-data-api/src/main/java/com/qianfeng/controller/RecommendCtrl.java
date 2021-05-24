package com.qianfeng.controller;

import com.qianfeng.bean.RecommendResult;
import com.qianfeng.service.RecommendService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.*;

/**
 * @Description: 推荐服务接口
 * @Author: QF
 * @Date: 2020/6/26 10:32 PM
 * @Version V1.0
 */
@RestController
@SpringBootApplication
@RequestMapping(value = "api/v1/")
public class RecommendCtrl {

    @Autowired
    private RecommendService rs;


    /**
     * 指定用户ID, 给用户推荐物品，两路 召回+ LR(PMML)排序模型
     * @param uid
     * @return
     */
    @RequestMapping(value = "recommend/{uid}", method = {RequestMethod.GET})
    @ResponseBody
    public RecommendResult recommend(@PathVariable String uid) {
        return rs.recommend(uid);
    }

}
