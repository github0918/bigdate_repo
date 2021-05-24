package com.qianfeng.service.impl;


import com.qianfeng.bean.RecommendInfo;
import com.qianfeng.bean.RecommendResult;
import com.qianfeng.repostory.HbaseModelFeatureQuery;
import com.qianfeng.repostory.LRModelPredict;
import com.qianfeng.service.RecommendService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * @Description: 推荐服务接口
 * @Author: QF
 * @Date: 2020/7/30 9:59 AM
 * @Version V1.0
 */

@Service
public class RecommendServiceImpl implements RecommendService {
    private static final Logger logger = LoggerFactory.getLogger(RecommendServiceImpl.class);

    //定义执行线程池 --- 线程数量60个
    private ExecutorService pool = Executors.newFixedThreadPool(60);

    /**
     * 根据用户ID，查询HBASE中存储的两路[ALS和Itemc]召回策略结果，并将召回结果，根据从HBASE获取的相关用户及物品特征向量
     * 送入到排序模型进行点击率预测，对每个物品送入到排序模型使用多线程方式，根据打分结果从高到低排序输出。
     * 需要注意的是：
     * 当前这个方法将所有的als和itemcf算法产生的结果都取出来了，实际情况下我们应该每个算法里面取一定
     * 数量[数量和每个算法取数的比例应该是可控的参数],然后对一定数据的物品排序，同时取过的数据，下次就不能再使用了个逻辑
     * 大家可以自己思考实现
     *
     * @param uid 用户ID
     * @return 获取的推荐列表
     */
    @Override
    public RecommendResult recommend(String uid) {
        //初始化推荐结果对象
        RecommendResult result = new RecommendResult();
        result.setCode(0);
        result.setMsg("ok");

        // 从HBASE 中读取特征数据并转换格式
        // key:给uid推荐(ItemCF/ALS)文章Id
        // value:文章基础向量,文章嵌入向量,用户基础向量
        Map<String, String> featureMap = HbaseModelFeatureQuery.transItemFeatureList(uid);

        if (featureMap == null) {
            String msg = String.format("user %s : not exists or  user some info is null", uid);
            logger.error(msg);
            result.setCode(-1);
            result.setMsg(msg);
            result.setData(null);
            return result;
        }

        // 多线程请求预测模型，并获取每个物品的预测结果
        ArrayList<Future<String>> futures = new ArrayList<Future<String>>();
        for (Map.Entry<String, String> entry : featureMap.entrySet()) {
            //获取模型预测实例
            ModelPredict mp = new ModelPredict(entry.getKey() + ":" + entry.getValue());
           //将预测模型加入到线程池中
            Future<String> f = pool.submit(mp);
            futures.add(f);
        }

        //将每个线程执行的结果循环放到list中
        List<String> preditRes = new ArrayList<String>();
        for (Future<String> f : futures) {
            try {
                preditRes.add(f.get());
            } catch (Exception e) {
                logger.error("get predict value error", e);
            }
        }


        // 按照预测结果的可能性数值从高到低排序
        Collections.sort(preditRes, new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                //o1=aid:预测的点击率  o2=aid:预测的点击率
                Double prob1 = Double.valueOf(o1.split(":")[1]);
                Double prob2 = Double.valueOf(o2.split(":")[1]);
                if (prob1 == prob2) {
                    return 0;
                } else {
                    return prob2 > prob1 ? 1 : -1;
                }
            }

        });

        // 封装推荐结果
        List<RecommendInfo> recoInfoList = new ArrayList<RecommendInfo>();
        //循环排序之后的列表(aid:预测点击率)
        for (String preditItem : preditRes) {
            RecommendInfo ri = new RecommendInfo();
            ri.setAid(Integer.valueOf(preditItem.split(":")[0]));
            ri.setProbability(Double.valueOf(preditItem.split(":")[1]));
            recoInfoList.add(ri);
        }
        //设置推荐结果列表到返回对象中
        result.setData(recoInfoList);
        return result;
    }


    //模型预测封装
    private class ModelPredict implements Callable<String> {
        private String feature;

        public ModelPredict(String feature) {
            this.feature = feature;
        }

        @Override
        public String call() {
            String[] fArr = feature.split(":");
            String itemId = fArr[0];  //推荐文章id
            String itemFeature = fArr[1]; //推荐的文章基础向量,文章嵌入向量,用户基础向量
            //预测点击可能性
            Double value = LRModelPredict.predictProbability(itemFeature);
            return itemId + ":" + value;
        }
    }
}


