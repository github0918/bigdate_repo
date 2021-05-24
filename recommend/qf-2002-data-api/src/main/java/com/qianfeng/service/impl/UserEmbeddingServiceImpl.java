package com.qianfeng.service.impl;

import com.facebook.presto.jdbc.PrestoArray;
import com.qianfeng.bean.UserEmbeddingInfo;
import com.qianfeng.bean.UserEmbeddingResult;
import com.qianfeng.repostory.MilvusTemplate;
import com.qianfeng.repostory.PrestoQuery;
import com.qianfeng.service.UserEmbeddingService;
import io.milvus.client.MilvusClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.*;

/**
 * @Description: 根据SQL查询Pesto数据
 * @Author: QF
 * @Date: 2020/6/26 10:41 PM
 * @Version V1.0
 */

@Service
public class UserEmbeddingServiceImpl implements UserEmbeddingService {

    private static final Logger logger = LoggerFactory.getLogger(UserEmbeddingServiceImpl.class);

    //注解presto的工具类
    @Autowired
    private PrestoQuery prestoQuery;

    //注解milvus的工具
    @Autowired
    private MilvusTemplate milvusTemplate;


    /**
     * 将用户Embedding插入的milvus中，如果数据量太大，不合适以API方式插入，单独写一个异步插入程序
     * 你可以看到此方法是将所有的用户向量数据先查询出来，然后放入到集合中再插入到milvus，当数据量大时
     * 首先内存会不断增加，同时由于是所有数据全部查询出来再插入milvus，没有分批插入，效率也不会太高。
     * 数据量小时，可以用。
     */
    @Override
    public UserEmbeddingResult loadEmbedding() {
        //获取milvus的客户端
        MilvusClient client = milvusTemplate.getMilvusClient();
        //使用presto查询的sql语句
        String embeddingSQL = "select * from dws_news.user_content_embedding";
        // 从presto 查询用户Embedding
        List<Map<String, Object>> rawResult = prestoQuery.queryBySQL(embeddingSQL);
        UserEmbeddingResult uer = new UserEmbeddingResult();  //输出结果对象
        if (rawResult == null) {
            uer.setCode(-1);
            uer.setMsg("presto query error,please check your sql");
            return uer;

        } else {
            uer.setCode(0);
            uer.setMsg("ok");
        }

        // 将查询的数据，转换为vectors 和vectorIds
        List<List<Float>> vectors = new ArrayList<>();
        List<Long> vectorIds = new ArrayList<>();
        // 用户向量的维度
        for (int i = 0; i < rawResult.size(); ++i) {
            //获取用户向量 每一行中用户向量
            Object userVector = rawResult.get(i).get("user_vector");
            //将用户向量转换成object数组
            PrestoArray prestoArray = (PrestoArray) userVector;
            List<Object> listValue = Arrays.asList((Object[]) prestoArray.getArray());
            //初始化一个集合
            List<Float> floatList = new ArrayList<>();
            //循环向量
            for (Object value : listValue) {
                floatList.add(((Double) value).floatValue());
            }
            //将转换好之后的用户向量存储到vectors
            //[[0.897678876789,0.567886678567,],[],]
            vectors.add(floatList);

            //获取向量id
            Object ouid = rawResult.get(i).get("uid");  //获取uid
            Long uid = Long.parseLong((String) ouid);  //字符串转换long类型
            vectorIds.add(uid);
        }

        // 插入milvus的集合
        String collectionName = "user_embedding";

        // 如果集合不存在创建一个內积距离集合, 维度为64 ，这是我们生成的用户向量的维度
        if (!milvusTemplate.hasCollection(collectionName)) {
            milvusTemplate.createIPCollection(collectionName, 64);
        }
        // 插入数据
        Boolean ok = milvusTemplate.loadData(vectors, vectorIds, collectionName);

        if (!ok) {
            uer.setCode(-2);
            uer.setMsg("user embedding load data to milvus error");
        }
        return uer;
    }



    /**
     * 查询与指定用户相似的用户，即从milvus中查询与之相似的向量
     *
     * @param vectorId
     * @param topK
     * @return
     */
    @Override
    public UserEmbeddingResult searchSimUserById(Long vectorId, long topK) {
        String collectionName = "user_embedding";
        //为什么需要linkedHashMap
        LinkedHashMap<String, Float> res = milvusTemplate.searchIPVectorById(collectionName, vectorId, topK);
        UserEmbeddingResult uer = new UserEmbeddingResult();


        if (res == null) {
            uer.setCode(-1);
            uer.setMsg("some error, maybe not found the uid in milvus ,please confirm the uid already insert into milvus.");
            return uer;
        } else {
            uer.setMsg("ok");
            uer.setCode(0);
        }

        List<UserEmbeddingInfo> ueiList = new ArrayList<>();
        for (Map.Entry<String, Float> entry : res.entrySet()) {
            UserEmbeddingInfo ue = new UserEmbeddingInfo();
            ue.setDistance(entry.getValue());
            ue.setUid(entry.getKey());
            ueiList.add(ue);
        }
        uer.setData(ueiList);

        return uer;
    }

}
