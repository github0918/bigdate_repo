package com.qianfeng.repostory;


import com.google.gson.JsonObject;
import io.milvus.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;


/**
 * milvus template 实现milvus引擎的相关操作
 */
public class MilvusTemplate {

    private static final Logger logger = LoggerFactory.getLogger(MilvusTemplate.class);

    private String host;
    private int port;

    private volatile MilvusClient milvusClient;

    public MilvusTemplate(String host, int port) {
        this.host = host;
        this.port = port;
    }

    // 获取milvus client
    public MilvusClient getMilvusClient() {
        if (null == this.milvusClient) {
            synchronized (this) {
                if (null == this.milvusClient) {
                    MilvusClient client = new MilvusGrpcClient();

                    // Connect to Milvus server
                    ConnectParam connectParam = new ConnectParam.Builder()
                            .withHost(host)
                            .withPort(port)
                            .build();
                    try {
                        Response connectResponse = client.connect(connectParam);
                    } catch (ConnectFailedException e) {
                        System.out.println("Failed to connect to Milvus server: " + e.toString());
                        logger.error("get milvus connect error", e);
                    }
                    this.milvusClient = client;
                    logger.info("milvus client " + this.milvusClient);
                }
            }
        }
        return this.milvusClient;
    }

    /**
     * milvus 插入数据
     *
     * @param vectors        要插入的向量
     * @param vectorIds      要插入的每个向量ID
     * @param collectionName 出入milvus的集合名称
     * @return
     */
    public Boolean loadData(List<List<Float>> vectors, List<Long> vectorIds, String collectionName) {
        vectors =
                vectors.stream().map(MilvusTemplate::normalizeVector).collect(Collectors.toList());

        InsertParam insertParam =
                new InsertParam.Builder(collectionName)
                        .withFloatVectors(vectors) //向量
                        .withVectorIds(vectorIds) //用户id
                        .build();
        //调用客户端插入数据
        InsertResponse insertResponse = this.getMilvusClient().insert(insertParam);

        if (!insertResponse.ok()) {
            logger.error("load data to milvus error: " + insertResponse.getResponse().getMessage());
            return false;
        }
//        List<Long> resVectorIds = insertResponse.getVectorIds();
        return true;
    }

    /**
     * 创建milvus 內积类型的集合，也就是余弦距离度量相似度
     *
     * @param collectionName 集合的名称
     * @param dimension      集合中向量的维度
     * @return
     */
    public Boolean createIPCollection(String collectionName, long dimension) {
        final long indexFileSize = 1024; // maximum size (in MB) of each index file
        final MetricType metricType = MetricType.IP;
        // we choose IP (Inner Product) as our metric type

        CollectionMapping collectionMapping =
                new CollectionMapping.Builder(collectionName, dimension)
                        .withIndexFileSize(indexFileSize)
                        .withMetricType(metricType)
                        .build();

        // 检查集合是否存在，不存在才创建
        HasCollectionResponse hasCollectionResponse = this.getMilvusClient().hasCollection(collectionName);
        if (!hasCollectionResponse.hasCollection()) {
            Response createCollectionResponse = this.getMilvusClient().createCollection(collectionMapping);
            if (!createCollectionResponse.ok()) {
                logger.error("create milvus collection error " + createCollectionResponse.getMessage());
                return false;
            }
        }

        return true;
    }

    /**
     * 删除一个milvs集合
     *
     * @param collectionName 集合的名称
     * @return
     */
    public Boolean deleteCollection(String collectionName) {

        // 检查集合是否存在，存在才删除
        HasCollectionResponse hasCollectionResponse = this.getMilvusClient().hasCollection(collectionName);
        if (!hasCollectionResponse.hasCollection()) {
            Response dropCollectionResponse = this.getMilvusClient().dropCollection(collectionName);
            if (!dropCollectionResponse.ok()) {
                logger.error("drop milvus collection error " + dropCollectionResponse.getMessage());
                return false;
            }
        }

        return true;
    }

    /**
     * 判断一个集合是否存在
     *
     * @param collectionName 集合名称
     * @return
     */
    public Boolean hasCollection(String collectionName) {

        // 检查集合是否存在，存在才删除
        HasCollectionResponse hasCollectionResponse = this.getMilvusClient().hasCollection(collectionName);
        // hasCollectionResponse.hasCollection() : true 存在
        if (!hasCollectionResponse.hasCollection()) {
            return false;
        }

        return true;
    }

    /**
     * 根据向量ID，搜索与之相似的向量，使用內积距离，就是余弦距离
     *
     * @param collectionName 集合名称
     * @param userId       要搜索的向量ID
     * @param topK           要返回topK个相似向量ID
     * @return
     */
    public LinkedHashMap<String, Float> searchIPVectorById(String collectionName, Long userId, long topK) {
        //初始化集合
        List<Long> userIds = new ArrayList<>();
        userIds.add(userId);
        // 根据用户ID检索出，该用户的向量  --- > 云讯查询多个用户的相似用户
        GetEntityByIDResponse response = this.getMilvusClient().getEntityByID(collectionName, userIds);

        List<List<Float>> searchVec = response.getFloatVectors();
        if (searchVec.get(0).size() == 0) {
            logger.error("search vector id not exists");
            return null;
        }

        // 构建搜索参数
        JsonObject searchParamsJson = new JsonObject();
        searchParamsJson.addProperty("nprobe", 20);  //值越大搜索约精准

        SearchParam searchParam =
                new SearchParam.Builder(collectionName) //添加集合名
                        .withFloatVectors(searchVec)  //添加搜索的向量
                        .withTopK(topK)  //设置搜索多少个
                        .withParamsInJson(searchParamsJson.toString())
                        .build();

        // 根据用户向量，从milvus查询与之相似向量
        SearchResponse searchResponse = this.getMilvusClient().search(searchParam);

        //判断搜索响应是否正常
        if (searchResponse.ok()) {
            List<List<SearchResponse.QueryResult>> queryResultsList =
                    searchResponse.getQueryResultsList();

            final double epsilon = 0.001;
            SearchResponse.QueryResult firstQueryResult = queryResultsList.get(0).get(0);
            //[123]
            //firstQueryResult.getVectorId() != userIds.get(0)  返回结果中的第一个id是否和传入进去的用户id相等
            //Math.abs(1 - firstQueryResult.getDistance()) > epsilon   要求相似度很高；
            // Math.abs(1 - firstQueryResult.getDistance()) 小于0.0001  0.81268847
            //判断如果第一个数据不是自己，直接报错。自己和自己的距离：0.999998
            if (firstQueryResult.getVectorId() != userIds.get(0)
                    || Math.abs(1 - firstQueryResult.getDistance()) > epsilon) {
                logger.error("search user from milvus error ");
                return null;
            }
        }

        // 获取查询的相似的向量ID，这里就是用户ID
        List<List<Long>> resultIds = searchResponse.getResultIdsList();  //获取相似用户的id列表
        List<Long> simIds = resultIds.get(0);  //取相似用户id 【123,125,126】
        // 获取与之相似向量的余弦值
        List<List<Float>> resultDistances = searchResponse.getResultDistancesList();
        List<Float> simIdsDistances = resultDistances.get(0);  //[0.432,0.788,0.985]

        // 封装数据
        LinkedHashMap<String, Float> resDis = new LinkedHashMap<String, Float>();
        for (int i = 0; i < simIdsDistances.size(); i++) {
            HashMap<Long, Float> ud = new HashMap<>();
            //判断相似用户id列表中是否和当用用户id一样，，一样则直接进下一个的循环
            if (simIds.get(i).equals(userId)) {  //排除当前用户的id
                continue;
            }
            resDis.put(simIds.get(i).toString(), simIdsDistances.get(i));
        }

        return resDis;
    }




    // Helper function that normalizes a vector if you are using IP (Inner Product) as your metric
    // type , 正则化向量，如果使用內积度量距离时
    static List<Float> normalizeVector(List<Float> vector) {
        float squareSum = vector.stream().map(x -> x * x).reduce((float) 0, Float::sum);
        final float norm = (float) Math.sqrt(squareSum);
        vector = vector.stream().map(x -> x / norm).collect(Collectors.toList());
        return vector;
    }
}