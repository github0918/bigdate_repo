package com.qianfeng.repostory;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description: 查询hbase中存储的特征数据
 * @Author: QF
 * @Date: 2020/8/6 10:56 AM
 * @Version V1.0
 */
@Component
public class HbaseModelFeatureQuery {

    private static final Logger logger = LoggerFactory.getLogger(HbaseModelFeatureQuery.class);
    @Autowired
    private HbaseTemplate hbaseTemplate;

    private static HbaseTemplate hbaseTemplateStatic;

    private static HbaseModelFeatureQuery hbaseModeFeatureQuery;

    static String FEATURE_TABLE_NAME = "recommend:union-feature";

    //初始化
    @PostConstruct
    public void init(){
        hbaseModeFeatureQuery = this;
        hbaseModeFeatureQuery.hbaseTemplateStatic=this.hbaseTemplate;
        hbaseTemplateStatic.getConnection();
    }

    //从Hbase union-feature 取出所有的列column-value 转换成map=> key->column value->value
    public static Map<String,String> parseFeatures(String uid) {
        Map<String,String> map = new HashMap<String,String>();
        try {
            //将用户id传入封装hbase的Get对象中
            Get get = new Get(uid.getBytes());
            Table htable = hbaseTemplateStatic.getConnection().getTable(TableName.valueOf(FEATURE_TABLE_NAME));
            //获取某个用户的结果数据
            Result rs = htable.get(get);
            //对rs结果进行判断
            if(rs.isEmpty()) {
                logger.error("user not exist" + ":" + uid);
                return null;
            }else {
                Cell[] cells = rs.rawCells();
                for(Cell cell : cells ) {
                    //获取hbase中某行的列
                    String qualifier = new String(CellUtil.cloneQualifier(cell));
                    //获取hbase中某行的value
                    String value = new String(CellUtil.cloneValue(cell),"UTF-8");
                    //value不等于空
                    if(!"".equals(value)) {
                        map.put(qualifier, value);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }

    // 将从hbase取出的特征数据，转换为每个物品列表及其对应的相关向量
    // 注意这里将所有的als和itemcf算法产生的结果都取出来了，实际情况下我们应该每个算法里面取topk,然后
    // 对topk的数据排序，同时取过的数据，下次就不能再使用了，并且每个算法取的数据比例是可控的，这个逻辑
    // 需要大家自己实现
    public static Map<String,String> transItemFeatureList(String uid) {
        //解析用户特征
        Map<String,String> parseFeatures = parseFeatures(uid);
        if (parseFeatures==null){
            return null;
        }

        //定义kve-value
        Map<String,String> itemFeatureMap = new HashMap<String,String>();
        String userVector = parseFeatures.get("uf"); //取出用户基础特征向量
        String als = parseFeatures.get("als"); //取出als召回算法推荐物品列表及物品基础向量、物品嵌入向量
        String itemcf = parseFeatures.get("itemcf");
        //任何一列值不能为空
        if (itemcf==null|| userVector==null|| als==null){
            logger.error("user info [uf,als,itemcf] some features is null");
            return null;
        }
        //uf,als,itemcf三个向量都不为空
        String[]  alsItemArray = als.split(";");  //拆分每一个item(文章)
        for (String alsItem :alsItemArray){
            String[] alsInfo = alsItem.split(":"); //拆分：itemId:rating:基础向量:文章嵌入向量
            String alsItemID = alsInfo[0];
            String alsItemVector = alsInfo[2];
            String alsItemEmbedding = alsInfo[3];
            String unionFeature = StringUtils.strip(alsItemVector,"[]")+","+
                    StringUtils.strip(alsItemEmbedding,"[]")+","+
                    StringUtils.strip(userVector,"[]");
            itemFeatureMap.put(alsItemID,unionFeature);
        }

        //基于ItemCF的推荐的物品及向量
        String[]  itemcfArray = itemcf.split(";");
        for (String itemcfItem :itemcfArray){
            String[] itemcfInfo = itemcfItem.split(":");
            String itemcfItemID = itemcfInfo[0];
            String itemcfItemVector = itemcfInfo[2];
            String itemcfItemEmbedding = itemcfInfo[3];
            String unionFeature = StringUtils.strip(itemcfItemVector,"[]")+","+
                    StringUtils.strip(itemcfItemEmbedding,"[]")+","+
                    StringUtils.strip(userVector,"[]");
            itemFeatureMap.put(itemcfItemID,unionFeature);
        }
        return itemFeatureMap;
    }

}

