package com.qianfeng.repostory;

import com.alibaba.fastjson.JSONObject;
import org.dmg.pmml.FieldName;
import org.jpmml.evaluator.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Component;
import org.xml.sax.SAXException;

import javax.annotation.PostConstruct;
import javax.xml.bind.JAXBException;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @Description:
 * @Author: QF
 * @Date: 2020/8/3 5:07 PM
 * @Version V1.0
 *  使用LR模型为推荐的每一个物品进行点击率的预测
 */

@Component
public class LRModelPredict {

    private static Logger logger = LoggerFactory.getLogger(LRModelPredict.class);
    private static Evaluator evaluator;

    //初始化LR模型的计算器
    @PostConstruct
    public void init() {
        //使用类加载器--加载pmml模型
        ClassPathResource classPathResource = new ClassPathResource("lr.pmml");
        try {
            InputStream inputStream = classPathResource.getInputStream();
            //将模型还原成计算器
            evaluator = new LoadingModelEvaluatorBuilder().load(inputStream).build();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SAXException e) {
            e.printStackTrace();
        } catch (JAXBException e) {
            e.printStackTrace();
        }
        evaluator.verify();
    }


    /**
     * 传入特征给模型进行预测，特征格式为逗号分隔的特征值
     *
     * @param feature 格式为 逗号分隔的特征值
     * @return
     */
    public static Double predictProbability(String feature) {
        // 获取模型定义的特征字段
        List<? extends InputField> inputFields = evaluator.getInputFields();

        //将传入进来的向量进行拆分
        String[] featureArray = feature.split(","); //输入向量

        //判断特征字段个数是否和输入的字段数量不等
        if (inputFields.size() != featureArray.length) {
            logger.error(String.format("model input feature size error, need features size %s ,but %s",
                    inputFields.size(), featureArray.length));
            return -1.0;
        }

        //使用有序的Map
        Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();
        int count = 0;
        // 变量模型的特征字段，给每个特征字段赋值
        for (InputField inputField : inputFields) {
            //取出对应特征字段名称
            FieldName inputName = inputField.getName();
            Object rawValue = featureArray[count];
            count = count + 1;
            //为对应的特征字段赋值
            FieldValue inputValue = inputField.prepare(rawValue);
            arguments.put(inputName, inputValue);
            System.out.println(inputName.getValue()+"==="+inputValue.getValue().toString());
        }

        // 预测结果
        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        // 解析模型预测结果
        Map<String, ?> resultRecord = EvaluatorUtil.decodeAll(results);
//        logger.info(resultRecord.toString());
        String label = resultRecord.get("label").toString();
        // 获取概率
        String probability = resultRecord.get(String.format("probability(%s)", label)).toString();

        return Double.valueOf(probability); //将概率转换成double类型
    }


    /**
     * 支持传入JSON 格式的特征
     *
     * @param feature json 格式特征
     * @return
     */
    public static Double predictProbabilityByJson(JSONObject feature) {
        // 获取模型定义的特征
        List<? extends InputField> inputFields = evaluator.getInputFields();

        Map<FieldName, FieldValue> arguments = new LinkedHashMap<>();
        for (InputField inputField : inputFields) {
            FieldName inputName = inputField.getName();
            String name = inputName.getValue();
            Object rawValue = feature.getDoubleValue(name);
            FieldValue inputValue = inputField.prepare(rawValue);
            arguments.put(inputName, inputValue);
        }

        Map<FieldName, ?> results = evaluator.evaluate(arguments);
        Map<String, ?> resultRecord = EvaluatorUtil.decodeAll(results);
        logger.info(resultRecord.toString());
        String label = resultRecord.get("label").toString();
        String probability = resultRecord.get(String.format("probability(%s)", label)).toString();

        return Double.valueOf(probability);
    }
}
