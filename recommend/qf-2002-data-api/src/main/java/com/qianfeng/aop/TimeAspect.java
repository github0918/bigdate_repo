package com.qianfeng.aop;

import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;


@Component
@Aspect
/**
 * 记录函数执行时间
 */
public class TimeAspect {

    private static final Logger logger = LoggerFactory.getLogger(TimeAspect.class);

    /**
     * 定义一个切入点.
     * <p>
     * <p>
     * ~ 第一个 * 代表任意修饰符及任意返回值.
     * ~ 第二个 * 定义在controller包或者子包
     * ~ 第三个 * 任意方法
     * ~ .. 匹配任意数量的参数.
     */
    @Pointcut("execution(* com.qianfeng.controller ..*.*(..)) || execution(* com.qianfeng.repostory ..*query*(..))")

    public void logPointcut() {
    }

    @org.aspectj.lang.annotation.Around("logPointcut()")

    public Object doAround(ProceedingJoinPoint joinPoint) throws Throwable {
        long start = System.currentTimeMillis();
        try {
            Object result = joinPoint.proceed();
            long end = System.currentTimeMillis();
            logger.info("执行耗时 " + joinPoint + "\tUse time : " + (end - start) + " ms!");
            return result;

        } catch (Throwable e) {
            long end = System.currentTimeMillis();
            logger.info("执行耗时 " + joinPoint + "\tUse time : " + (end - start) + " ms with exception : " + e.getMessage());
            throw e;
        }
    }

}