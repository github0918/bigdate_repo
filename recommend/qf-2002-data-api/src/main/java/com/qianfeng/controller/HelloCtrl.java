package com.qianfeng.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HelloCtrl {

    @RequestMapping("/say")
    public String sayHello(){
        return "hello,i am spring boot!!!";
    }
}
