package com.wugk.zktest.controller;

import com.wugk.zktest.data.Data;
import com.wugk.zktest.server.CuratorServer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;

@RestController
public class ZkController {

    @Autowired
    CuratorServer curatorServer;

    @PostConstruct
    void init() {
        curatorServer.init();
    }

    @PostMapping(value = "/create")
    public void CreateNode(Data data) {
        try {
            curatorServer.create(data.getPath(), data.getValue());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @GetMapping(value = "/createDefault")
    public void CreateNode() {
        try {
            curatorServer.create("/myroot", "myroot");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @GetMapping(value = "/hello")
    public String sayHello(){
        return "hello";
    }
}
