package com.lk.es.controller;

import com.lk.es.bean.Content;
import com.lk.es.service.ContentService;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@RestController
public class ContentController {

    @Autowired
    ContentService contentService;

    @GetMapping("/insertDoc/{keyword}")
    public Boolean insertDoc(@PathVariable("keyword") String keyword){
        return contentService.parseContent(keyword);
    }

    @GetMapping("/getDoc/{keyword}/{pageNo}/{pageSize}")
    public List<Map<String,Object>> getContents(@PathVariable("keyword") String keyword,@PathVariable("pageNo") int pageNo,@PathVariable("pageSize") int pageSize){
//       return contentService.getContents(keyword,pageNo,pageSize);
        return contentService.getHighContents(keyword,pageNo,pageSize);

    }

}
