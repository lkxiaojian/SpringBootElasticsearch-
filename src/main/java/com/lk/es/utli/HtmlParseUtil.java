package com.lk.es.utli;

import com.lk.es.bean.Content;
import lombok.SneakyThrows;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class HtmlParseUtil {


//    public static void main(String[] args){
//        parseJD("vue");
//    }

    public  List<Content> parseJD(String name){

        String url=" https://search.jd.com/Search?keyword="+name;
        try {
            Document document = Jsoup.parse(new URL(url), 30000);
            Element element = document.getElementById("J_goodsList");
            Elements li = element.getElementsByTag("li");
            ArrayList<Content> contents = new ArrayList<>();
            for (Element el : li) {
                String image = el.getElementsByTag("img").eq(0).attr("data-lazy-img");
                String price = el.getElementsByClass("p-price").eq(0).text();
                String title = el.getElementsByClass("p-name").eq(0).text();
//                System.out.println("===================");
//                System.out.println(image);
//                System.out.println(price);
//                System.out.println(title);
                Content content = new Content(title, image, price);
                contents.add(content);
            }
            return contents;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;

    }
}
