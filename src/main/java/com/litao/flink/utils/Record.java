package com.litao.flink.utils;

// 基础Record pojo类,模仿kafka事件
// pojo类 : 1.公共类 2.拥有无参构造 3.属性公共 4.属性可序列化
public class Record {
    public Long id;
    public String name;
    public String page;
    public Long timestamp;

    public Record() {
    }

    public Record(Long id, String name, String page, Long timestamp) {
        this.id = id;
        this.name = name;
        this.page = page;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "Record{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", page='" + page + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
