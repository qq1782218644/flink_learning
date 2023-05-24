package com.litao.flink.utils;

import java.util.Objects;

// 基础Record pojo类,模仿kafka事件
// pojo类 : 1.公共类 2.拥有无参构造 3.属性公共 4.属性可序列化 (POJO类需要实现hashCode和equals方法)
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return Objects.equals(id, record.id) && Objects.equals(name, record.name) && Objects.equals(page, record.page) && Objects.equals(timestamp, record.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name, page, timestamp);
    }
}
