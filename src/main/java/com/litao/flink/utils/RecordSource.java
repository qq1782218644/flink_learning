package com.litao.flink.utils;

import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.HashMap;
import java.util.Random;

public class RecordSource implements SourceFunction<Record> {
    private Boolean flag = true;

    @Override
    public void run(SourceContext<Record> ctx) throws Exception {

        Random ran = new Random();
        HashMap<Long, String> users = new HashMap<Long, String>() {{
            put(1L, "Harden");
            put(2L, "James");
            put(3L, "Curry");
            put(4L, "Durant");
        }};
        String[] pages = {"home", "cart", "fav", "party", "search", "language", "start", "end"};

        while (flag) {
            Long id = ran.nextInt(4) + 1L;
            ctx.collect(
                    new Record(
                            id,
                            users.get(id),
                            pages[ran.nextInt(pages.length)],
                            Calendar.getInstance().getTimeInMillis()
                    )
            );
            Thread.sleep(2 * 1000L);
        }
    }

    @Override
    public void cancel() {
        flag = false;
    }
}
