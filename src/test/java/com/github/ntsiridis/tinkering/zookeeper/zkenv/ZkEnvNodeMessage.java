package com.github.ntsiridis.tinkering.zookeeper.zkenv;

import java.util.Date;

/**
 * Environment message instances
 * monitored by the execution environment
 */
public class ZkEnvNodeMessage {
    //type of message
    private final ZkEnvMessageType type;
    //when the event is created
    private final Date createdAt;
    // optional message - description
    private String description;
    // optional data to carry with message
    private Object data;

    public ZkEnvNodeMessage(ZkEnvMessageType type) {
        if (type == null) {
            type = ZkEnvMessageType.UNDEFINED;
        }
        this.type = type;
        this.createdAt = new Date();
    }

    public ZkEnvMessageType getType() {
        return type;
    }

    public Date getCreatedAt() {
        return createdAt;
    }

    public String getDescription() {
        return description;
    }

    public ZkEnvNodeMessage setDescription(String description) {
        this.description = description;
        return this;
    }

    public Object getData() {
        return data;
    }

    public ZkEnvNodeMessage setData(Object data) {
        this.data = data;
        return this;
    }
}
