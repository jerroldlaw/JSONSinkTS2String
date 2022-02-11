/*
 * Copyright © 2019 Christopher Matta (chris.matta@gmail.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.jerroldlaw.kafka.connect.smt;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.transforms.Transformation;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.json.JSONObject;

import java.util.Map;

public abstract class JSONSinkTS2String<R extends ConnectRecord<R>> implements Transformation<R> {

  public static final String OVERVIEW_DOC =
    "Convert JSON to String with Sink timestamp.";

  private interface ConfigName {
    String PAYLOAD_FIELD = "payload.field";
  }

  public static final ConfigDef CONFIG_DEF = new ConfigDef()
          .define(ConfigName.PAYLOAD_FIELD, ConfigDef.Type.STRING, "", ConfigDef.Importance.HIGH,
                  "Field name for Payload key name");

  private static final String PURPOSE = "Convert JSON to String with Sink timestamp.";

  private Cache<Schema, Schema> schemaUpdateCache;
  private String payloadField;

  @Override
  public void configure(Map<String, ?> props) {
    final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
    payloadField = config.getString(ConfigName.PAYLOAD_FIELD);
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<Schema, Schema>(16));
  }

  @Override
  public R apply(R record) {
    JSONObject jo = new JSONObject(record.value().toString());

    if (payloadField == "") {
      jo.put("sink_ts", System.currentTimeMillis());
    }
    else {
      jo.getJSONObject(payloadField).put("sink_ts", System.currentTimeMillis());
    }

    return newRecord(record, null, jo.toString());
  }

  @Override
  public ConfigDef config() {
    return CONFIG_DEF;
  }

  @Override
  public void close() {
    schemaUpdateCache = null;
  }

  protected abstract Schema operatingSchema(R record);

  protected abstract Object operatingValue(R record);

  protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

  public static class Value<R extends ConnectRecord<R>> extends JSONSinkTS2String<R> {
    @Override
    protected Schema operatingSchema(R record) {
      return record.valueSchema();
    }

    @Override
    protected Object operatingValue(R record) {
      return record.value();
    }

    @Override
    protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
      return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
    }

  }
}


