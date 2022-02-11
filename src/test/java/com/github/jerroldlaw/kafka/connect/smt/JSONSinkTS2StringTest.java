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

import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class JSONSinkTS2StringTest {

  private JSONSinkTS2String<SourceRecord> xform = new JSONSinkTS2String.Value<>();

  @After
  public void tearDown() throws Exception {
    xform.close();
  }

  @Test
  public void JSONSinkTS2String() {
    final Map<String, Object> props = new HashMap<>();

    xform.configure(props);

    final SourceRecord record = new SourceRecord(null, null, "test", 0,
      null, "{\"schema\":{\"type\":\"struct\",\"fields\":[{\"type\":\"int64\",\"optional\":false,\"field\":\"ID\"},{\"type\":\"string\",\"optional\":true,\"field\":\"FIRST_NAME\"},{\"type\":\"string\",\"optional\":true,\"field\":\"LAST_NAME\"},{\"type\":\"string\",\"optional\":true,\"field\":\"EMAIL\"},{\"type\":\"string\",\"optional\":true,\"field\":\"GENDER\"},{\"type\":\"string\",\"optional\":true,\"field\":\"CLUB_STATUS\"},{\"type\":\"string\",\"optional\":true,\"field\":\"COMMENTS\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"version\":1,\"field\":\"CREATE_TS\"},{\"type\":\"int64\",\"optional\":true,\"name\":\"org.apache.kafka.connect.data.Timestamp\",\"version\":1,\"field\":\"UPDATE_TS\"},{\"type\":\"string\",\"optional\":true,\"field\":\"COUNTRY\"},{\"type\":\"string\",\"optional\":true,\"field\":\"table\"},{\"type\":\"string\",\"optional\":true,\"field\":\"scn\"},{\"type\":\"string\",\"optional\":true,\"field\":\"op_type\"},{\"type\":\"string\",\"optional\":true,\"field\":\"op_ts\"},{\"type\":\"string\",\"optional\":true,\"field\":\"current_ts\"},{\"type\":\"string\",\"optional\":true,\"field\":\"row_id\"},{\"type\":\"string\",\"optional\":true,\"field\":\"username\"}],\"optional\":false},\"payload\":{\"ID\":4,\"FIRST_NAME\":\"Hashim\",\"LAST_NAME\":\"Rumke\",\"EMAIL\":\"hrumke3@sohu.com\",\"GENDER\":\"Male\",\"CLUB_STATUS\":\"platinum\",\"COMMENTS\":\"Self-enabling 24/7 firmware\",\"CREATE_TS\":1642829835586,\"UPDATE_TS\":1642829835000,\"COUNTRY\":null,\"table\":\"ORCLCDB.C##MYUSER.CUSTOMERS\",\"scn\":\"2289579\",\"op_type\":\"R\",\"op_ts\":null,\"current_ts\":\"1642834602453\",\"row_id\":null,\"username\":null}}");

    final SourceRecord transformedRecord = xform.apply(record);

    System.out.println(transformedRecord.value().toString());
    System.out.println(transformedRecord.value() instanceof String);
  }
}