/**
 * Copyright (c) 2012 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

/**
 * Redis client binding for YCSB.
 *
 * All YCSB records are mapped to a Redis *hash field*.  For scanning
 * operations, all keys are saved (by an arbitrary hash) in a sorted set.
 */

package com.yahoo.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.Client;
import com.yahoo.ycsb.workloads.GDPRWorkload;
import java.util.concurrent.atomic.AtomicLong;

import java.io.IOException;
import java.io.File;
import java.io.FileWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

/**
 * YCSB binding for a dummy client for workload tracing.
 */
public class TracerClient extends DB {

  public static final String TRACER_FILE_PROPERTY = "tracer.file";

  public static final String INDEX_KEY = "_indices";

  public static final String MOCK_VALUES_PROPERTY = "mockvalues";

  private static boolean mockvalues = true;

  private static FileWriter fileWriter;

  // Store the counts as instance variables
  private static int recordCount = 100000;      // Number of KV pairs
  private static int userCount = 16;
  private static int purposeCount = 64;
  private static int objStart = 25;      // Starting objection number
  private static int objCount = 64;      // Number of objections
  private static AtomicLong operationCounter = new AtomicLong(0);

  // Field indices from the workload
  private static final int FIELD_PUR = 0;   // Purpose
  private static final int FIELD_TTL = 1;   // TTL
  private static final int FIELD_USR = 2;   // User
  private static final int FIELD_OBJ = 3;   // Objection
  private static final int FIELD_DEC = 4;   // Declaration
  private static final int FIELD_ACL = 5;   // ACL
  private static final int FIELD_SHR = 6;   // Share
  private static final int FIELD_SRC = 7;   // Source
  private static final int FIELD_LOG = 8;   // Log
  private static final int FIELD_DATA = 9;  // Data
  
  private static String[] fieldnames = {
      "PUR", "TTL", "USR", "OBJ", "DEC", "ACL", "SHR", "SRC", "LOG", "Data"
  };

  public void init() {
    Properties props = getProperties();
    String tracerFilePath = props.getProperty(TRACER_FILE_PROPERTY);
    System.out.println(tracerFilePath);

    // Read user and purpose counts using GDPRWorkload constants
    recordCount = Integer.parseInt(props.getProperty(
        Client.RECORD_COUNT_PROPERTY, 
        Client.DEFAULT_RECORD_COUNT));
    userCount = Integer.parseInt(props.getProperty(
        GDPRWorkload.USER_COUNT_PROPERTY, 
        GDPRWorkload.USER_COUNT_PROPERTY_DEFAULT));
    purposeCount = Integer.parseInt(props.getProperty(
        GDPRWorkload.PURPOSE_COUNT_PROPERTY, 
        GDPRWorkload.PURPOSE_COUNT_PROPERTY_DEFAULT));
    objStart = Integer.parseInt(props.getProperty(
        GDPRWorkload.OBJECTIVE_START_PROPERTY,
        GDPRWorkload.OBJECTIVE_START_PROPERTY_DEFAULT));
    objCount = Integer.parseInt(props.getProperty(
        GDPRWorkload.OBJECTIVE_COUNT_PROPERTY,
        GDPRWorkload.OBJECTIVE_COUNT_PROPERTY_DEFAULT));
    
    System.out.println("TracerClient initialized with userCount=" + userCount + 
                       ", purposeCount=" + purposeCount +
                       ", objStart=" + objStart +
                       ", objCount=" + objCount);

    // Init the trace file
    File traceFile = new File(tracerFilePath);
    try {
      if(!traceFile.exists()){
        traceFile.createNewFile();
      }
    } catch (IOException e) {
      e.getStackTrace();
    }

    // Init the trace stream writer
    try {
      setFileWriter(new FileWriter(traceFile.getPath(), true));
    } catch (IOException e) {
      e.getStackTrace();
    }

    boolean mockvaluesCfg = Boolean.parseBoolean(props.getProperty(MOCK_VALUES_PROPERTY));
    setMockValues(mockvaluesCfg);
  }

  // Close the trace file
  public void cleanup() {
    try {
      getFileWriter().close();
    } catch (IOException e) {
      e.getStackTrace();
    }
  }

  // Accessors functions for the fileWriter
  public FileWriter getFileWriter() {
    return this.fileWriter;
  }

  public void setFileWriter(FileWriter newFileWriter) {
    this.fileWriter = newFileWriter;
  }

  // Accessors functions for the mockvaues
  public boolean getMockValues() {
    return this.mockvalues;
  }

  public void setMockValues(boolean newMockValues) {
    this.mockvalues = newMockValues;
  }

  public String mergeValues(Map<String, ByteIterator> values) {
    if (mockvalues) {
      return "VAL";
    }
    String val = "";
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      val += entry.getValue().toString();
    }
    return val;
  }

  public String getValData(Map<String, ByteIterator> values) {
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String predType = entry.getKey().toString();
      if (predType == "Data") {
        if (mockvalues) {
          return "VAL";
        } else {
          return entry.getValue().toString();
        }
      }
    }
    return "";
  }

  public String fieldToSetPred(String predType, String value) {
    String val = "";
    switch(predType) {
    case "DEC": // 
      val += "DEC";
      break;
    case "USR": // data owner
      val += setSessPred(value);
      break;
    case "SRC": // data source
      val += setOrigPred(value);
      break;
    case "OBJ": // objections
      val += setObjPred(value);
      break;
    case "LOG": // monitor
      val += setMonitorPred(value);
      break;
    case "ACL": // Access Control
      val += "ACL";
      break;
    case "Data": // the value
      break;
    case "PUR": // data purpose
      val += setPurPred(value);
      break;
    case "SHR": // data sharing
      val += setSharePred(value);
      break;
    case "TTL": // data time to live
      val += setExpPred(value);
      break;
    default: // invalid
      val += "error";
      break;  
    }
    return val;
  }

  public String buildSetPredicates(Map<String, ByteIterator> values) {
    String val = "";
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String predType = entry.getKey().toString();
      String value = entry.getValue().toString();
      String pred = fieldToSetPred(predType, value);
      val += (pred != "") ? pred + "&" : "";
    }
    return val.substring(0, val.length() - 1);
  }

  public String fieldToCondPred(String predType, String value) {
    String val = "";
    switch(predType) {
    case "DEC": // 
      val += "DEC";
      break;
    case "USR": // data owner
      val += sessPred(value);
      break;
    case "SRC": // data source
      val += origPred(value);
      break;
    case "OBJ": // objections
      val += objPred(value);
      break;
    case "LOG": // 
      val += monitorPred(value);
      break;
    case "ACL": //
      val += "ACL";
      break;
    case "Data": // the value
      break;
    case "PUR": // data purpose
      val += purPred(value);
      break;
    case "SHR": // data sharing
      val += sharePred(value);
      break;
    case "TTL": // data time to live
      val += expPred(value);
      break;
    default: // invalid
      val += "error";
      break;  
    }
    return val;
  }

  public String buildCondPredicates(Map<String, ByteIterator> values) {
    String val = "";
    for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
      String predType = entry.getKey().toString();
      String value = entry.getValue().toString();
      String pred = fieldToCondPred(predType, value);
      val += (pred != "") ? pred + "&" : "";
    }
    return val.substring(0, val.length() - 1);
  }

  public String sessPred(String cond) {  
    String pred = "sessionKeyIs(\"" + cond + "\")";
    return pred;
  }

  public String expPred(String cond) {
    String pred = "objExpIs(\"" + cond + "\")";
    return pred;
  }

  public String purPred(String cond) {
    String pred = "objPurIs(\"" + cond + "\")";
    return pred;
  }

  public String origPred(String cond) {
    String pred = "objOrigIs(\"" + cond + "\")";
    return pred;
  }

  public String sharePred(String cond) {
    String pred = "objShareIs(\"" + cond + "\")";
    return pred;
  }

  public String objPred(String cond) {
    String pred = "objObjectionsIs(\"" + cond + "\")";
    return pred;
  }

  public String monitorPred(String cond) {
    String pred = "monitor(\"" + cond + "\")";
    return pred;
  }

  public String setSessPred(String cond) {  
    String pred = "sessionKey(\"" + cond + "\")";
    return pred;
  }

  public String setExpPred(String cond) {
    String pred = "objExp(\"" + cond + "\")";
    return pred;
  }

  public String setPurPred(String cond) {
    String pred = "objPur(\"" + cond + "\")";
    return pred;
  }

  public String setOrigPred(String cond) {
    String pred = "objOrig(\"" + cond + "\")";
    return pred;
  }

  public String setSharePred(String cond) {
    String pred = "objShare(\"" + cond + "\")";
    return pred;
  }

  public String setObjPred(String cond) {
    String pred = "objObjections(\"" + cond + "\")";
    return pred;
  }

  public String setMonitorPred(String cond) {
    String pred = "monitor(\"" + cond + "\")";
    return pred;
  }

  // Helper method to extract user from key (deterministic)
  private String extractUserFromKey(String key) {
    try {
      long keyNum = Long.parseLong(key.replaceAll("[^0-9]", ""));
      // Direct mapping from load phase: keyN -> userN
      return "user" + (keyNum % userCount);
    } catch (Exception e) {
      return "user0";
    }
  }

  // Helper method to extract purpose from key (deterministic)
  private String extractPurposeFromKey(String key) {
    try {
      long keyNum = Long.parseLong(key.replaceAll("[^0-9]", ""));
      // Direct mapping from load phase: keyN -> purposeN
      return "purpose" + (keyNum % purposeCount);
    } catch (Exception e) {
      return "purpose0";
    }
  }

  // Helper to find a user based on condition and field type
  private String extractUserFromCond(String cond, int fieldnum, int selector) {
    long seqNum = Long.parseLong(cond.replaceAll("[^0-9]", ""));
    if (fieldnum == FIELD_PUR) {
      long userId = ((((long)purposeCount * selector) + seqNum) % recordCount) % userCount;
      return "user" + userId;
    }
    else if (fieldnum == FIELD_OBJ) {
      seqNum -= objStart; // remove the offset from the objections
      long userId = ((((long)objCount * selector) + seqNum) % recordCount) % userCount;
      return "user" + userId;
    }
    else if (fieldnum == FIELD_USR) {
      return cond;
    }
    return "user0";
  }


  // Helper to find a purpose based on condition and field type
  private String extractPurposeFromCond(String cond, int fieldnum, int selector) {
    long seqNum = Long.parseLong(cond.replaceAll("[^0-9]", ""));
    if (fieldnum == FIELD_PUR) {
      return cond;
    }
    else if (fieldnum == FIELD_OBJ) {
      seqNum -= objStart; // remove the offset from the objections
      long purposeId = ((((long)objCount * selector) + seqNum) % recordCount) % purposeCount;
      return "purpose" + purposeId;
    }
    else if (fieldnum == FIELD_USR) {
      long purposeId = ((((long)userCount * selector) + seqNum) % recordCount) % purposeCount;
      return "purpose" + purposeId;
    }
    return "purpose0";
  }

  /*
   * Calculate a hash for a key to store it in an index. The actual return value
   * of this function is not interesting -- it primarily needs to be fast and
   * scattered along the whole space of doubles. In a real world scenario one
   * would probably use the ASCII values of the keys.
   */
  private double hash(String key) {
    return key.hashCode();
  }

  // XXX jedis.select(int index) to switch to `table`

  @Override
  public Status read(String table, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    
    // result holds conditions for the "get" operation
    String query;
    // Check if key ends with "_meta_only" suffix
    String suffix = "_meta_only";

    if (key.endsWith(suffix)) {
      // Extract base key and generate get_meta query
      String baseKey = key.substring(0, key.length() - suffix.length());
      if (result.isEmpty()) {
        /* should never be executed */
        query = "query(GET(\"" + baseKey + "\",\"meta_only\"))\n";
      } else {
        /* GDPR workload GET */
        /* always specify session key and allowed valid purpose based on the key */
        String sessionPred = "sessionKey(\"" + extractUserFromKey(baseKey) + "\")";
        String purposePred = "objPurIs(\"" + extractPurposeFromKey(baseKey) + "\")";
        query = "query(GET(\"" + baseKey + "\",\"meta_only\"))&" + sessionPred + "&" + purposePred + "&" + buildCondPredicates(result) + "\n";
      }
    }
    else {
      // Regular get query
      if (result.isEmpty()) {
        String sessionPred = "sessionKey(\"" + extractUserFromKey(key) + "\")";
        query = "query(GET(\"" + key + "\"))" + "&" + sessionPred + "\n";
      } else {
        /* GDPR workload GET */
        /* always specify session key based on the key - purpose is provided by the db.read of the GDPRWorkload.java */
        String sessionPred = "sessionKey(\"" + extractUserFromKey(key) + "\")";
        query = "query(GET(\"" + key + "\"))&" + sessionPred + "&" + buildCondPredicates(result) + "\n";
      }
    }

    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status readMeta(String table, int fieldnum, String cond, String keymatch,
      Vector<HashMap<String, ByteIterator>> result) {
    
    String query;
    String suffix = "_only_meta";

    /* GDPR workload GETM */
    /* always specify session key based on the condition */
    int selector = (int)operationCounter.incrementAndGet();
    String sessionPred = "sessionKey(\"" + extractUserFromCond(cond, fieldnum, selector) + "\")";
    String purposePred = "";
    if (fieldnum != FIELD_PUR) {
      // if we do not have a purpose condition (e.g., we have user, objection), add a matching purpose so that the query succeeds
      purposePred = "objPurIs(\"" + extractPurposeFromCond(cond, fieldnum, selector) + "\")&";
    }

    if (keymatch.endsWith(suffix)) {
      // keymatch ends with "_only_meta"
      String baseKey = keymatch.substring(0, keymatch.length() - suffix.length());
      query = "query(GETM(\"" + baseKey + "\",\"metadata\"))&" + sessionPred + "&" + purposePred + fieldToCondPred(fieldnames[fieldnum], cond) + "\n";
    } else {
      // keymatch does not end with "_only_meta"
      query = "query(GETM(\"" + keymatch + "\",\"data\"))&" + sessionPred + "&" + purposePred + fieldToCondPred(fieldnames[fieldnum], cond) + "\n";
    }
    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status insert(String table, String key,
      Map<String, ByteIterator> values) {
    
    String query = "";
    if (key.startsWith("user")) {
      String sessionPred = "sessionKey(\"" + extractUserFromKey(key) + "\")";
      query = "query(PUT(\"" + key + "\",\"" + mergeValues(values) + "\"))" + "&" + sessionPred + "\n";
    } else if (key.startsWith("key")) {
      query = "query(PUT(\"" + key + "\",\"" + getValData(values) + "\"))&" + buildSetPredicates(values) + "\n";
    }
    
    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status insertTTL(String table, String key,
      Map<String, ByteIterator> values, int ttl) {

    String query = "";
    if (key.startsWith("user")) {
      String sessionPred = "sessionKey(\"" + extractUserFromKey(key) + "\")";
      query = "query(PUT(\"" + key + "\",\"" + mergeValues(values) + "\"))" + "&" + sessionPred + "\n";
    } else if (key.startsWith("key")) {
      query = "query(PUT(\"" + key + "\",\"" + getValData(values) + "\"))&" + buildSetPredicates(values) + "\n";
    }

    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    /* GDPR workload delete */
    /* always specify session key and allowed valid purpose based on the key */
    String sessionPred = "sessionKey(\"" + extractUserFromKey(key) + "\")";
    String purposePred = "objPurIs(\"" + extractPurposeFromKey(key) + "\")";
    // String query = "query(DELETE(\"" + key + "\"))\n";
    String query = "query(DELETE(\"" + key + "\"))&" + sessionPred + "&" + purposePred + "\n";
    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status deleteMeta(String table, int fieldnum, String condition, String keymatch) {
    /* GDPR workload deletem */
    /* always specify session key based on the condition */
    int selector = (int)operationCounter.incrementAndGet();
    String sessionPred = "sessionKey(\"" + extractUserFromCond(condition, fieldnum, selector) + "\")";
    String purposePred = "";
    if (fieldnum != FIELD_PUR) {
      // if we do not have a purpose condition (e.g., we have user, objection), add a matching purpose so that the query succeeds
      purposePred = "objPurIs(\"" + extractPurposeFromCond(condition, fieldnum, selector) + "\")&";
    }
    String query = "query(DELETEM(\"" + keymatch + "\"))&" + sessionPred + "&" + purposePred + fieldToCondPred(fieldnames[fieldnum], condition) + "\n";

    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }
    
    return Status.OK;    
  }

  @Override
  public Status update(String table, String key,
      Map<String, ByteIterator> values) {
    
    String query = "";
    String suffix = "_meta_only";
    if (key.endsWith(suffix)) {
      // Metadata-only update
      String baseKey = key.substring(0, key.length() - suffix.length());
      /* always specify session key based on the condition */
      String sessionPred = "sessionKey(\"" + extractUserFromKey(baseKey) + "\")";
      String purposePred = "objPurIs(\"" + extractPurposeFromKey(baseKey) + "\")";
      query = "query(PUT(\"" + baseKey + "\",\"meta_only\"))&" + sessionPred + "&" + purposePred + "&" + buildSetPredicates(values) + "\n";
    } else if (key.startsWith("user")) {
      // Regular put query for GDPR workloads
      /* always specify session key based on the condition */
      String sessionPred = "sessionKey(\"" + extractUserFromKey(key) + "\")";
      query = "query(PUT(\"" + key + "\",\"" + mergeValues(values) + "\"))" + "&" + sessionPred + "\n";
    } else if (key.startsWith("key")) {
      // Regular put query
      query = "query(PUT(\"" + key + "\",\"" + getValData(values) + "\"))&" + buildSetPredicates(values) + "\n";
    }

    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }
    return Status.OK;
  }

  @Override
  public Status updateMeta(String table, int fieldnum, String condition, 
      String keymatch, String newfieldname, String newmetadatavalue) {
    /* GDPR workload PUTM */
    /* always specify session key based on the condition */
    int selector = (int)operationCounter.incrementAndGet();
    String sessionPred = "sessionKey(\"" + extractUserFromCond(condition, fieldnum, selector) + "\")";
    String purposePred = "";
    if (fieldnum != FIELD_PUR) {
      // if we do not have a purpose condition (e.g., we have user, objection), add a matching purpose so that the query succeeds
      purposePred = "objPurIs(\"" + extractPurposeFromCond(condition, fieldnum, selector) + "\")&";
    }
    String query = "query(PUTM(\"" + keymatch + "\"))&" + sessionPred + "&" + purposePred + fieldToCondPred(fieldnames[fieldnum], condition) +
                    "&" + fieldToSetPred(newfieldname, newmetadatavalue) + "\n";
    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
      Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    String query = "query(SCAN(\"" + startkey + "\",\"" + recordcount + "\"))\n";
    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }
    return Status.OK;
  }

  @Override
  public Status verifyTTL(String table, long recordcount) {
    // String query = "verifyTTL " + recordcount + "\n";
    // try {
    //   getFileWriter().write(query);
    // } catch (IOException e) {
    //   e.getStackTrace();
    // }
    return Status.OK;
  }

  @Override
  public Status readLog(String table, int logcount) {
    String query = "query(getLogs(\"" + logcount + "\"))\n";
    try {
      getFileWriter().write(query);
    } catch (IOException e) {
      e.getStackTrace();
    }
    return Status.OK;
  }
}
