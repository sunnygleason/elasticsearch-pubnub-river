/*
 * Licensed to ElasticSearch and Shay Banon under one or more contributor license agreements. See
 * the NOTICE file distributed with this work for additional information regarding copyright
 * ownership. ElasticSearch licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.elasticsearch.pubnub.river;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest.OpType;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.base.Throwables;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.river.AbstractRiverComponent;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiverSettings;
import org.json.JSONObject;

import com.pubnub.api.Callback;
import com.pubnub.api.Pubnub;
import com.pubnub.api.PubnubError;
import com.pubnub.api.PubnubException;

/**
 *
 */
public class PubNubRiver extends AbstractRiverComponent implements River {
  private final Client client;

  private final int bulkSize;
  private volatile boolean closed = false;
  private volatile Thread consumerThread;

  private final String pubnubPublishKey;
  private final String pubnubSubscribeKey;
  private final String pubnubSecretKey;
  private final String pubnubCipherKey;
  private final boolean pubnubUseSsl;
  private final String pubnubChannels;
  private final String pubnubUuid;

  private volatile Pubnub pubnub;

  private BlockingQueue<JSONObject> queue = new ArrayBlockingQueue<JSONObject>(100000);

  @SuppressWarnings({"unchecked"})
  @Inject
  public PubNubRiver(RiverName riverName, RiverSettings settings, Client client) {
    super(riverName, settings);
    this.client = client;

    if (settings.settings().containsKey("pubnub")) {
      Map<String, Object> pubnubSettings = (Map<String, Object>) settings.settings().get("pubnub");

      pubnubPublishKey = XContentMapValues.nodeStringValue(pubnubSettings.get("publishKey"), null);
      pubnubSubscribeKey =
          XContentMapValues.nodeStringValue(pubnubSettings.get("subscribeKey"), null);
      pubnubSecretKey = XContentMapValues.nodeStringValue(pubnubSettings.get("secretKey"), null);
      pubnubCipherKey = XContentMapValues.nodeStringValue(pubnubSettings.get("cipherKey"), null);
      pubnubUseSsl = XContentMapValues.nodeBooleanValue(pubnubSettings.get("useSsl"), true);
      pubnubChannels = XContentMapValues.nodeStringValue(pubnubSettings.get("channels"), null);
      pubnubUuid = XContentMapValues.nodeStringValue(pubnubSettings.get("uuid"), null);
    } else {
      throw new RuntimeException("Unable to initialize River: 'pubnub' config key missing");
    }

    if (settings.settings().containsKey("index")) {
      Map<String, Object> indexSettings = (Map<String, Object>) settings.settings().get("index");
      bulkSize = XContentMapValues.nodeIntegerValue(indexSettings.get("bulk_size"), 100);
    } else {
      bulkSize = 100;
    }
  }

  @Override
  public void start() {
    if (this.pubnubCipherKey != null && this.pubnubCipherKey.length() > 0) {
      this.pubnub =
          new Pubnub(pubnubPublishKey, pubnubSubscribeKey, pubnubSecretKey, pubnubCipherKey,
              pubnubUseSsl);
    } else {
      this.pubnub = new Pubnub(pubnubPublishKey, pubnubSubscribeKey, pubnubUseSsl);
    }

    if (this.pubnubUuid != null && this.pubnubUuid.length() > 0) {
      this.pubnub.setUUID(this.pubnubUuid);
    }

    logger.info("creating pubnub river, uuid={}, channels={}", this.pubnub.getUUID(),
        this.pubnubChannels);

    String[] channels = this.pubnubChannels.split(",");

    try {
      this.pubnub.subscribe(channels, new ElasticSearchIndexingCallback());
    } catch (PubnubException pubnubEx) {
      throw Throwables.propagate(pubnubEx);
    }

    consumerThread =
        EsExecutors.daemonThreadFactory(settings.globalSettings(), "pubnub_river:consumer")
            .newThread(new Consumer());

    consumerThread.start();
  }

  @Override
  public void close() {
    if (closed) {
      return;
    }

    logger.info("closing pubnub river");
    closed = true;
    consumerThread.interrupt();
  }

  private class ElasticSearchIndexingCallback extends Callback {
    @Override
    public void connectCallback(String channel, Object message) {
      logger.info("pubnub service connected");
    }

    @Override
    public void disconnectCallback(String channel, Object message) {
      logger.info("pubnub service disconnected");
    }

    @Override
    public void reconnectCallback(String channel, Object message) {
      logger.info("pubnub service reconnected");
    }

    @Override
    public void errorCallback(String channel, PubnubError error) {
      logger.warn("pubnub service error: {}", error);
    }

    @Override
    public void successCallback(String channel, Object message) {
      logger.trace("pubnub message received: channel={}, message={}", channel, message);
      queue.add((JSONObject) message);
    }

    @Override
    public void successCallback(String channel, Object message, String timeToken) {
      // not used
    }
  }

  private class Consumer implements Runnable {
    @Override
    public void run() {
      logger.info("consumer thread starting...");
      while (true) {
        if (closed) {
          break;
        }

        if (queue.isEmpty()) {
          try {
            logger.trace("no messages found in queue, sleeping 500ms");
            Thread.sleep(500);
          } catch (InterruptedException ex) {
            // ignore, if we are closing, we will exit later
          }

          continue;
        }

        List<JSONObject> todo = new ArrayList<JSONObject>(bulkSize);
        queue.drainTo(todo, bulkSize);

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        for (JSONObject message : todo) {
          try {
            String action = (String) message.get("action");

            String index = (String) message.get("index");
            String type = (String) message.get("type");
            String id = (String) message.get("id");

            Long version = Long.parseLong((String) message.get("version"));

            JSONObject theValue = message.getJSONObject("_source");

            if ("create".equals(action) || "update".equals(action)) {
              IndexRequestBuilder theReq = new IndexRequestBuilder(client);

              if ("create".equals(action)) {
                theReq.setOpType(OpType.CREATE);
              } else {
                theReq.setOpType(OpType.INDEX);
              }

              theReq.setIndex(index);
              theReq.setId(id);
              theReq.setType(type);
              theReq.setSource(theValue.toString());

              theReq.setVersionType(VersionType.EXTERNAL);
              theReq.setVersion(version);

              bulkRequestBuilder.add(theReq);
            } else if ("delete".equals(action)) {
              bulkRequestBuilder.add(new DeleteRequest(index, type, id));
            } else {
              throw new IllegalArgumentException(message.toString());
            }
          } catch (Exception e) {
            logger.warn("failed to parse request", e);
            continue;
          }
        }

        if (logger.isTraceEnabled()) {
          logger.trace("executing bulk with [{}] actions", bulkRequestBuilder.numberOfActions());
        }

        bulkRequestBuilder.execute(new ActionListener<BulkResponse>() {
          @Override
          public void onResponse(BulkResponse response) {
            if (response.hasFailures()) {
              for (BulkItemResponse item : response.getItems()) {
                logger.info("execution error {}", item.getFailureMessage());
              }
            }
          }

          @Override
          public void onFailure(Throwable e) {
            logger.warn("failed to execute bulk", e);
          }
        });

        logger.info("indexer updated {} documents", bulkRequestBuilder.numberOfActions());
      }
      logger.info("consumer thread exiting...");
    }
  }
}
