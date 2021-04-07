/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */
package org.whispersystems.textsecuregcm.storage;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationRequest;
import org.whispersystems.textsecuregcm.entities.DirectoryReconciliationResponse;
import org.whispersystems.textsecuregcm.util.Constants;

import javax.ws.rs.ProcessingException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.codahale.metrics.MetricRegistry.name;

public class DirectoryReconciler extends AccountDatabaseCrawlerListener {

  private static final Logger logger = LoggerFactory.getLogger(DirectoryReconciler.class);
  private static final MetricRegistry metricRegistry      = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);

  private final DirectoryReconciliationClient reconciliationClient;
  private final Timer                         sendChunkTimer;
  private final Meter                         sendChunkErrorMeter;

  public DirectoryReconciler(String name, DirectoryReconciliationClient reconciliationClient) {
    this.reconciliationClient = reconciliationClient;
    sendChunkTimer            = metricRegistry.timer(name(DirectoryReconciler.class, name, "sendChunk"));
    sendChunkErrorMeter       = metricRegistry.meter(name(DirectoryReconciler.class, name, "sendChunkError"));
  }

  @Override
  public void onCrawlStart() { }

  @Override
  public void onCrawlEnd(Optional<UUID> fromUuid) {
    DirectoryReconciliationRequest  request  = new DirectoryReconciliationRequest(fromUuid.orElse(null), null, Collections.emptyList());
    sendChunk(request);
  }

  @Override
  protected void onCrawlChunk(Optional<UUID> fromUuid, List<Account> chunkAccounts) throws AccountDatabaseCrawlerRestartException {
    DirectoryReconciliationRequest  request  = createChunkRequest(fromUuid, chunkAccounts);
    DirectoryReconciliationResponse response = sendChunk(request);
    if (response.getStatus() == DirectoryReconciliationResponse.Status.MISSING) {
      throw new AccountDatabaseCrawlerRestartException("directory reconciler missing");
    }
  }

  @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
  private DirectoryReconciliationRequest createChunkRequest(Optional<UUID> fromUuid, List<Account> accounts) {
    List<DirectoryReconciliationRequest.User> users = new ArrayList<>(accounts.size());
    for (Account account : accounts) {
      if (account.isEnabled() && account.isDiscoverableByPhoneNumber()) {
        users.add(new DirectoryReconciliationRequest.User(account.getUuid(), account.getNumber()));
      }
    }

    Optional<UUID> toUuid = Optional.empty();

    if (!accounts.isEmpty()) {
      toUuid = Optional.of(accounts.get(accounts.size() - 1).getUuid());
    }

    return new DirectoryReconciliationRequest(fromUuid.orElse(null), toUuid.orElse(null), users);
  }

  private DirectoryReconciliationResponse sendChunk(DirectoryReconciliationRequest request) {
    try (Timer.Context timer = sendChunkTimer.time()) {
      DirectoryReconciliationResponse response = reconciliationClient.sendChunk(request);
      if (response.getStatus() != DirectoryReconciliationResponse.Status.OK) {
        sendChunkErrorMeter.mark();
        logger.warn("reconciliation error: " + response.getStatus());
      }
      return response;
    } catch (ProcessingException ex) {
      sendChunkErrorMeter.mark();
      logger.warn("request error: ", ex);
      throw new ProcessingException(ex);
    }
  }

}
