/*
 * Copyright 2013-2020 Signal Messenger, LLC
 * SPDX-License-Identifier: AGPL-3.0-only
 */

package org.whispersystems.textsecuregcm.limits;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SharedMetricRegistries;
import io.lettuce.core.SetArgs;
import org.whispersystems.textsecuregcm.controllers.RateLimitExceededException;
import org.whispersystems.textsecuregcm.redis.FaultTolerantRedisCluster;
import org.whispersystems.textsecuregcm.util.Constants;

import java.time.Duration;

import static com.codahale.metrics.MetricRegistry.name;

/**
 * 增加了redis全局锁
 * key针对具体的用户，可以防止重复提交（因为获取不到锁），
 * 如果key相同（粗力度），则在并发限流时会因为获取不到锁而失败，
 * 因此适合细粒度的限流
 * 粗粒度限流可以借助 redis-cell 模块
 */
public class LockingRateLimiter extends RateLimiter {

  private final Meter meter;

  public LockingRateLimiter(FaultTolerantRedisCluster cacheCluster, String name, int bucketSize, double leakRatePerMinute) {
    super(cacheCluster, name, bucketSize, leakRatePerMinute);

    MetricRegistry metricRegistry = SharedMetricRegistries.getOrCreate(Constants.METRICS_NAME);
    //统计锁获取失败的次数
    this.meter = metricRegistry.meter(name(getClass(), name, "locked"));
  }

  @Override
  public void validate(String key, int amount) throws RateLimitExceededException {
    if (!acquireLock(key)) {
      meter.mark();
      throw new RateLimitExceededException("Locked", Duration.ZERO);
    }

    try {
      super.validate(key, amount);
    } finally {
      releaseLock(key);
    }
  }

  @Override
  public void validate(String key) throws RateLimitExceededException {
    validate(key, 1);
  }

  private void releaseLock(String key) {
    cacheCluster.useCluster(connection -> connection.sync().del(getLockName(key)));
  }

  private boolean acquireLock(String key) {
    return cacheCluster.withCluster(connection -> connection.sync().set(getLockName(key), "L", SetArgs.Builder.nx().ex(10))) != null;
  }

  private String getLockName(String key) {
    return "leaky_lock::" + name + "::" + key;
  }


}
