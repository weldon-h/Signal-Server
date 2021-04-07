/**
 * Copyright (C) 2013 Open WhisperSystems
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.whispersystems.textsecuregcm.limits;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/**
 * 漏斗模型
 */
public class LeakyBucket {

  /**
   * 漏斗大小
   */
  private final int    bucketSize;

  /**
   * 每毫秒漏出量
   */
  private final double leakRatePerMillis;

  /**
   * 漏斗剩余空间
   */
  private int spaceRemaining;

  /**
   * 最新更新时间
   */
  private long lastUpdateTimeMillis;

  /**
   * 初始化漏斗
   * 剩余容量=漏斗大小
   * @param bucketSize
   * @param leakRatePerMillis
   */
  public LeakyBucket(int bucketSize, double leakRatePerMillis) {
    this(bucketSize, leakRatePerMillis, bucketSize, System.currentTimeMillis());
  }

  private LeakyBucket(int bucketSize, double leakRatePerMillis, int spaceRemaining, long lastUpdateTimeMillis) {
    this.bucketSize           = bucketSize;
    this.leakRatePerMillis    = leakRatePerMillis;
    this.spaceRemaining       = spaceRemaining;
    this.lastUpdateTimeMillis = lastUpdateTimeMillis;
  }

  public boolean add(int amount) {
    this.spaceRemaining       = getUpdatedSpaceRemaining();
    this.lastUpdateTimeMillis = System.currentTimeMillis();

    if (this.spaceRemaining >= amount) {
      this.spaceRemaining -= amount;
      return true;
    } else {
      return false;
    }
  }

  /**
   * 获取漏斗剩余空间
   * @return
   */
  private int getUpdatedSpaceRemaining() {
    //上一次更新到现在的时间差值
    long elapsedTime = System.currentTimeMillis() - this.lastUpdateTimeMillis;

    return Math.min(this.bucketSize,
                    (int)Math.floor(this.spaceRemaining + (elapsedTime * this.leakRatePerMillis)));
  }

  public String serialize(ObjectMapper mapper) throws JsonProcessingException {
    return mapper.writeValueAsString(new LeakyBucketEntity(bucketSize, leakRatePerMillis, spaceRemaining, lastUpdateTimeMillis));
  }

  public static LeakyBucket fromSerialized(ObjectMapper mapper, String serialized) throws IOException {
    LeakyBucketEntity entity = mapper.readValue(serialized, LeakyBucketEntity.class);

    return new LeakyBucket(entity.bucketSize, entity.leakRatePerMillis,
                           entity.spaceRemaining, entity.lastUpdateTimeMillis);
  }

  private static class LeakyBucketEntity {
    @JsonProperty
    private int    bucketSize;

    @JsonProperty
    private double leakRatePerMillis;

    @JsonProperty
    private int    spaceRemaining;

    @JsonProperty
    private long   lastUpdateTimeMillis;

    public LeakyBucketEntity() {}

    private LeakyBucketEntity(int bucketSize, double leakRatePerMillis,
                              int spaceRemaining, long lastUpdateTimeMillis)
    {
      this.bucketSize           = bucketSize;
      this.leakRatePerMillis    = leakRatePerMillis;
      this.spaceRemaining       = spaceRemaining;
      this.lastUpdateTimeMillis = lastUpdateTimeMillis;
    }
  }
}