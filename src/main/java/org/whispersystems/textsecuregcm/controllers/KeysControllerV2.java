/**
 * Copyright (C) 2014 Open Whisper Systems
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
package org.whispersystems.textsecuregcm.controllers;


import com.codahale.metrics.annotation.Timed;
import com.google.common.base.Optional;
import org.whispersystems.textsecuregcm.entities.DeviceKey;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseItemV2;
import org.whispersystems.textsecuregcm.entities.PreKeyResponseV2;
import org.whispersystems.textsecuregcm.entities.PreKeyStateV2;
import org.whispersystems.textsecuregcm.entities.PreKeyV2;
import org.whispersystems.textsecuregcm.federation.FederatedClientManager;
import org.whispersystems.textsecuregcm.federation.NoSuchPeerException;
import org.whispersystems.textsecuregcm.limits.RateLimiters;
import org.whispersystems.textsecuregcm.storage.Account;
import org.whispersystems.textsecuregcm.storage.AccountsManager;
import org.whispersystems.textsecuregcm.storage.Device;
import org.whispersystems.textsecuregcm.storage.KeyRecord;
import org.whispersystems.textsecuregcm.storage.Keys;

import javax.validation.Valid;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.LinkedList;
import java.util.List;

import io.dropwizard.auth.Auth;

@Path("/v2/keys")
public class KeysControllerV2 extends KeysController {

  public KeysControllerV2(RateLimiters rateLimiters, Keys keys, AccountsManager accounts,
                          FederatedClientManager federatedClientManager)
  {
    super(rateLimiters, keys, accounts, federatedClientManager);
  }


  @Timed
  @PUT
  @Consumes(MediaType.APPLICATION_JSON)
  public void setKeys(@Auth Account account, @Valid PreKeyStateV2 preKeys)  {
    Device  device        = account.getAuthenticatedDevice().get();
    boolean updateAccount = false;

    if (!preKeys.getDeviceKey().equals(device.getDeviceKey())) {
      device.setDeviceKey(preKeys.getDeviceKey());
      updateAccount = true;
    }

    if (!preKeys.getIdentityKey().equals(account.getIdentityKey())) {
      account.setIdentityKey(preKeys.getIdentityKey());
      updateAccount = true;
    }

    if (updateAccount) {
      accounts.update(account);
    }

    keys.store(account.getNumber(), device.getId(), preKeys.getPreKeys(), preKeys.getLastResortKey());
  }

  @Timed
  @PUT
  @Path("/device")
  @Consumes(MediaType.APPLICATION_JSON)
  public void setDeviceKey(@Auth Account account, @Valid DeviceKey deviceKey) {
    Device device = account.getAuthenticatedDevice().get();
    device.setDeviceKey(deviceKey);
    accounts.update(account);
  }

  @Timed
  @GET
  @Path("/{number}/{device_id}")
  @Produces(MediaType.APPLICATION_JSON)
  public Optional<PreKeyResponseV2> getDeviceKey(@Auth                   Account account,
                                                 @PathParam("number")    String number,
                                                 @PathParam("device_id") String deviceId,
                                                 @QueryParam("relay")    Optional<String> relay)
      throws RateLimitExceededException
  {
    try {
      if (account.isRateLimited()) {
        rateLimiters.getPreKeysLimiter().validate(account.getNumber() +  "__" + number + "." + deviceId);
      }

      if (relay.isPresent()) {
        return federatedClientManager.getClient(relay.get()).getKeysV2(number, deviceId);
      }

      TargetKeys                 targetKeys  = getLocalKeys(number, deviceId);
      Account                    destination = targetKeys.getDestination();
      List<PreKeyResponseItemV2> devices     = new LinkedList<>();

      for (Device device : destination.getDevices()) {
        if (device.isActive() && (deviceId.equals("*") || device.getId() == Long.parseLong(deviceId))) {
          DeviceKey deviceKey = device.getDeviceKey();
          PreKeyV2  preKey    = null;

          if (targetKeys.getKeys().isPresent()) {
            for (KeyRecord keyRecord : targetKeys.getKeys().get()) {
              if (keyRecord.getDeviceId() == device.getId()) {
                preKey = new PreKeyV2(keyRecord.getKeyId(), keyRecord.getPublicKey());
              }
            }
          }

          if (deviceKey != null || preKey != null) {
            devices.add(new PreKeyResponseItemV2(device.getId(), device.getRegistrationId(), deviceKey, preKey));
          }
        }
      }

      if (devices.isEmpty()) return Optional.absent();
      else                   return Optional.of(new PreKeyResponseV2(destination.getIdentityKey(), devices));
    } catch (NoSuchPeerException | NoSuchUserException e) {
      throw new WebApplicationException(Response.status(404).build());
    }
  }



}