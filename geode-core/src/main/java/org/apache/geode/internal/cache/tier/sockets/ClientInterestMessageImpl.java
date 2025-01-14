/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import org.apache.geode.DataSerializer;
import org.apache.geode.distributed.DistributedSystem;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.EventID;
import org.apache.geode.internal.cache.tier.InterestType;
import org.apache.geode.internal.cache.tier.MessageType;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.internal.serialization.Version;

/**
 * Class <code>ClientInterestMessageImpl</code> represents an update to the a client's interest
 * registrations made by the server on behalf of the client.
 *
 *
 * @since GemFire 5.6
 */
public class ClientInterestMessageImpl implements ClientMessage {

  private static final long serialVersionUID = -797925585426839008L;

  /**
   * This <code>ClientMessage</code>'s <code>EventID</code>
   */
  private EventID eventId;

  /**
   * This <code>ClientMessage</code>'s key
   */
  private Object keyOfInterest;

  /**
   * This <code>ClientMessage</code>'s region name
   */
  private String regionName;

  /**
   * Whether the interest represented by this <code>ClientMessage</code> is durable
   */
  private boolean isDurable;

  /**
   * Whether the create or update events for this <code>ClientMessage</code> is sent as an
   * invalidate
   *
   * @since GemFire 6.0.3
   */
  private boolean forUpdatesAsInvalidates;

  /**
   * This <code>ClientMessage</code>'s interest type (key or regex)
   */
  private int interestType;

  /**
   * This <code>ClientMessage</code>'s interest result policy (none, key, key-value)
   */
  private byte interestResultPolicy;

  /**
   * This <code>ClientMessage</code>'s action (add or remove interest)
   */
  private byte action;

  /**
   * A byte representing a register interest message
   */
  protected static final byte REGISTER = (byte) 0;

  /**
   * A byte representing an unregister interest message
   */
  protected static final byte UNREGISTER = (byte) 1;

  /**
   *
   * @param eventId The EventID of this message
   * @param regionName The name of the region whose interest is changing
   * @param keyOfInterest The key in the region whose interest is changing
   * @param action The action (add or remove interest)
   */
  public ClientInterestMessageImpl(EventID eventId, String regionName, Object keyOfInterest,
      int interestType, byte interestResultPolicy, boolean isDurable,
      boolean sendUpdatesAsInvalidates, byte action) {
    this.eventId = eventId;
    this.regionName = regionName;
    this.keyOfInterest = keyOfInterest;
    this.interestType = interestType;
    this.interestResultPolicy = interestResultPolicy;
    this.isDurable = isDurable;
    this.forUpdatesAsInvalidates = sendUpdatesAsInvalidates;
    this.action = action;
  }

  public ClientInterestMessageImpl(DistributedSystem distributedSystem,
      ClientInterestMessageImpl message, Object keyOfInterest) {
    this.eventId = new EventID(distributedSystem);
    this.regionName = message.regionName;
    this.keyOfInterest = keyOfInterest;
    this.interestType = message.interestType;
    this.interestResultPolicy = message.interestResultPolicy;
    this.isDurable = message.isDurable;
    this.forUpdatesAsInvalidates = message.forUpdatesAsInvalidates;
    this.action = message.action;
  }

  /**
   * Default constructor.
   */
  public ClientInterestMessageImpl() {}

  @Override
  public Message getMessage(CacheClientProxy proxy, boolean notify) throws IOException {
    Version clientVersion = proxy.getVersion();
    Message message = null;
    if (clientVersion.compareTo(Version.GFE_57) >= 0) {
      message = getGFEMessage();
    } else {
      throw new IOException(
          "Unsupported client version for server-to-client message creation: " + clientVersion);
    }

    return message;
  }

  protected Message getGFEMessage() throws IOException {
    Message message = new Message(isRegister() ? 7 : 6, Version.CURRENT);
    message.setTransactionId(0);

    // Set the message type
    switch (this.action) {
      case REGISTER:
        message.setMessageType(MessageType.CLIENT_REGISTER_INTEREST);
        break;
      case UNREGISTER:
        message.setMessageType(MessageType.CLIENT_UNREGISTER_INTEREST);
        break;
      default:
        String s = "Unknown action: " + this.action;
        throw new IOException(s);
    }

    // Add the region name
    message.addStringPart(this.regionName, true);

    // Add the key
    message.addStringOrObjPart(this.keyOfInterest);

    // Add the interest type
    message.addObjPart(Integer.valueOf(this.interestType));

    // Add the interest result policy (if register interest)
    if (isRegister()) {
      message.addObjPart(Byte.valueOf(this.interestResultPolicy));
    }

    // Add the isDurable flag
    message.addObjPart(Boolean.valueOf(this.isDurable));

    // Add the forUpdatesAsInvalidates flag
    message.addObjPart(Boolean.valueOf(this.forUpdatesAsInvalidates));

    // Add the event id
    message.addObjPart(this.eventId);

    return message;
  }

  @Override
  public boolean shouldBeConflated() {
    return false;
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.CLIENT_INTEREST_MESSAGE;
  }

  public void writeExternal(ObjectOutput out) throws IOException {
    toData(out, InternalDataSerializer.createSerializationContext(out));
  }

  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    fromData(in, InternalDataSerializer.createDeserializationContext(in));
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeObject(this.eventId, out);
    DataSerializer.writeString(this.regionName, out);
    DataSerializer.writeObject(this.keyOfInterest, out);
    DataSerializer.writePrimitiveBoolean(this.isDurable, out);
    DataSerializer.writePrimitiveBoolean(this.forUpdatesAsInvalidates, out);
    DataSerializer.writePrimitiveInt(this.interestType, out);
    DataSerializer.writePrimitiveByte(this.interestResultPolicy, out);
    DataSerializer.writePrimitiveByte(this.action, out);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    this.eventId = (EventID) DataSerializer.readObject(in);
    this.regionName = DataSerializer.readString(in);
    this.keyOfInterest = DataSerializer.readObject(in);
    this.isDurable = DataSerializer.readPrimitiveBoolean(in);
    this.forUpdatesAsInvalidates = DataSerializer.readPrimitiveBoolean(in);
    this.interestType = DataSerializer.readPrimitiveInt(in);
    this.interestResultPolicy = DataSerializer.readPrimitiveByte(in);
    this.action = DataSerializer.readPrimitiveByte(in);
  }

  @Override
  public EventID getEventId() {
    return this.eventId;
  }

  public String getRegionName() {
    return this.regionName;
  }

  public Object getKeyOfInterest() {
    return this.keyOfInterest;
  }

  public int getInterestType() {
    return this.interestType;
  }

  public byte getInterestResultPolicy() {
    return this.interestResultPolicy;
  }

  public boolean getIsDurable() {
    return this.isDurable;
  }

  public boolean getForUpdatesAsInvalidates() {
    return this.forUpdatesAsInvalidates;
  }

  public boolean isKeyInterest() {
    return this.interestType == InterestType.KEY;
  }

  public boolean isRegister() {
    return this.action == REGISTER;
  }

  @Override
  public String getRegionToConflate() {
    return null;
  }

  @Override
  public Object getKeyToConflate() {
    // This method can be called by HARegionQueue.
    // Use this to identify the message type.
    return "interest";
  }

  @Override
  public Object getValueToConflate() {
    // This method can be called by HARegionQueue
    // Use this to identify the message type.
    return "interest";
  }

  @Override
  public void setLatestValue(Object value) {}

  public String toString() {
    return new StringBuilder().append(getClass().getSimpleName()).append("[").append("eventId=")
        .append(this.eventId).append("; regionName=").append(this.regionName)
        .append("; keyOfInterest=").append(this.keyOfInterest).append("; isDurable=")
        .append(this.isDurable).append("; forUpdatesAsInvalidates=")
        .append(this.forUpdatesAsInvalidates).append("; interestType=").append(this.interestType)
        .append("; interestResultPolicy=").append(this.interestResultPolicy).append("; action=")
        .append(this.action).append("]").toString();
  }

  @Override
  public Version[] getSerializationVersions() {
    return null;
  }
}
