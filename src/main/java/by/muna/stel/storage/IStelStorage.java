package by.muna.stel.storage;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;

public interface IStelStorage {
    // rpc calls

    int pollRpcId();

    void rpcCallCreated(int rpcId, int dcId);
    void rpcCallPrepared(int rpcId, long rpcMessageId);
    void rpcCallSent(int rpcId);
    void rpcCallDelivered(int rpcId);
    void rpcCallReplied(int rpcId, long replyMessageId);
    void rpcCallReplyHandled(int rpcId);

    void rpcCallFailed(int rpcId);

    StelRpcState getRpcState(int rpcId);

    int getRpcDcId(int rpcId);
    int getRpcIdByMessageId(int dcId, long rpcMessageId);

    long getRpcMessageId(int rpcId);
    long getRpcReplyMessageId(int rpcId);

    // datacenters

    byte[] getAuthKey(int dcId);
    void putAuthKey(int dcId, byte[] authKey);

    boolean isKnowDatacenters();
    Iterator<InetSocketAddress> getInitialServers();
    Iterator<Integer> getKnownDatacenterIds();

    Iterator<InetSocketAddress> getDatacenterAddresses(int dcId);
    void updateDatacenters(List<StelDatacenter> datacenters);

    // mt storage

    int pollSeqNo(int dcId, long authKeyId, long sessionId, boolean increment);
    void setSeqNo(int dcId, long authKeyId, long sessionId, int seqNo);

    // service storage

    void storeIntValue(int key, int value);
    Integer getIntValue(int key);

    void storeStringValue(int key, String value);
    String getStringValue(int key);

    void storeBlobValue(int key, byte[] value);
    byte[] getBlobValue(int key);

    void forgotValue(int key);

    // persistance

    void persist();
}
