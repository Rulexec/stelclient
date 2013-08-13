package by.muna.stel.messages;

import by.muna.tl.ITypedData;

public interface IStelListener {
    void onData(int rpcId, ITypedData data);

    void onSent(int rpcId);
    void onDelivered(int rpcId);

    void onConnectError(int rpcId);
}
