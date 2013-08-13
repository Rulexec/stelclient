package by.muna.stel.messages;

import by.muna.stel.storage.StelRpcState;

public interface IStelRpcCheckStatusListener {
    void onStatus(StelRpcState state);
}
