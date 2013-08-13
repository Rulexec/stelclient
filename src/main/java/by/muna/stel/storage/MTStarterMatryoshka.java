package by.muna.stel.storage;

import by.muna.mt.IMTConnectionListener;
import by.muna.mt.IMTDataListener;
import by.muna.mt.MTClient;
import by.muna.mt.storage.IMTStorage;
import by.muna.mt.storage.ISeqNoPoller;
import by.muna.mt.storage.MTTransientStorage;
import by.muna.tl.ITypedData;

public class MTStarterMatryoshka extends MTTransientStorage implements IMTDataListener, IMTConnectionListener {
    private boolean swapped = false;
    private IMTStorage storage;
    private IMTDataListener dataListener;
    private IMTConnectionListener connectionListener;

    //private

    public MTStarterMatryoshka(IMTDataListener dataListener, IMTConnectionListener connectionListener) {
        super();

        this.dataListener = dataListener;
        this.connectionListener = connectionListener;
    }

    public void swap(
        MTStorage storage, IMTDataListener dataListener, IMTConnectionListener connectionListener,
        long authKeyId)
    {
        synchronized (this) {
            this.storage = storage;
            this.dataListener = dataListener;
            this.connectionListener = connectionListener;

            ISeqNoPoller seqNoPoller = this.getSeqNoPoller(authKeyId, 1);
            int seqNo = seqNoPoller.pollSeqNo(false);

            storage.setSeqNo(authKeyId, 1, seqNo);

            this.swapped = true;
        }
    }

    // mt storage

    @Override
    public ISeqNoPoller getSeqNoPoller(long authKeyId, long sessionId) {
        if (this.swapped) return this.storage.getSeqNoPoller(authKeyId, sessionId);

        synchronized (this) {
            if (this.swapped) return this.storage.getSeqNoPoller(authKeyId, sessionId);

            return super.getSeqNoPoller(authKeyId, sessionId);
        }
    }

    // data listener

    @Override
    public void onData(MTClient client, long authKeyId, long sessionId, long messageId, ITypedData data) {
        this.dataListener.onData(client, authKeyId, sessionId, messageId, data);
    }

    // connection listener

    @Override
    public void onConnected(MTClient client) {
        this.connectionListener.onConnected(client);
    }

    @Override
    public void onConnectError(MTClient client, boolean graceful) {
        this.connectionListener.onConnectError(client, graceful);
    }
}
