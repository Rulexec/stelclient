package by.muna.stel.storage;

import by.muna.mt.storage.IMTStorage;
import by.muna.mt.storage.ISeqNoPoller;

import java.util.HashMap;
import java.util.Map;

public class MTStorage implements IMTStorage {
    private IStelStorage storage;
    private int dcId;

    private Map<Long, Map<Long, Long>> serverSalts = new HashMap<Long, Map<Long, Long>>();
    private Map<Long, Map<Long, ISeqNoPoller>> seqNumbers = new HashMap<Long, Map<Long, ISeqNoPoller>>();

    public MTStorage(IStelStorage storage, int dcId) {
        this.storage = storage;

        this.dcId = dcId;
    }

    @Override
    public int getTimeDiff() {
        return 0; // TODO
    }

    @Override
    public void syncTime(int serverTime) {
        // TODO
    }

    @Override
    public long getServerSalt(long authKeyId, long sessionId) {
        Map<Long, Long> sessionServerSalts = this.serverSalts.get(authKeyId);

        if (sessionServerSalts == null) return 0;

        Long salt = sessionServerSalts.get(sessionId);

        if (salt == null) return 0;
        else return salt;
    }

    @Override
    public void addServerSalt(long authKeyId, long sessionId, int validSince, int validUntil, long salt) {
        // TODO
    }

    @Override
    public void serverSalt(long authKeyId, long sessionId, long salt) {
        Map<Long, Long> sessionServerSalts = this.serverSalts.get(authKeyId);

        if (sessionServerSalts == null) {
            sessionServerSalts = new HashMap<Long, Long>();
            this.serverSalts.put(authKeyId, sessionServerSalts);
        }

        sessionServerSalts.put(sessionId, salt);
    }

    @Override
    public ISeqNoPoller getSeqNoPoller(final long authKeyId, final long sessionId) {
        Map<Long, ISeqNoPoller> sessionSeqNumbers = this.seqNumbers.get(authKeyId);

        if (sessionSeqNumbers == null) {
            sessionSeqNumbers = new HashMap<Long, ISeqNoPoller>();
            this.seqNumbers.put(authKeyId, sessionSeqNumbers);
        }

        ISeqNoPoller seqNoPoller = sessionSeqNumbers.get(sessionId);

        if (seqNoPoller == null) {
            seqNoPoller = new ISeqNoPoller() {
                private int seqNo = 0;

                @Override
                public int pollSeqNo(boolean increment) {
                    return MTStorage.this.storage.pollSeqNo(
                        MTStorage.this.dcId, authKeyId, sessionId, increment
                    );
                }
            };
            sessionSeqNumbers.put(sessionId, seqNoPoller);
        }

        return seqNoPoller;
    }

    public void setSeqNo(long authKeyId, long sessionId, int seqNo) {
        this.storage.setSeqNo(this.dcId, authKeyId, sessionId, seqNo);
    }
}
