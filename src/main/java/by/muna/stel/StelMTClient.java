package by.muna.stel;

import by.muna.mt.IMTAuthKeyListener;
import by.muna.mt.IMTConnectionListener;
import by.muna.mt.IMTDataListener;
import by.muna.mt.MTClient;
import by.muna.mt.by.muna.mt.keys.MTAuthKey;
import by.muna.mt.logging.VerboseMTClientLogger;
import by.muna.mt.messages.IMTMessageStatusListener;
import by.muna.mt.tl.MTPing;
import by.muna.mt.tl.MTPong;
import by.muna.mt.tl.MTRpcResult;
import by.muna.stel.messages.IStelListener;
import by.muna.stel.storage.IStelStorage;
import by.muna.stel.storage.MTStarterMatryoshka;
import by.muna.stel.storage.MTStorage;
import by.muna.stel.storage.StelRpcState;
import by.muna.theatre.Actor;
import by.muna.theatre.ActorBehavior;
import by.muna.theatre.exceptions.ActorStoppedException;
import by.muna.tl.ITypedData;
import by.muna.tl.TypedData;
import by.muna.vk.tl.VKTL;
import by.muna.yasly.SocketThread;

import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Queue;

class StelMTClient implements IMTConnectionListener, IMTDataListener {
    private static enum StelMTClientState {
        STARTING, STARTED
    }
    private static enum ActorMessageType {
        CONNECTED, SEND, SEND_ACK
    }
    private static class ActorMessage {
        private ActorMessageType type;
        private Object data;

        public ActorMessage(ActorMessageType type) {
            this.type = type;
        }
        public ActorMessage(ActorMessageType type, Object data) {
            this.type = type;
            this.data = data;
        }

        public ActorMessageType getType() {
            return this.type;
        }

        @SuppressWarnings("unchecked")
        public <T> T getData() {
            return (T) this.data;
        }
    }
    private static class SendMessage {
        private int rpcId;
        private ITypedData data;

        public SendMessage(int rpcId, ITypedData data) {
            this.rpcId = rpcId;
            this.data = data;
        }

        private int getRpcId() {
            return this.rpcId;
        }

        private ITypedData getData() {
            return this.data;
        }
    }

    private StelClient stelClient;
    private MTClient mtClient;

    private IStelStorage stelStorage;

    private long authKeyId = 0;

    private int dcId;

    private Iterator<InetSocketAddress> addresses;

    private boolean isConnecting = false;

    private IStelListener listener;

    private StelMTClientState state = StelMTClientState.STARTING;
    private Actor<ActorMessage, Object> actor;
    private Queue<SendMessage> preStartMessages = new LinkedList<SendMessage>();

    private boolean isStopping = false;

    public StelMTClient(
        StelClient client, int dcId, long authKeyId, MTClient mtClient, MTStarterMatryoshka matryoshka)
    {
        this.stelClient = client;
        this.mtClient = mtClient;

        this.stelStorage = this.stelClient.getStorage();

        this.dcId = dcId;
        this.authKeyId = authKeyId;

        this.addresses = this.stelStorage.getDatacenterAddresses(this.dcId);

        MTStorage mtStorage = this.stelClient.getMTStorage(this.dcId);

        matryoshka.swap(mtStorage, this, this, this.authKeyId);

        this.state = StelMTClientState.STARTED;

        this.createActor();
    }
    public StelMTClient(StelClient client, SocketThread socketThread, int dcId) {
        this.stelClient = client;
        this.mtClient = new MTClient(socketThread);
        this.mtClient.setLogger(new VerboseMTClientLogger());

        this.stelStorage = this.stelClient.getStorage();

        this.mtClient.setConnectionListener(this);
        this.mtClient.setOnData(this);

        this.mtClient.addSchema(VKTL.SCHEMA);

        this.dcId = dcId;

        this.mtClient.setStorage(this.stelClient.getMTStorage(this.dcId));

        this.createActor();
    }

    private void createActor() {
        this.actor = this.stelClient.getActorsThread().createActor(new ActorBehavior<ActorMessage, Object>() {
            @Override
            public Object onMessage(ActorMessage message) {
                StelMTClient.this.onActorMessage(message);

                return null;
            }
        });
    }

    public void setListener(IStelListener listener) {
        this.listener = listener;
    }

    private void createAuthKey() {
        byte[] authKey = this.stelStorage.getAuthKey(this.dcId);

        if (authKey != null) {
            this.authKeyId = this.mtClient.addAuthKey(new MTAuthKey(authKey));

            this.pingAndConnect();
        } else {
            this.mtClient.generateAuthKey(new IMTAuthKeyListener() {
                @Override
                public void onAuthKeyResult(MTClient client, byte[] authKey, boolean graceful) {
                    if (authKey != null) {
                        StelMTClient.this.stelStorage.putAuthKey(
                            StelMTClient.this.dcId,
                            authKey
                        );

                        StelMTClient.this.authKeyId = StelMTClient.this.mtClient.addAuthKey(
                            new MTAuthKey(authKey)
                        );

                        StelMTClient.this.pingAndConnect();
                    } else {
                        StelMTClient.this.die(false);
                    }
                }
            });
        }
    }
    private void pingAndConnect() {
        this.mtClient.send(this.authKeyId, 1,
            new TypedData(MTPing.CONSTRUCTOR)
                .setTypedData(MTPing.pingId, 1L),
            false,
            new IMTMessageStatusListener() {
                @Override public void onConstructed(long messageId) {}
                @Override public void onSent(long messageId) {}
                @Override public void onDelivered(long messageId) {
                    try {
                        StelMTClient.this.actor.send(new ActorMessage(ActorMessageType.CONNECTED));
                    } catch (ActorStoppedException e) {
                        StelMTClient.this.die(true);
                    }
                }

                @Override
                public void onConnectionError(long messageId) {
                    StelMTClient.this.die(false);
                }
            }
        );
    }

    public void connect(Iterator<InetSocketAddress> addresses) {
        if (this.isConnecting) {
            throw new RuntimeException("Connect after connect is not supported.");
        }

        this.isConnecting = true;

        this.addresses = addresses;

        this.mtClient.connect(this.addresses.next());

        if (this.authKeyId == 0) {
            this.createAuthKey();
        } else {
            this.pingAndConnect();
        }
    }

    private void processSendMessage(SendMessage sendMessage) {
        if (this.isStopping) {
            this.listener.onConnectError(sendMessage.getRpcId());
            return;
        }

        final int rpcId = sendMessage.getRpcId();
        this.stelStorage.rpcCallCreated(rpcId, this.dcId);

        this.mtClient.send(this.authKeyId, 1, sendMessage.getData(), new IMTMessageStatusListener() {
            @Override
            public void onConstructed(long messageId) {
                StelMTClient.this.stelStorage.rpcCallPrepared(rpcId, messageId);
            }

            @Override
            public void onSent(long messageId) {
                StelMTClient.this.stelStorage.rpcCallSent(rpcId);
            }

            @Override
            public void onDelivered(long messageId) {
                StelMTClient.this.stelStorage.rpcCallDelivered(rpcId);
            }

            @Override
            public void onConnectionError(long messageId) {
                StelMTClient.this.stelStorage.rpcCallFailed(rpcId);
            }
        });
    }
    public void onActorMessage(ActorMessage message) {
        switch (message.getType()) {
        case SEND:
            SendMessage sendMessage = message.getData();

            switch (this.state) {
            case STARTED:
                this.processSendMessage(sendMessage);

                break;
            case STARTING:
                this.preStartMessages.add(sendMessage);

                break;
            default:
                throw new RuntimeException("Unknown send state: " + this.state);
            }

            break;
        case SEND_ACK:
            int ackRpcId = message.getData();
            long ackMessageId = this.stelStorage.getRpcReplyMessageId(ackRpcId);

            this.mtClient.confirmMessage(this.authKeyId, 1, ackMessageId);

            break;
        case CONNECTED:
            this.state = StelMTClientState.STARTED;

            while (!this.preStartMessages.isEmpty()) {
                SendMessage preStartMessage = this.preStartMessages.poll();
                this.processSendMessage(preStartMessage);
            }

            this.preStartMessages = null;

            break;
        default:
            throw new RuntimeException("Unknown actor message type: " + message.getType());
        }
    }

    public void call(int rpcId, ITypedData data) {
        try {
            this.actor.send(new ActorMessage(ActorMessageType.SEND, new SendMessage(rpcId, data)));
        } catch (ActorStoppedException e) {
            this.listener.onConnectError(rpcId);
        }
    }

    public void confirmMessage(int rpcId) {
        try {
            this.actor.send(new ActorMessage(ActorMessageType.SEND_ACK, rpcId));
        } catch (ActorStoppedException e) { e.printStackTrace(); }
    }

    private void die(boolean graceful) {
        this.isStopping = true;

        this.mtClient.disconnect(graceful);

        this.actor.stop();

        if (this.preStartMessages != null && !this.preStartMessages.isEmpty()) {
            SendMessage message = this.preStartMessages.poll();

            this.listener.onConnectError(message.getRpcId());
        }

        this.stelClient.clientDied(this.dcId);
    }

    @Override
    public void onData(MTClient client, long authKeyId, long sessionId, long messageId, ITypedData data) {
        switch (data.getId()) {
        case MTRpcResult.CONSTRUCTOR_ID:
            ITypedData body = data.getTypedData(MTRpcResult.result);

            long reqMsgId = data.getTypedData(MTRpcResult.reqMsgId);

            int rpcId = this.stelStorage.getRpcIdByMessageId(this.dcId, reqMsgId);

            if (rpcId != -1) {
                if (this.stelStorage.getRpcState(rpcId) == StelRpcState.REPLY_HANDLED) {
                    client.confirmMessage(authKeyId, sessionId, messageId);
                    break;
                }

                this.stelStorage.rpcCallReplied(rpcId, messageId);
            } else {
                this.mtClient.confirmMessage(this.authKeyId, 1, messageId);
            }

            this.listener.onData(rpcId, body);

            break;
        case MTPong.CONSTRUCTOR_ID: break;
        default:
            throw new RuntimeException("Unknown data type: " + data.toString());
        }
    }

    @Override public void onConnected(MTClient client) {}

    @Override
    public void onConnectError(MTClient client, boolean graceful) {
        this.die(graceful);
    }
}
