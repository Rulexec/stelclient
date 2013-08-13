package by.muna.stel;

import by.muna.mt.MTClient;
import by.muna.mt.tl.MTRpcError;
import by.muna.stel.messages.IStelListener;
import by.muna.stel.storage.*;
import by.muna.theatre.Actor;
import by.muna.theatre.ActorBehavior;
import by.muna.theatre.ActorsThread;
import by.muna.theatre.ActorsThreadImpl;
import by.muna.theatre.exceptions.ActorStoppedException;
import by.muna.tl.ITypedData;
import by.muna.util.StringUtil;
import by.muna.vk.tl.VKConfig;
import by.muna.vk.tl.VKDcOption;
import by.muna.yasly.SocketThread;
import by.muna.yasly.logging.SilentSocketLogger;

import java.net.InetSocketAddress;
import java.util.*;

public class StelClient implements IStelListener {
    private static class RpcCallRequest {
        private int dcId, rpcId;
        private ITypedData data;

        public RpcCallRequest(int dcId, int rpcId, ITypedData data) {
            this.dcId = dcId;
            this.rpcId = rpcId;
            this.data = data;
        }

        public int getDcId() {
            return this.dcId;
        }
        public void setDcId(int dcId) {
            this.dcId = dcId;
        }

        public int getRpcId() {
            return this.rpcId;
        }

        public ITypedData getData() {
            return this.data;
        }
    }
    private static enum StelClientState {
        STARTING, STARTED
    }
    public static enum ActorMessageType {
        RPC_CALL, STARTED, START_FAILED, RPC_CALL_REPLY_HANDLED
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

    private SocketThread socketThread;

    private IStelListener listener;
    private IStelStorage storage;

    private Queue<RpcCallRequest> preStartRequests = new LinkedList<RpcCallRequest>();

    private ActorsThread actorsThread = new ActorsThreadImpl();

    private StelClientState state = StelClientState.STARTING;
    private Actor<ActorMessage, Object> actor;

    private boolean isStopping = false;

    private Iterator<Integer> startingDcIds;

    private int mainDatacenter;
    private Map<Integer, StelMTClient> datacenterClients = new HashMap<Integer, StelMTClient>();

    private Map<Integer, RpcCallRequest> rpcCallsInProcessing = new HashMap<Integer, RpcCallRequest>();
    private Map<Integer, ITypedData> repliedButNotHandled = new HashMap<Integer, ITypedData>();

    public StelClient() {
        this.socketThread = new SocketThread();

        //this.socketThread.setLogger(new VerboseSocketLogger());
        this.socketThread.setLogger(new SilentSocketLogger());
    }

    public void setStorage(IStelStorage storage) {
        this.storage = storage;
    }
    public IStelStorage getStorage() {
        return this.storage;
    }
    ActorsThread getActorsThread() {
        return this.actorsThread;
    }

    public void setListener(IStelListener listener) {
        this.listener = listener;
    }

    private StelMTClient getStelMTClient(int dcId) {
        StelMTClient client = this.datacenterClients.get(dcId);

        if (client == null) {
            client = new StelMTClient(this, this.socketThread, dcId);
            client.setListener(this);

            client.connect(this.storage.getDatacenterAddresses(dcId));

            this.datacenterClients.put(dcId, client);
        }

        return client;
    }
    private void processRpcCallMessage(RpcCallRequest rpcRequest) {
        if (this.isStopping) {
            this.listener.onConnectError(rpcRequest.getRpcId());
            return;
        }

        switch (this.state) {
        case STARTING:
            this.preStartRequests.add(rpcRequest);

            break;
        case STARTED:
            int dcId = rpcRequest.getDcId();

            if (dcId == -1) {
                dcId = this.mainDatacenter;
            }

            StelMTClient client = this.getStelMTClient(dcId);

            client.call(rpcRequest.getRpcId(), rpcRequest.getData());

            break;
        default:
            throw new RuntimeException("Unknown state at rpc call: " + this.state);
        }
    }
    private void onActorMessage(ActorMessage message) {
        switch (message.getType()) {
        case RPC_CALL:
            this.processRpcCallMessage((RpcCallRequest) message.getData());
            break;
        case RPC_CALL_REPLY_HANDLED:
            int handledRpcId = message.getData();
            int handledRpcDcId = this.storage.getRpcDcId(handledRpcId);

            StelMTClient handledRpcClient = this.getStelMTClient(handledRpcDcId);
            handledRpcClient.confirmMessage(handledRpcId);

            break;
        case STARTED:
            this.state = StelClientState.STARTED;

            while (!this.preStartRequests.isEmpty()) {
                RpcCallRequest request = this.preStartRequests.poll();

                this.processRpcCallMessage(request);
            }

            this.preStartRequests = null;
            break;
        default:
            throw new RuntimeException(
                "Unknown stel actor message type: " + message.getType() + ": " + message.getData()
            );
        }
    }

    public void start() {
        this.actor = this.actorsThread.createActor(new ActorBehavior<ActorMessage, Object>() {
            @Override
            public Object onMessage(ActorMessage actorMessage) {
                StelClient.this.onActorMessage(actorMessage);

                return null;
            }
        });

        if (!this.storage.isKnowDatacenters()) {
            StelStarterClient starterClient = new StelStarterClient(this, this.socketThread);
            starterClient.start(this.storage.getInitialServers());
        } else {
            this.startingDcIds = this.storage.getKnownDatacenterIds();

            this.mainDatacenter = this.startingDcIds.next();

            StelMTClient client = new StelMTClient(this, this.socketThread, this.mainDatacenter);

            client.setListener(this);

            client.connect(this.storage.getDatacenterAddresses(this.mainDatacenter));

            this.datacenterClients.put(this.mainDatacenter, client);

            this.onStarted();
        }
    }
    void onStarted() {
        try {
            this.actor.send(new ActorMessage(ActorMessageType.STARTED));
        } catch (ActorStoppedException e) { e.printStackTrace(); }
    }
    void started(int dcId, long authKeyId, MTClient mtClient, MTStarterMatryoshka matryoshka) {
        StelMTClient client = new StelMTClient(this, dcId, authKeyId, mtClient, matryoshka);

        this.mainDatacenter = dcId;
        this.datacenterClients.put(this.mainDatacenter, client);

        this.onStarted();
    }
    void startFailed(boolean graceful) {
        try {
            this.actor.send(new ActorMessage(ActorMessageType.START_FAILED));
        } catch (ActorStoppedException e) { e.printStackTrace(); }
    }
    void clientDied(int dcId) {
        this.datacenterClients.remove(dcId);
    }

    MTStorage getMTStorage(int dcId) {
        return new MTStorage(this.storage, dcId);
    }

    public void call(int rpcId, ITypedData data) {
        this.call(-1, rpcId, data);
    }
    private void call(int dcId, int rpcId, ITypedData data) {
        try {
            RpcCallRequest callRequest = new RpcCallRequest(dcId, rpcId, data);

            this.actor.send(new ActorMessage(
                ActorMessageType.RPC_CALL,
                callRequest
            ));

            this.rpcCallsInProcessing.put(rpcId, callRequest);
        } catch (ActorStoppedException e) {
            this.listener.onConnectError(rpcId);
        }
    }

    public void callReplyHandled(int rpcId) {
        this.storage.rpcCallReplyHandled(rpcId);
        this.rpcCallsInProcessing.remove(rpcId);
        this.repliedButNotHandled.remove(rpcId);

        try {
            this.actor.send(new ActorMessage(ActorMessageType.RPC_CALL_REPLY_HANDLED, rpcId));
        } catch (ActorStoppedException e) { e.printStackTrace(); }
    }

    public StelRpcState checkRpcStatus(int rpcId) {
        StelRpcState state = this.storage.getRpcState(rpcId);

        switch (state) {
        case NOT_CREATED:
            return this.rpcCallsInProcessing.containsKey(rpcId) ?
                   StelRpcState.CREATED : StelRpcState.NOT_CREATED;
        case CREATED:
            if (!this.rpcCallsInProcessing.containsKey(rpcId)) {
                this.storage.rpcCallFailed(rpcId);

                return StelRpcState.FAILED;
            } else {
                return StelRpcState.CREATED;
            }
        case REPLY_HANDLED:
            return state;
        case FAILED:
            this.listener.onConnectError(rpcId);

            return state;
        }

        ITypedData notHandledData = this.repliedButNotHandled.get(rpcId);
        if (notHandledData != null) {
            this.listener.onData(rpcId, notHandledData);

            return StelRpcState.REPLIED;
        }

        // TODO: send request for MT msgs_state_req.
        // Also request for msg_resend_req, if status is REPLIED.
        // If state is SENT, but server says, that message is not delivered,
        // notify, that status is FAILED.

        return state;
    }

    @Override
    public void onData(int rpcId, ITypedData data) {
        boolean consumed = false;

        RpcCallRequest rpcCall = this.rpcCallsInProcessing.get(rpcId);

        switch (data.getId()) {
        case VKConfig.CONSTRUCTOR_ID:
            Object[] dcOptionsArray = data.<ITypedData>getTypedData(VKConfig.dcOptions).getTypedData(0);

            List<StelDatacenter> datacenters = new LinkedList<StelDatacenter>();
            Map<Integer, List<InetSocketAddress>> dcAddresses = new HashMap<Integer, List<InetSocketAddress>>();

            for (Object dcOptionObj : dcOptionsArray) {
                ITypedData dcOption = (ITypedData) dcOptionObj;

                switch (dcOption.getId()) {
                case VKDcOption.CONSTRUCTOR_ID:
                    int dcId = dcOption.getTypedData(VKDcOption.id);

                    List<InetSocketAddress> addresses = dcAddresses.get(dcId);
                    if (addresses == null) {
                        addresses = new LinkedList<InetSocketAddress>();
                        dcAddresses.put(dcId, addresses);
                    }

                    InetSocketAddress address = new InetSocketAddress(
                        StringUtil.fromUTF8(dcOption.<byte[]>getTypedData(VKDcOption.ipAddress)),
                        dcOption.<Integer>getTypedData(VKDcOption.port)
                    );
                    addresses.add(address);

                    break;
                default:
                    throw new RuntimeException("Unknown DcOption: " + dcOption.toString());
                }
            }

            for (Map.Entry<Integer, List<InetSocketAddress>> addresses : dcAddresses.entrySet()) {
                datacenters.add(new StelDatacenter(addresses.getKey(), addresses.getValue()));
            }

            this.storage.updateDatacenters(datacenters);

            break;
        case MTRpcError.CONSTRUCTOR_ID:
            if (rpcId == -1) break;

            int rpcErrorCode = data.getTypedData(MTRpcError.errorCode);

            if (rpcErrorCode == 303) {
                consumed = true;

                if (rpcCall == null) {
                    this.failAndConfirmRpcCall(rpcId);

                    break;
                }

                String message = StringUtil.fromUTF8(data.<byte[]>getTypedData(MTRpcError.errorMessage));

                if (message.startsWith("PHONE_MIGRATE_")) {
                    int dcId = 0;

                    int messageLength = message.length();

                    int i = 14; // end of PHONE_MIGRATE_
                    while (true) {
                        char c = message.charAt(i);

                        if (!(c >= '0' && c <= '9')) break;

                        dcId = (dcId * 10) + (c - '0');

                        if (i++ >= messageLength) break;
                    }

                    this.mainDatacenter = dcId;

                    rpcCall.setDcId(dcId);

                    this.call(dcId, rpcId, rpcCall.getData());
                } else {
                    throw new RuntimeException("Unknown 303 rpc error: " + message);
                }
            }

            break;
        }

        if (rpcId == -1) return;
        if (rpcCall == null) {
            switch (this.storage.getRpcState(rpcId)) {
            case REPLY_HANDLED:
            case FAILED:
                consumed = true;

                StelMTClient rpcCallClient = this.getStelMTClient(this.storage.getRpcDcId(rpcId));
                rpcCallClient.confirmMessage(rpcId);
            }
        }

        if (consumed) return;

        this.repliedButNotHandled.put(rpcId, data);

        this.listener.onData(rpcId, data);
    }
    private void failAndConfirmRpcCall(int rpcId) {
        StelRpcState state = this.storage.getRpcState(rpcId);

        switch (state) {
        case REPLY_HANDLED:
        case FAILED:
            break;
        default:
            this.storage.rpcCallFailed(rpcId);
            this.listener.onConnectError(rpcId);
        }

        StelMTClient rpcCallClient = this.getStelMTClient(this.storage.getRpcDcId(rpcId));
        rpcCallClient.confirmMessage(rpcId);
    }

    @Override
    public void onSent(int rpcId) {
        if (rpcId == -1) return;

        this.listener.onSent(rpcId);
    }

    @Override
    public void onDelivered(int rpcId) {
        if (rpcId == -1) return;

        this.listener.onDelivered(rpcId);
    }

    @Override
    public void onConnectError(int rpcId) {
        if (rpcId == -1) return;

        this.storage.rpcCallFailed(rpcId);

        this.rpcCallsInProcessing.remove(rpcId);
        this.listener.onConnectError(rpcId);
    }

    public void stop() {
        this.isStopping = true;

        this.socketThread.stop();
        this.actorsThread.stop(true);

        this.storage.persist();
    }
}
