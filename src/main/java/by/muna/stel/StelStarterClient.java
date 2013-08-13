package by.muna.stel;

import by.muna.mt.IMTAuthKeyListener;
import by.muna.mt.IMTConnectionListener;
import by.muna.mt.IMTDataListener;
import by.muna.mt.MTClient;
import by.muna.mt.by.muna.mt.keys.MTAuthKey;
import by.muna.mt.logging.VerboseMTClientLogger;
import by.muna.mt.messages.IMTMessageStatusListener;
import by.muna.mt.tl.MTRpcResult;
import by.muna.stel.storage.IStelStorage;
import by.muna.stel.storage.MTStarterMatryoshka;
import by.muna.stel.storage.StorageContants;
import by.muna.tl.ITypedData;
import by.muna.tl.TypedData;
import by.muna.vk.tl.VKConfig;
import by.muna.vk.tl.VKHelpGetConfig;
import by.muna.vk.tl.VKTL;
import by.muna.yasly.SocketThread;

import java.net.InetSocketAddress;
import java.util.Iterator;

class StelStarterClient implements IMTDataListener, IMTConnectionListener {
    private StelClient stelClient;
    private IStelStorage stelStorage;

    private MTClient mtClient;

    private Iterator<InetSocketAddress> addresses;

    private long authKeyId;
    private byte[] authKey;

    private boolean started = false;

    private MTStarterMatryoshka matryoshka;

    private int getDatacentersRpcId;

    public StelStarterClient(StelClient client, SocketThread socketThread) {
        this.stelClient = client;
        this.stelStorage = this.stelClient.getStorage();

        this.mtClient = new MTClient(socketThread);
        this.mtClient.setLogger(new VerboseMTClientLogger());

        this.mtClient.addSchema(VKTL.SCHEMA);

        this.matryoshka = new MTStarterMatryoshka(this, this);

        this.mtClient.setStorage(this.matryoshka);
        this.mtClient.setConnectionListener(this.matryoshka);
        this.mtClient.setOnData(this.matryoshka);
    }

    public void start(Iterator<InetSocketAddress> addresses) {
        this.addresses = addresses;

        byte[] unknownAuthKey = this.stelStorage.getBlobValue(StorageContants.UNKNOWN_DATACENTER_AUTHKEY);
        if (unknownAuthKey != null) {
            String unknownDatacenterAddress = this.stelStorage.getStringValue(
                StorageContants.UNKNOWN_DATACENTER_ADDRESS
            );

            if (unknownDatacenterAddress != null) {
                String parts[] = unknownDatacenterAddress.split(":");

                String host = parts[0];
                int port = Integer.parseInt(parts[1]);

                InetSocketAddress address = new InetSocketAddress(host, port);

                this.authKey = unknownAuthKey;
                this.authKeyId = this.mtClient.addAuthKey(new MTAuthKey(this.authKey));

                this.mtClient.connect(address);

                return;
            } else {
                this.stelStorage.forgotValue(StorageContants.UNKNOWN_DATACENTER_AUTHKEY);
            }
        }

        this.mtClient.connect(this.addresses.next());
    }

    @Override
    public void onData(MTClient client, long authKeyId, long sessionId, long messageId, ITypedData data) {
        switch (data.getId()) {
        case MTRpcResult.CONSTRUCTOR_ID:
            ITypedData body = data.getTypedData(MTRpcResult.result);

            switch (body.getId()) {
            case VKConfig.CONSTRUCTOR_ID:
                this.started = true;

                this.stelStorage.rpcCallReplied(this.getDatacentersRpcId, messageId);

                int dcId = body.getTypedData(VKConfig.thisDc);

                this.stelClient.getStorage().putAuthKey(dcId, this.authKey);

                this.stelClient.onData(-1, body);

                this.stelStorage.rpcCallReplyHandled(this.getDatacentersRpcId);

                this.stelStorage.forgotValue(StorageContants.UNKNOWN_DATACENTER_AUTHKEY);
                this.stelStorage.forgotValue(StorageContants.UNKNOWN_DATACENTER_ADDRESS);
                this.stelStorage.forgotValue(StorageContants.GET_DATACENTERS_RPC);

                this.mtClient.confirmMessage(authKeyId, sessionId, messageId);

                this.stelClient.started(dcId, this.authKeyId, this.mtClient, this.matryoshka);

                break;
            default:
                throw new RuntimeException("Starter unknown rpc reply: " + body.toString());
            }
            break;
        default:
            throw new RuntimeException("Starter unknown message: " + data.toString());
        }
    }

    private void makeGetConfigRequest() {
        this.getDatacentersRpcId = StelStarterClient.this.stelStorage.pollRpcId();
        this.stelStorage.storeIntValue(
            StorageContants.GET_DATACENTERS_RPC,
            this.getDatacentersRpcId
        );
        // We don't know, what datacenter is it. So 0.
        // Anyway, this value (datacenter id) will not used.
        this.stelStorage.rpcCallCreated(
            this.getDatacentersRpcId, 0
        );

        this.mtClient.send(
            this.authKeyId, 1,
            new TypedData(VKHelpGetConfig.CONSTRUCTOR),
            new IMTMessageStatusListener() {
                @Override
                public void onConstructed(long messageId) {
                    StelStarterClient.this.stelStorage.rpcCallPrepared(
                        StelStarterClient.this.getDatacentersRpcId, messageId
                    );
                }

                @Override
                public void onSent(long messageId) {
                    StelStarterClient.this.stelStorage.rpcCallSent(
                        StelStarterClient.this.getDatacentersRpcId
                    );
                }

                @Override
                public void onDelivered(long messageId) {
                    StelStarterClient.this.stelStorage.rpcCallDelivered(
                        StelStarterClient.this.getDatacentersRpcId
                    );
                }

                @Override
                public void onConnectionError(long messageId) {
                    StelStarterClient.this.stelStorage.rpcCallFailed(
                        StelStarterClient.this.getDatacentersRpcId
                    );
                    StelStarterClient.this.mtClient.disconnect(false);
                }
            }
        );
    }

    @Override
    public void onConnected(MTClient client) {
        if (this.authKey == null) {
            this.mtClient.generateAuthKey(new IMTAuthKeyListener() {
                @Override
                public void onAuthKeyResult(MTClient client, byte[] authKey, boolean graceful) {
                    if (authKey != null) {
                        StelStarterClient.this.stelStorage.storeBlobValue(
                            StorageContants.UNKNOWN_DATACENTER_AUTHKEY,
                            authKey
                        );

                        InetSocketAddress address = StelStarterClient.this.mtClient.getAddress();
                        StelStarterClient.this.stelStorage.storeStringValue(
                            StorageContants.UNKNOWN_DATACENTER_ADDRESS,
                            address.getAddress().getHostAddress() + ":" + Integer.toString(address.getPort())
                        );

                        StelStarterClient.this.authKey = authKey;

                        StelStarterClient.this.authKeyId =
                            StelStarterClient.this.mtClient.addAuthKey(new MTAuthKey(authKey));

                        StelStarterClient.this.makeGetConfigRequest();
                    } else {
                        StelStarterClient.this.mtClient.disconnect(false);
                    }
                }
            });
        } else {
            this.makeGetConfigRequest();
        }
    }

    @Override
    public void onConnectError(MTClient client, boolean graceful) {
        if (this.started) return;

        if (graceful) {
            this.stelClient.startFailed(true);
            return;
        }

        if (this.addresses.hasNext()) {
            this.mtClient.connect(this.addresses.next());
        } else {
            this.stelClient.startFailed(false);
        }
    }
}
