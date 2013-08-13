package by.muna.stel.storage;

import java.net.InetSocketAddress;
import java.util.List;

public class StelDatacenter {
    private int dcId;
    private List<InetSocketAddress> addresses;

    public StelDatacenter(int dcId, List<InetSocketAddress> addresses) {
        this.dcId = dcId;
        this.addresses = addresses;
    }

    public int getDcId() {
        return this.dcId;
    }

    public List<InetSocketAddress> getAddresses() {
        return this.addresses;
    }
}
