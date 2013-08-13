package by.muna.stel.storage;

public enum StelRpcState {
    NOT_CREATED,
    CREATED,
    PREPARED,
    SENT,
    DELIVERED,
    REPLIED,
    REPLY_HANDLED,

    FAILED
}
