package org.apache.kafka.migration;

public class ZkControllerState {
    public static ZkControllerState EMPTY = new ZkControllerState(-1, -1, -1);

    private final int controllerId;
    private final int controllerEpoch;
    private final int zkVersion;

    public ZkControllerState(int controllerId, int controllerEpoch, int zkVersion) {
        this.controllerId = controllerId;
        this.controllerEpoch = controllerEpoch;
        this.zkVersion = zkVersion;
    }

    public int controllerId() {
        return controllerId;
    }

    public int controllerEpoch() {
        return controllerEpoch;
    }

    public int zkVersion() {
        return zkVersion;
    }

    @Override
    public String toString() {
        return "ZkControllerState{" +
                "controllerId=" + controllerId +
                ", controllerEpoch=" + controllerEpoch +
                ", zkVersion=" + zkVersion +
                '}';
    }
}
