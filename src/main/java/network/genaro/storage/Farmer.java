package network.genaro.storage;

/*
"farmer":{
"userAgent":"8.7.3",
"protocol":"1.2.0-local",
"address":"59.46.230.210",
"port":9001,
"nodeID":"70a8a597a49aa732860218292b73f6bbc2f63925",
"lastSeen":"2018-11-06T09:34:47.564Z"},
 */

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown=true)
public class Farmer {
    private String userAgent;
    private String protocol;
    private String address;
    private String port;
    private String nodeID;
    private String lastSeen;

    public String getUserAgent() {
        return userAgent;
    }

    public void setUserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getAddress() {
        return address;
    }

    public void setAddress(String address) {
        this.address = address;
    }

    public String getPort() {
        return port;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public String getNodeID() {
        return nodeID;
    }

    public void setNodeID(String nodeID) {
        this.nodeID = nodeID;
    }

    public String getLastSeen() {
        return lastSeen;
    }

    public void setLastSeen(String lastSeen) {
        this.lastSeen = lastSeen;
    }

    @Override
    public String toString() {
        return "Farmer{" +
                "userAgent='" + userAgent + '\'' +
                ", protocol='" + protocol + '\'' +
                ", address='" + address + '\'' +
                ", port='" + port + '\'' +
                ", nodeID='" + nodeID + '\'' +
                ", lastSeen='" + lastSeen + '\'' +
                '}';
    }
}
