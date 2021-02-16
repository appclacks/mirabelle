package fr.mcorbin.mirabelle.memtable;

import java.util.HashMap;

public class Serie {
    private String serviceName;
    private HashMap<String, String> labels;

    public Serie(String name, HashMap<String, String> labels) {
        this.serviceName = name;
        this.labels = labels;
    }

    @Override
    public int hashCode() {
        return serviceName.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Serie other = (Serie) obj;
        if (!this.serviceName.equals(other.getServiceName()) || !this.labels.equals(other.getLabels())) {
            return false;
        }
        return true;
    }

    public String getServiceName() {
        return serviceName;
    }

    public HashMap<String, String> getLabels() {
        return labels;
    }
}
