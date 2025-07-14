package com.zfq.common.taskdistributor.merge.impl.disk.group;

import java.util.Objects;

public class RegisterInfo {
    private String group;
    private boolean committed;

    public RegisterInfo(String group, boolean committed) {
        this.group = group;
        this.committed = committed;
    }

    public RegisterInfo() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RegisterInfo that = (RegisterInfo) o;
        return Objects.equals(group, that.group);
    }

    @Override
    public int hashCode() {
        return Objects.hash(group);
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public boolean isCommitted() {
        return committed;
    }

    public void setCommitted(boolean committed) {
        this.committed = committed;
    }

}
