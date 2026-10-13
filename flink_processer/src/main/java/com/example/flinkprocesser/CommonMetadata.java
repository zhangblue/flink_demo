package com.example.flinkprocesser;

import java.io.Serializable;

/** 跨事件保存的 common 元数据。空值在 JSON 输入边界归一化。 */
public class CommonMetadata implements Serializable {
    private static final long serialVersionUID = 1L;

    private String selfAppPackage;
    private String selfAppName;
    private String model;
    private String brand;
    private String os;
    private String osVersion;

    public CommonMetadata() {
    }

    public CommonMetadata(String selfAppPackage, String selfAppName, String model,
                          String brand, String os, String osVersion) {
        this.selfAppPackage = selfAppPackage;
        this.selfAppName = selfAppName;
        this.model = model;
        this.brand = brand;
        this.os = os;
        this.osVersion = osVersion;
    }

    public String getSelfAppPackage() {
        return selfAppPackage;
    }

    public void setSelfAppPackage(String selfAppPackage) {
        this.selfAppPackage = selfAppPackage;
    }

    public String getSelfAppName() {
        return selfAppName;
    }

    public void setSelfAppName(String selfAppName) {
        this.selfAppName = selfAppName;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getBrand() {
        return brand;
    }

    public void setBrand(String brand) {
        this.brand = brand;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getOsVersion() {
        return osVersion;
    }

    public void setOsVersion(String osVersion) {
        this.osVersion = osVersion;
    }

    public boolean isComplete() {
        return selfAppPackage != null
                && selfAppName != null
                && model != null
                && brand != null
                && os != null
                && osVersion != null;
    }
}
