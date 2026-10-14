package com.example.flinkprocesser;

import java.io.Serializable;

/**
 * 按设备启动周期保存在 keyed state 中、供后续事件补全的六项 common 元数据。
 *
 * <p>空白和非法类型在 JSON 输入边界归一化为 null；六字段均非 null 才能作为可用状态。</p>
 */
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

    /**
     * 判断六字段是否全部非 null，供状态补全算子决定能否保存或使用 common 状态。
     *
     * <p>此处只检查空值，依赖 JSON 提取边界提前完成空白值归一化。</p>
     */
    public boolean isComplete() {
        return selfAppPackage != null
                && selfAppName != null
                && model != null
                && brand != null
                && os != null
                && osVersion != null;
    }
}
