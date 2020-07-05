package com.edureka.futurecart.stream.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "status",
        "category",
        "sub_category",
        "last_modified_timestamp",
        "case_no",
        "create_timestamp",
        "created_employee_key",
        "call_center_id",
        "product_code",
        "country_cd",
        "communication_mode"
})
public class CaseRecord {

    @JsonProperty("status")
    private String status;
    @JsonProperty("category")
    private String category;
    @JsonProperty("sub_category")
    private String subCategory;
    @JsonProperty("last_modified_timestamp")
    private String lastModifiedTimestamp;
    @JsonProperty("case_no")
    private String caseNo;
    @JsonProperty("create_timestamp")
    private String createTimestamp;
    @JsonProperty("created_employee_key")
    private String createdEmployeeKey;
    @JsonProperty("call_center_id")
    private String callCenterId;
    @JsonProperty("product_code")
    private String productCode;
    @JsonProperty("country_cd")
    private String countryCd;
    @JsonProperty("communication_mode")
    private String communicationMode;

    @JsonProperty("status")
    public String getStatus() {
        return status;
    }

    @JsonProperty("status")
    public void setStatus(String status) {
        this.status = status;
    }

    @JsonProperty("category")
    public String getCategory() {
        return category;
    }

    @JsonProperty("category")
    public void setCategory(String category) {
        this.category = category;
    }

    @JsonProperty("sub_category")
    public String getSubCategory() {
        return subCategory;
    }

    @JsonProperty("sub_category")
    public void setSubCategory(String subCategory) {
        this.subCategory = subCategory;
    }

    @JsonProperty("last_modified_timestamp")
    public String getLastModifiedTimestamp() {
        return lastModifiedTimestamp;
    }

    @JsonProperty("last_modified_timestamp")
    public void setLastModifiedTimestamp(String lastModifiedTimestamp) {
        this.lastModifiedTimestamp = lastModifiedTimestamp;
    }

    @JsonProperty("case_no")
    public String getCaseNo() {
        return caseNo;
    }

    @JsonProperty("case_no")
    public void setCaseNo(String caseNo) {
        this.caseNo = caseNo;
    }

    @JsonProperty("create_timestamp")
    public String getCreateTimestamp() {
        return createTimestamp;
    }

    @JsonProperty("create_timestamp")
    public void setCreateTimestamp(String createTimestamp) {
        this.createTimestamp = createTimestamp;
    }

    @JsonProperty("created_employee_key")
    public String getCreatedEmployeeKey() {
        return createdEmployeeKey;
    }

    @JsonProperty("created_employee_key")
    public void setCreatedEmployeeKey(String createdEmployeeKey) {
        this.createdEmployeeKey = createdEmployeeKey;
    }

    @JsonProperty("call_center_id")
    public String getCallCenterId() {
        return callCenterId;
    }

    @JsonProperty("call_center_id")
    public void setCallCenterId(String callCenterId) {
        this.callCenterId = callCenterId;
    }

    @JsonProperty("product_code")
    public String getProductCode() {
        return productCode;
    }

    @JsonProperty("product_code")
    public void setProductCode(String productCode) {
        this.productCode = productCode;
    }

    @JsonProperty("country_cd")
    public String getCountryCd() {
        return countryCd;
    }

    @JsonProperty("country_cd")
    public void setCountryCd(String countryCd) {
        this.countryCd = countryCd;
    }

    @JsonProperty("communication_mode")
    public String getCommunicationMode() {
        return communicationMode;
    }

    @JsonProperty("communication_mode")
    public void setCommunicationMode(String communicationMode) {
        this.communicationMode = communicationMode;
    }


}