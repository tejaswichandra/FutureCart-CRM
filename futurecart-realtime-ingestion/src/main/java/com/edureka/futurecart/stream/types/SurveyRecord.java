package com.edureka.futurecart.stream.types;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;


@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
        "Q1",
        "Q3",
        "Q2",
        "Q5",
        "Q4",
        "case_no",
        "survey_timestamp",
        "survey_id"
})
public class SurveyRecord {
    @JsonProperty("Q1")
    private Integer q1;
    @JsonProperty("Q3")
    private Integer q3;
    @JsonProperty("Q2")
    private Integer q2;
    @JsonProperty("Q5")
    private Integer q5;
    @JsonProperty("Q4")
    private String q4;
    @JsonProperty("case_no")
    private String caseNo;
    @JsonProperty("survey_timestamp")
    private String surveyTimestamp;
    @JsonProperty("survey_id")
    private String surveyId;

    @JsonProperty("Q1")
    public Integer getQ1() {
        return q1;
    }

    @JsonProperty("Q1")
    public void setQ1(Integer q1) {
        this.q1 = q1;
    }

    @JsonProperty("Q3")
    public Integer getQ3() {
        return q3;
    }

    @JsonProperty("Q3")
    public void setQ3(Integer q3) {
        this.q3 = q3;
    }

    @JsonProperty("Q2")
    public Integer getQ2() {
        return q2;
    }

    @JsonProperty("Q2")
    public void setQ2(Integer q2) {
        this.q2 = q2;
    }

    @JsonProperty("Q5")
    public Integer getQ5() {
        return q5;
    }

    @JsonProperty("Q5")
    public void setQ5(Integer q5) {
        this.q5 = q5;
    }

    @JsonProperty("Q4")
    public String getQ4() {
        return q4;
    }

    @JsonProperty("Q4")
    public void setQ4(String q4) {
        this.q4 = q4;
    }

    @JsonProperty("case_no")
    public String getCaseNo() {
        return caseNo;
    }

    @JsonProperty("case_no")
    public void setCaseNo(String caseNo) {
        this.caseNo = caseNo;
    }

    @JsonProperty("survey_timestamp")
    public String getSurveyTimestamp() {
        return surveyTimestamp;
    }

    @JsonProperty("survey_timestamp")
    public void setSurveyTimestamp(String surveyTimestamp) {
        this.surveyTimestamp = surveyTimestamp;
    }

    @JsonProperty("survey_id")
    public String getSurveyId() {
        return surveyId;
    }

    @JsonProperty("survey_id")
    public void setSurveyId(String surveyId) {
        this.surveyId = surveyId;
    }

}

