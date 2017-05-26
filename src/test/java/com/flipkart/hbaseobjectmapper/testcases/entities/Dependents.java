package com.flipkart.hbaseobjectmapper.testcases.entities;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.EqualsAndHashCode;
import lombok.ToString;

import java.io.Serializable;
import java.util.List;

@ToString
@EqualsAndHashCode
public class Dependents implements Serializable {
    @JsonProperty
    private Integer uidSpouse;
    @JsonProperty
    private List<Integer> uidChildren;

    public Dependents() {
    }

    public Dependents(Integer uidSpouse, List<Integer> uidChildren) {
        this.uidSpouse = uidSpouse;
        this.uidChildren = uidChildren;
    }
}
