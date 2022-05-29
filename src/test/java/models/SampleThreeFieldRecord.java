package models;

import org.apache.iceberg.relocated.com.google.common.base.Objects;

public class SampleThreeFieldRecord {
    private Integer employeeId;
    private String name;
    private Double baseSalary;

    public SampleThreeFieldRecord() {
    }

    public SampleThreeFieldRecord(Integer employeeId, String name, Double baseSalary) {
        this.employeeId = employeeId;
        this.name = name;
        this.baseSalary = baseSalary;
    }

    public Integer getEmployeeId() {
        return employeeId;
    }

    public void setEmployeeId(Integer employeeId) {
        this.employeeId = employeeId;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Double getBaseSalary() {
        return baseSalary;
    }

    public void setBaseSalary(Double baseSalary) {
        this.baseSalary = baseSalary;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        SampleThreeFieldRecord record = (SampleThreeFieldRecord) o;
        return Objects.equal(employeeId, record.employeeId) && Objects.equal(name, record.name) && Objects.equal(baseSalary, record.baseSalary);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(employeeId, name, baseSalary);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("{\"employeeId\"=");
        buffer.append(employeeId);
        buffer.append(",\"name\"=\"");
        buffer.append(name);
        buffer.append("\",\"baseSalary\"=");
        buffer.append(baseSalary);
        buffer.append("}");
        return buffer.toString();
    }
}
