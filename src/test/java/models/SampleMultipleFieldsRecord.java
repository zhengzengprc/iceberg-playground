package models;

import org.apache.iceberg.relocated.com.google.common.base.Objects;

public class SampleMultipleFieldsRecord {
    private Integer studentID;
    private String name;
    private String course;

    private double gpa;

    public SampleMultipleFieldsRecord() {
    }

    public SampleMultipleFieldsRecord(Integer studentID, String name, String course, double gpa) {
        this.studentID = studentID;
        this.name = name;
        this.course = course;
        this.gpa = gpa;
    }

    public Integer getStudentID() {
        return studentID;
    }

    public void setStudentID(Integer studentID) {
        this.studentID = studentID;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCourse() {
        return course;
    }

    public void setCourse(String course) {
        this.course = course;
    }

    public double getGpa() {
        return gpa;
    }

    public void setGpa(double gpa) {
        this.gpa = gpa;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SampleMultipleFieldsRecord record = (SampleMultipleFieldsRecord) o;
        return Objects.equal(studentID, record.studentID) && Objects.equal(name, record.name) && Objects.equal(course, record.course) && Objects.equal(gpa, record.gpa);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(studentID, name, course, gpa);
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();
        buffer.append("SampleMultipleFieldsRecord{");
        buffer.append("studentID=").append(studentID);
        buffer.append(", name='").append(name).append('\'');
        buffer.append(", course='").append(course).append('\'');
        buffer.append(", gpa=").append(gpa);
        buffer.append('}');
        return buffer.toString();
    }
}
