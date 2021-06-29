package org.srg.smartclient.jpa;

import org.srg.smartclient.annotations.SmartClientField;
import org.srg.smartclient.annotations.SmartClientHandler;

import javax.persistence.*;

@SmartClientHandler(loadOrder = 2)
@Entity
@Table(name = "employee_status", uniqueConstraints = @UniqueConstraint( name = "unqEmployeeStatus", columnNames = {"employee_id", "start_date"}))
@AssociationOverride(
        name = "owner",
        joinColumns = @JoinColumn(name = "employee_id", nullable = false),
        foreignKey = @ForeignKey(name="fkEmployeeStatus_Employee")
)
public class EmployeeStatus extends HistoricalEntity<Employee> {
    private String status;

    @SmartClientField(foreignDisplayField = "name")
    @Override
    public Employee getOwner() {
        return super.getOwner();
    }

    @SmartClientField(canEdit = true)
    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
