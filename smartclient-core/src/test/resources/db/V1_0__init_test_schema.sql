CREATE TABLE simpleentity
(
    id      INT AUTO_INCREMENT,
    name    VARCHAR(40),

    CONSTRAINT pkSimpleEntity
        PRIMARY KEY (id)
);

CREATE TABLE countries(
    id          INT AUTO_INCREMENT,
    name     VARCHAR(40),

    CONSTRAINT pkCountries
        PRIMARY KEY (id)
);


CREATE TABLE locations(
    id          INT AUTO_INCREMENT,
    country_id  INT NOT NULL,
    city        VARCHAR(100),

    CONSTRAINT pkLocations
        PRIMARY KEY (id),

    CONSTRAINT fkLocations_Countries
        FOREIGN KEY (country_id) REFERENCES countries(id)

);

CREATE TABLE employee (
    id              INT AUTO_INCREMENT,
    name            VARCHAR(100),
    email           VARCHAR(50),
    location_id     INT,
    firedAt         TIMESTAMP(6),
    deleted         BOOLEAN default false,

    CONSTRAINT pkEmployee
        PRIMARY KEY (id),

    CONSTRAINT fkEmployee_Locations
        FOREIGN KEY (location_id) REFERENCES locations(id)
);

CREATE TABLE employee_role
(
    employee_id     INT NOT NULL,
    role            ENUM ('EMPLOYEE', 'PM', 'AM', 'C-Level', 'FM', 'Admin', 'Developer') NOT NULL,

    CONSTRAINT pkEmployeeRole
        PRIMARY KEY  (employee_id, role),

    CONSTRAINT fkEmployeeId
        FOREIGN KEY (employee_id) REFERENCES employee (id),

    CONSTRAINT uqUserRole
        UNIQUE (employee_id, role)
);


CREATE TABLE client
(
    id              INT not null,
    name            VARCHAR(255) NOT NULL,

    CONSTRAINT pkClients
        PRIMARY KEY (id)
);

CREATE TABLE client_data
(
    id              INT NOT NULL,
    data            VARCHAR(255) NOT NULL,
    client_id       INT NOT NULL,

    CONSTRAINT pk_client_data
        PRIMARY KEY (id),

    CONSTRAINT fkClientData_ClientId
        FOREIGN KEY (client_id) REFERENCES CLIENT (id)
);

CREATE TABLE project
(
    id              INT NOT NULL,
    client_id       INT,
    manager_id      INT NULL,
    name            VARCHAR(255) NOT NULL,

    CONSTRAINT pkProjects
        PRIMARY KEY (id),

    CONSTRAINT fkProject_ClientId
        FOREIGN KEY (client_id) REFERENCES client (id),

    CONSTRAINT fkProject_ManagerId
        FOREIGN KEY (manager_id) REFERENCES employee (id)
);

CREATE TABLE project_team
(
    project_id      INT NOT NULL,
    employee_id     INT NOT NULL,

    CONSTRAINT pkProjectTeam
        PRIMARY KEY (project_id, employee_id),

    CONSTRAINT fkProjectTeam_Project
        FOREIGN KEY (project_id) REFERENCES project (id),

    CONSTRAINT fkProjectTeam_Employee
        FOREIGN KEY (employee_id) REFERENCES employee (id)
);

CREATE TABLE employee_status
(
    id              INT auto_increment,
    employee_id     INT NOT NULL,
    status          VARCHAR(255) NULL,

    start_date      DATE NOT NULL,
    end_date        DATE NULL,

    CONSTRAINT pkEmployeeStatus
        PRIMARY KEY (id),

    CONSTRAINT unqEmployee_Status
        UNIQUE (employee_id, start_date),

    CONSTRAINT fkEmployeeStatus_Employee
        FOREIGN KEY (employee_id) REFERENCES employee (id)
);


CREATE TABLE time_log_entry
(
    employee_id     INT NOT NULL,
    project_id      INT NOT NULL,

    work_date       DATE NOT NULL,
    minutes         INT NOT NULL,

    CONSTRAINT pkTimeLogEntry
        PRIMARY KEY (project_id, employee_id, work_date),

    CONSTRAINT fkTimeLogEntry_Project
        FOREIGN KEY (project_id) REFERENCES project (id),

    CONSTRAINT fkTimeLogEntry_Employee
        FOREIGN KEY (employee_id) REFERENCES employee (id)
);

INSERT INTO countries VALUES (1, 'Ukraine');
INSERT INTO countries VALUES (2, 'USA');

INSERT INTO locations VALUES (1, 1, 'Kharkiv');
INSERT INTO locations VALUES (2, 1, 'Lviv');
INSERT INTO locations VALUES (3, 2, 'USA');


INSERT INTO employee VALUES (1, 'admin', 'admin@acmE.org', 1, '2000-01-02 03:04:05', false);
INSERT INTO employee VALUES (2, 'developer', 'developer@acme.org', 2, null, false);
INSERT INTO employee VALUES (3, 'UseR3', 'u3@emca.org', 3, null, false);
INSERT INTO employee VALUES (6, 'user2', 'u2@emca.org', 3, null, false);
INSERT INTO employee VALUES (4, 'manager1', 'pm1@acmE.org', 1, null, false);
INSERT INTO employee VALUES (5, 'manager2', 'pm2@acme.org', 2, '2000-05-04 03:02:01', false);

INSERT INTO employee_role VALUES (1, 'Admin');
INSERT INTO employee_role VALUES (1, 'Developer');
INSERT INTO employee_role VALUES (2, 'Developer');
INSERT INTO employee_role VALUES (4, 'PM');
INSERT INTO employee_role VALUES (5, 'PM');


INSERT INTO client VALUES (1, 'client 1');
INSERT INTO client VALUES (2, 'client 2');

INSERT INTO client_data VALUES (1, 'Data1: client 1', 1);
INSERT INTO client_data VALUES (2, 'Data2: client 2', 2);

INSERT INTO project VALUES (1, 1, 4, 'Project 1 for client 1');
INSERT INTO project VALUES (2, 1, 4, 'Project 2 for client 1');
INSERT INTO project VALUES (3, 2, 5, 'Project 1 for client 2');
INSERT INTO project VALUES (4, 2, 5, 'Project 2 for client 2');
INSERT INTO project VALUES (5, 2, 5, 'Project 3 for client 2');

INSERT INTO project_team VALUES (1, 1);
INSERT INTO project_team VALUES (1, 2);

INSERT INTO project_team VALUES (2, 2);
INSERT INTO project_team VALUES (2, 3);

INSERT INTO project_team VALUES (3, 4);
INSERT INTO project_team VALUES (3, 5);

INSERT INTO employee_status VALUES (1, 1, 'status 1', '2000-05-04', '2000-06-04');
INSERT INTO employee_status VALUES (2, 1, 'status 2', '2000-06-05', '2000-07-05');
INSERT INTO employee_status VALUES (3, 1, 'status 3', '2000-07-06', null);


INSERT INTO time_log_entry VALUES (2, 1, '2020-03-06', 111);
INSERT INTO time_log_entry VALUES (2, 3, '2020-03-06', 222);
INSERT INTO time_log_entry VALUES (1, 1, '2020-03-07', 111);
INSERT INTO time_log_entry VALUES (1, 3, '2020-03-07', 222);


INSERT INTO time_log_entry VALUES ( 2, 1, '2020-03-11', 11);
INSERT INTO time_log_entry VALUES (2, 3, '2020-03-11', 22);
INSERT INTO time_log_entry VALUES (1, 1, '2020-03-12', 11);
INSERT INTO time_log_entry VALUES (1, 3, '2020-03-12', 22);

INSERT INTO time_log_entry VALUES (2, 1, '2020-03-12', 1);
INSERT INTO time_log_entry VALUES (2, 3, '2020-03-12', 2);
INSERT INTO time_log_entry VALUES (1, 1, '2020-03-13', 1);
INSERT INTO time_log_entry VALUES (1, 3, '2020-03-13', 2);
