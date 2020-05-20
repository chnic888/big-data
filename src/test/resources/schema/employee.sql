create table employee
(
	id int auto_increment,
	employee_no int not null,
	first_name varchar(128) not null,
	title varchar(32) not null,
	state varchar(8) not null,
	laptop varchar(8) not null,
	active boolean not null,
	comment varchar(255) null,
	constraint employee_pk
		primary key (id)
);

INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (10, 'Andrew', 'Manager', 'DE', 'PC', 1, 'Marketing');
INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (11, 'Arun', 'Manager', 'NJ', 'PC', 1, 'HRBP');
INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (12, 'Harish', 'Sales', 'NJ', 'MAC', 0, 'Sales');
INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (13, 'Robert', 'Manager', 'PA', 'MAC', 1, null);
INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (14, 'Laura', 'Engineer', 'PA', 'MAC', 1, 'IT');
INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (15, 'Anju', 'CEO', 'PA', 'PC', 0, 'HQ');
INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (16, 'Aarathi', 'Manager', 'NJ', 'PC', 0, null);
INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (17, 'Parvathy', 'Engineer', 'DE', 'MAC', 0, 'IT');
INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (18, 'Gopika', 'Admin', 'DE', 'MAC', 1, 'IT');
INSERT INTO test.employee (employee_no, first_name, title, state, laptop, active, comment) VALUES (19, 'Steven', 'Engineer', 'PA', 'MAC', 1, 'IT');