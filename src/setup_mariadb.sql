-- MariaDB setup for local connection
CREATE USER IF NOT EXISTS 'ducklake_user'@'localhost' IDENTIFIED BY 'Nzy-f6y@brNF_6AFaC2MrZAU';
GRANT ALL PRIVILEGES ON *.* TO 'ducklake_user'@'localhost';
CREATE DATABASE IF NOT EXISTS AHRI_TRE;
GRANT ALL PRIVILEGES ON AHRI_TRE.* TO 'ducklake_user'@'localhost';
FLUSH PRIVILEGES;
