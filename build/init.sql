CREATE USER 'accessuser'@'%' IDENTIFIED BY 'accesspwd';
CREATE DATABASE cache_db;
GRANT ALL PRIVILEGES ON cache_db.* TO 'accessuser'@'%';
USE cache_db;

-- Create tables
CREATE TABLE cache_eutils (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  base64_data VARCHAR(255) UNIQUE,
  cached_value TEXT NOT NULL
);

CREATE TABLE cache_pmids (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  pmid BIGINT UNIQUE,
  cached_value TEXT NOT NULL
);

CREATE TABLE pmid_errors (
  id INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  pmid BIGINT UNIQUE,
  num_retry INT NOT NULL
);

-- Prevent accident drop on tables
REVOKE DROP ON cache_db.* FROM 'accessuser'@'%';
FLUSH PRIVILEGES;

