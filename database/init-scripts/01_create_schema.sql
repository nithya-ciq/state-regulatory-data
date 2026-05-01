-- Initialize the jurisdiction schema for the Alcohol Licensing Jurisdiction Taxonomy
CREATE SCHEMA IF NOT EXISTS jurisdiction;

-- Grant usage to the application user
GRANT ALL ON SCHEMA jurisdiction TO jurisdiction_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA jurisdiction GRANT ALL ON TABLES TO jurisdiction_user;
ALTER DEFAULT PRIVILEGES IN SCHEMA jurisdiction GRANT ALL ON SEQUENCES TO jurisdiction_user;
