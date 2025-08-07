using ConfigEnv

# Load environment variables
dotenv()

println("Setting up local MariaDB connection...")
println("This script will help you create the database user for local access.")

# Get the required details
db_user = ENV["TRE_USER"]
db_password = ENV["TRE_PWD"]
db_name = ENV["TRE_DBNAME"]

println("\nDatabase setup details:")
println("User: $db_user")
println("Database: $db_name")
println("Password: [HIDDEN]")

println("\nTo set up the database user, run these MySQL commands as root:")
println("sudo mysql -u root")
println()
println("Then execute these SQL commands:")
println("-- Create user if it doesn't exist")
println("CREATE USER IF NOT EXISTS '$db_user'@'localhost' IDENTIFIED BY '$db_password';")
println()
println("-- Grant all privileges")
println("GRANT ALL PRIVILEGES ON *.* TO '$db_user'@'localhost';")
println()
println("-- Create database if it doesn't exist")
println("CREATE DATABASE IF NOT EXISTS $db_name;")
println()
println("-- Grant specific privileges on the database")
println("GRANT ALL PRIVILEGES ON $db_name.* TO '$db_user'@'localhost';")
println()
println("-- Refresh privileges")
println("FLUSH PRIVILEGES;")
println()
println("-- Exit MySQL")
println("EXIT;")

println("\nAlternatively, save this to a file and run it:")
sql_commands = """
-- MariaDB setup for local connection
CREATE USER IF NOT EXISTS '$db_user'@'localhost' IDENTIFIED BY '$db_password';
GRANT ALL PRIVILEGES ON *.* TO '$db_user'@'localhost';
CREATE DATABASE IF NOT EXISTS $db_name;
GRANT ALL PRIVILEGES ON $db_name.* TO '$db_user'@'localhost';
FLUSH PRIVILEGES;
"""

open("setup_mariadb.sql", "w") do file
    write(file, sql_commands)
end

println("SQL commands saved to: setup_mariadb.sql")
println("Run with: sudo mysql -u root < setup_mariadb.sql")
