using AHRI_TRE
using ConfigEnv
using Logging, LoggingExtras

using DBInterface
using DataFrames
using Dates


#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/create_database.log", "w")) do io, args
    # Write the module, level and message only
    println(io, args._module, " | ", "[", args.level, "] ", args.message)
end
minlogger = MinLevelLogger(logger, Logging.Info)
old_logger = global_logger(minlogger)

try
  println("Execution started at: ", Dates.now())
  createdatabase(ENV["TRE_SERVER"], ENV["TRE_USER"], ENV["TRE_PWD"], ENV["TRE_DBNAME"], replace = true)
finally
    global_logger(old_logger)  # Restore the old logger
end