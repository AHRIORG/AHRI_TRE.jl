using AHRI_TRE
using ConfigEnv
using Logging, LoggingExtras

#get environment variables
dotenv()

#region Setup Logging
logger = FormatLogger(open("logs/new_store.log", "w")) do io, args
  # Write the module, level and message only
  println(io, args._module, " | ", "[", args.level, "] ", args.message)
end
minlogger = MinLevelLogger(logger, Logging.Info)
old_logger = global_logger(minlogger)

start_time = Dates.now()

