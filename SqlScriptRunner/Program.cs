using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SqlScriptRunner
{
    class Program
    {
        private static void PrintHelp()
        {
            /**********************************************
				Make notes on console screen
			**********************************************/
            Console.WriteLine("Usage: SqlScriptRunner -c <connection string> -l <output log file> [big script to be run.sql]");
            Console.WriteLine("Command line arguments:");
            Console.WriteLine("\t-c\tConnection string.");
            Console.WriteLine("\t-l\tOutput log file.");
        }

        private static void Log(TextWriter writer, String text, params object[] args)
        {
            String log = String.Format("{0:yyyy-MM-dd hh:mm:ss } {1}", DateTime.Now, String.Format(text, args));

            if (writer != null)
                writer.WriteLine(log);

            Console.WriteLine(log);
        }

        static void Main(string[] args)
        {
            ArgsParser argsParser = new ArgsParser(args);
            SqlConnection sqlConnection = null;
            TextWriter logger = null;
            StreamReader reader = null;
            Int64 counter = 0;

            if (argsParser.HasArg("h"))
            {
                PrintHelp();
                Environment.Exit(0);
            }

            try
            {
                // Try to create output log file
                if (argsParser.HasArg("l"))
                {
                    try
                    {
                        logger = new StreamWriter(argsParser.GetArg("l"));
                    }
                    catch (Exception ex)
                    {
                        Log(null, "Error opening log file {0}: {1}", argsParser.GetArg("l"), ex.Message);
                        Environment.Exit(1);
                    }
                }

                // Try to open SQL connection
                try
                {
                    sqlConnection = new SqlConnection(argsParser.GetArg("c"));
                    sqlConnection.Open();
                }
                catch (Exception ex)
                {
                    Log(logger, "Error opening connection to {0}: {1}", argsParser.GetArg("c"), ex.Message);
                    Environment.Exit(2);
                }

                // Try to open SQL script
                if (File.Exists(argsParser.GetArgs().FirstOrDefault()))
                {
                    try
                    {
                        reader = new StreamReader(argsParser.GetArgs().FirstOrDefault());
                    }
                    catch (Exception ex)
                    {
                        Log(logger, "Error opening script file {0}: {1}", argsParser.GetArgs().FirstOrDefault(), ex.Message);
                        Environment.Exit(4);
                    }
                }
                else
                {
                    Log(logger, "Script file {0} not found.", argsParser.GetArgs().FirstOrDefault());
                    Environment.Exit(4);
                }

                // Set up counters
                Int64 numberOfAffectedRows = 0;

                // Process Script File
                StringBuilder scriptData = new StringBuilder();
                string nextScriptDataLine = String.Empty;

                Log(logger, "Running...");

                try
                {
                    while ((nextScriptDataLine = reader.ReadLine()) != null)
                    {
                        if (nextScriptDataLine.Trim().EndsWith("GO"))
                        {
                            // Remove the GO command
                            nextScriptDataLine = nextScriptDataLine.Trim().Substring(0, nextScriptDataLine.Length - 2);
                            if (!String.IsNullOrWhiteSpace(nextScriptDataLine))
                            {
                                scriptData.AppendLine(nextScriptDataLine);
                            }
                            // Execute script
                            numberOfAffectedRows = ExecuteScript(sqlConnection, scriptData);
                            scriptData.Clear();
                            if (numberOfAffectedRows > 0)
                            {
                                Log(logger, "Added {0} rows.", numberOfAffectedRows);
                                counter += numberOfAffectedRows;
                            }
                        }
                        else
                        {
                            scriptData.AppendLine(nextScriptDataLine);
                        }
                    }

                    // Execute the rest of the data
                    numberOfAffectedRows = ExecuteScript(sqlConnection, scriptData);
                    if (numberOfAffectedRows > 0)
                    {
                        Log(logger, "Added {0} rows.", numberOfAffectedRows);
                        counter += numberOfAffectedRows;
                    }
                }
                catch (Exception ex)
                {
                    Log(logger, "Error executing command: {0}", ex.Message);
                    Environment.Exit(5);
                }
            }
            finally
            {
                try
                {
                    Log(logger, "Finished adding {0} rows.", counter);

                    if (sqlConnection != null)
                        sqlConnection.Close();

                    if (reader != null)
                        reader.Close();

                    if (logger != null)
                        logger.Close();
                }
                catch
                {
                }
            }
        }

        private static long ExecuteScript(SqlConnection sqlConnection, StringBuilder scriptData)
        {
            String script = scriptData.ToString();
            if (!String.IsNullOrWhiteSpace(script))
            {
                SqlCommand sqlCommand = new SqlCommand(script, sqlConnection);
                sqlCommand.CommandTimeout = 0;
                return sqlCommand.ExecuteNonQuery();
            }
            return 0;
        }
    }
}
