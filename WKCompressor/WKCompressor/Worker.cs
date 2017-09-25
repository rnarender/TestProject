using Newtonsoft.Json;
using NLog;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace WKCompressor
{
    class Worker
    {
        readonly int BatchSize;
        int ConfigBatchSize;
        string ConnectionString;
        Process CurrentProcess;

        //long TotalRecords = 0;
        long ProcessedRecords = 0;
        //long FailedRecords = 0;
        long[] ExcludedInstanceIds = new long[0];
        long CurrentInstanceId = 0;
        long LastProcessedInstanceID;
        bool IsFinished = false;
        List<string> FailedInstanceIDs = new List<string>();
        private static Logger logger = LogManager.GetCurrentClassLogger();
        string InstanceDataFile;
        string newColumnName;
        CurrentInstanceData instData = null;

        public Worker()
        {
            BatchSize = int.Parse(ConfigurationManager.AppSettings["BatchSize"]);
            ConfigBatchSize = int.Parse(ConfigurationManager.AppSettings["ConfigBatchSize"]);
            newColumnName = ConfigurationManager.AppSettings["NewColumnName"];
            InstanceDataFile = ConfigurationManager.AppSettings["InstanceDataFile"];
            
            var TargetConnection = ConfigurationManager.ConnectionStrings["Default"];
            if (TargetConnection != null && !string.IsNullOrWhiteSpace(TargetConnection.ConnectionString))
                ConnectionString = TargetConnection.ConnectionString;

            var ExcludedIds = ConfigurationManager.AppSettings["ExcludeInstanceIds"];
            if (!string.IsNullOrWhiteSpace(ExcludedIds))
                ExcludedInstanceIds = Array.ConvertAll(ExcludedIds.Split(','), long.Parse);

            CurrentProcess = Process.GetCurrentProcess();
        }

        public bool CompressAllRecords()
        {
            var Result = false;
            try
            {
                // check if the new column is added or not
                if(!IsSimulationRecordStoreNewColumnAdded())
                {
                    logger.Log(LogLevel.Trace, $" The NEW COLUMN: {newColumnName} is not added to table, please updated the table definition");
                    return true;
                }

                // will get the last processed instance id from file
                if (File.Exists(InstanceDataFile))
                {
                    var fileContent = File.ReadAllText(InstanceDataFile);
                    instData = JsonConvert.DeserializeObject<CurrentInstanceData>(fileContent);
                }

                // check if the Compression is finished for all the record in Database
                if (instData != null && instData.IsFinished)
                {
                    logger.Log(LogLevel.Trace, " The Compression of the all the DB records in done");
                    ProcessFailedIntanceIDs();
                    return true;
                }

                if (instData != null && instData.LastInstanceID != 0)
                    CurrentInstanceId = instData.LastInstanceID;

                // cache all error out instance ID's
                if (instData != null)
                    FailedInstanceIDs.AddRange(instData.FailedIntanceIDs.Split(',').ToList<string>());

                while (ProcessedRecords < ConfigBatchSize)
                {
                    var BatchRecords = FetchRecords();
                    var BatchUpdates = BatchRecords.Select(r => ProcessRecords(r)).ToArray();
                    Task.WaitAll(BatchUpdates);
                    ProcessedRecords += BatchSize;
                }
                if (IsCompressionFinished())
                    IsFinished = true;

                    Result = true;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
            finally
            {
                // log the InstanceData to file..
                CurrentInstanceData instData = new CurrentInstanceData { FailedIntanceIDs = String.Join(",", FailedInstanceIDs.ToArray()), LastInstanceID = LastProcessedInstanceID, IsFinished= IsFinished };
                var fileContent = JsonConvert.SerializeObject(instData);
                File.WriteAllText(InstanceDataFile, fileContent);
            }
            return Result;
        }

        public bool CompressFailedRecords()
        {
            var Result = false;
            var fileContent = File.ReadAllText("InstanceData.txt");
            var instData = JsonConvert.DeserializeObject<CurrentInstanceData>(fileContent);
            return Result;
        }

        //void GetTotalRecordCount()
        //{
        //    Console.WriteLine("Fetching total record count");
        //    var Query = "SELECT COUNT(1) FROM UserAssignments";

        //    using (var Connection = new SqlConnection(ConnectionString))
        //    using (var Command = new SqlCommand(Query, Connection))
        //    {
        //        Connection.Open();
        //        TotalRecords = Convert.ToInt64(Command.ExecuteScalar());
        //    }
        //    Console.WriteLine("Got total record count as " + TotalRecords);
        //}

        IEnumerable<Tuple<long, string>> FetchRecords(bool isProcessFailed = false)
        {
            Console.WriteLine($"Fetching {BatchSize} records");
            Console.WriteLine("Current application memory in MB : " + CurrentProcess.WorkingSet64 / 1000000);

            var Data = new List<Tuple<long, string>>();
            var Query = string.Empty;

            if (ExcludedInstanceIds.Length > 0)
            {
                if (isProcessFailed)
                    Query = $"SELECT InstanceId, SimulationRecordsStoreDocument FROM UserAssignments WHERE InstanceId in ({instData.FailedIntanceIDs}) AND InstanceId NOT IN ({string.Join(",", ExcludedInstanceIds)})";
                else
                    Query = $"SELECT TOP {BatchSize} InstanceId, SimulationRecordsStoreDocument FROM UserAssignments WHERE InstanceId > {CurrentInstanceId} AND InstanceId NOT IN ({string.Join(",", ExcludedInstanceIds)})";
            }
            else
            {
                if (isProcessFailed)
                    Query = $"SELECT InstanceId, SimulationRecordsStoreDocument FROM UserAssignments WHERE InstanceId in ({instData.FailedIntanceIDs})";
                else
                    Query = $"SELECT TOP {BatchSize} InstanceId, SimulationRecordsStoreDocument FROM UserAssignments WHERE InstanceId > {CurrentInstanceId}";
            }

            using (var Connection = new SqlConnection(ConnectionString))
            using (var Adapter = new SqlDataAdapter(Query, Connection))
            using (var DS = new DataSet())
            {
                Adapter.Fill(DS);

                Data.AddRange(DS.Tables[0].AsEnumerable()
                    .Select(r => new Tuple<long, string>(Convert.ToInt64(r.ItemArray[0]), r.Field<string>("SimulationRecordsStoreDocument"))));
            }

            CurrentInstanceId = Data.OrderByDescending(a => a.Item1).Select(a => a.Item1).FirstOrDefault();

            Console.WriteLine($"Fetched {Data.Count} records");
            logger.Log(LogLevel.Trace, $"Fetched {Data.Count} records");
            Console.WriteLine($"Last InstanceId is {CurrentInstanceId}");
            logger.Log(LogLevel.Trace, $"Last InstanceId is {CurrentInstanceId}");

            return Data;
        }

        async Task ProcessRecords(Tuple<long, string> record)
        {
            Console.WriteLine("Processing InstanceId : " + record.Item1);
            logger.Log(LogLevel.Trace, "Processing InstanceId : " + record.Item1);
            var Data = record.Item2;
            var TrimmedData = Data.Trim();
            CurrentInstanceId = record.Item1;

            if (!(TrimmedData.Length % 4 == 0 && Regex.IsMatch(TrimmedData, @"^[a-zA-Z0-9\+/]*={0,3}$", RegexOptions.None)))
            {
                var CompressedData = StringCompressor.Compress(Data);
                try
                {
                    using (var Connection = new SqlConnection(ConnectionString))
                    using (var Command = new SqlCommand($"UPDATE UserAssignments SET {newColumnName} = '{CompressedData}' WHERE InstanceId = {record.Item1}", Connection))
                    {
                        if (Connection.State == ConnectionState.Closed)
                            Connection.Open();

                        await Command.ExecuteNonQueryAsync();
                        LastProcessedInstanceID = record.Item1;
                        Console.WriteLine("Finished processing InstanceId : " + record.Item1);
                        logger.Log(LogLevel.Trace, "Finished processing InstanceId : " + record.Item1);
                    }
                }
                catch (Exception ex)
                {
                    // store the instanceID into list
                    FailedInstanceIDs.Add(CurrentInstanceId.ToString());
                    logger.Log(LogLevel.Error, ex.Message);
                    Console.WriteLine(ex.Message);
                    Console.WriteLine(ex.StackTrace);
                }
            }
        }

        public bool IsSimulationRecordStoreNewColumnAdded()
        {
            bool result = false;
            var Query = $"SELECT * FROM information_schema.columns WHERE column_name = '{newColumnName}' AND table_name = 'UserAssignments'";

            using (var Connection = new SqlConnection(ConnectionString))
            using (var Adapter = new SqlDataAdapter(Query, Connection))
            using (var DS = new DataSet())
            {
                Adapter.Fill(DS);
                if (DS.Tables.Count > 0 && DS.Tables[0].Rows.Count > 0)
                    result = true;
            }

            return result;
        }

        public bool IsCompressionFinished()
        {
            bool result = false;
            var Query = "SELECT SimulationRecordsStoreDocument1 from userAssignments WHERE instanceId = (SELECT MAX(instanceId) from userAssignments)";

            using (var Connection = new SqlConnection(ConnectionString))
            using (var Adapter = new SqlDataAdapter(Query, Connection))
            using (var DS = new DataSet())
            {
                Adapter.Fill(DS);
                if (DS.Tables.Count > 0 && DS.Tables[0].Rows.Count > 0 && DS.Tables[0].Rows[0]["simulationRecordsStoreDocument1"].ToString() != "")
                    result = true;
            }

            return result;
        }

        public void ProcessFailedIntanceIDs()
        {
            if (instData.FailedIntanceIDs != string.Empty)
            {
                logger.Log(LogLevel.Trace, " Processing Failed Records ");
                var BatchRecords = FetchRecords(true);
                var BatchUpdates = BatchRecords.Select(r => ProcessRecords(r)).ToArray();
                Task.WaitAll(BatchUpdates);
            }
        }

        #region DecompressLast10RecordsDELETE AFTER TESTING

        IEnumerable<Tuple<long, string>> FetchLast10Records()
        {
            Console.WriteLine($"Fetching {BatchSize} records");
            Console.WriteLine("Current application memory in MB : " + CurrentProcess.WorkingSet64 / 1000000);

            var Data = new List<Tuple<long, string>>();
            //var Query = $"select top 10 InstanceId, SimulationRecordsStoreDocument from UserAssignments order by InstanceId desc";
            var Query = $"select InstanceId, SimulationRecordsStoreDocument from UserAssignments  where InstanceId = 1729723";
            using (var Connection = new SqlConnection(ConnectionString))
            using (var Adapter = new SqlDataAdapter(Query, Connection))
            using (var DS = new DataSet())
            {
                Adapter.Fill(DS);

                Data.AddRange(DS.Tables[0].AsEnumerable()
                    .Select(r => new Tuple<long, string>(Convert.ToInt64(r.ItemArray[0]), r.Field<string>("SimulationRecordsStoreDocument"))));
            }

            CurrentInstanceId = Data.OrderByDescending(a => a.Item1).Select(a => a.Item1).FirstOrDefault();

            Console.WriteLine($"Fetched {Data.Count} records");
            logger.Log(LogLevel.Trace, $"Fetched {Data.Count} records");
            Console.WriteLine($"Last InstanceId is {CurrentInstanceId}");
            logger.Log(LogLevel.Trace, $"Last InstanceId is {CurrentInstanceId}");

            return Data;
        }

        async Task ProcessRecordsLast10Records(Tuple<long, string> record)
        {
            Console.WriteLine("Processing InstanceId : " + record.Item1);
            logger.Log(LogLevel.Trace, "Processing InstanceId : " + record.Item1);
            var Data = record.Item2;
            var TrimmedData = Data.Trim();
            CurrentInstanceId = record.Item1;

            //if (!(TrimmedData.Length % 4 == 0 && Regex.IsMatch(TrimmedData, @"^[a-zA-Z0-9\+/]*={0,3}$", RegexOptions.None)))
            {
                var DeCompressedData = StringCompressor.Decompress(Data);
                try
                {
                    using (var Connection = new SqlConnection(ConnectionString))
                    using (var Command = new SqlCommand($"UPDATE UserAssignments SET SimulationRecordsStoreDocument = '{DeCompressedData}' WHERE InstanceId = {record.Item1}", Connection))
                    {
                        if (Connection.State == ConnectionState.Closed)
                            Connection.Open();

                        await Command.ExecuteNonQueryAsync();
                        LastProcessedInstanceID = record.Item1;
                        Console.WriteLine("Finished processing InstanceId : " + record.Item1);
                        logger.Log(LogLevel.Trace, "Finished processing InstanceId : " + record.Item1);
                    }
                }
                catch (Exception ex)
                {
                    // store the instanceID into list
                    FailedInstanceIDs.Add(CurrentInstanceId.ToString());
                    logger.Log(LogLevel.Error, ex.Message);
                    Console.WriteLine(ex.Message);
                    Console.WriteLine(ex.StackTrace);
                }
            }
        }

        public void processLast10Records()
        {
            var BatchRecords = FetchLast10Records();
            var BatchUpdates = BatchRecords.Select(r => ProcessRecordsLast10Records(r)).ToArray();
            Task.WaitAll(BatchUpdates);
        }
        #endregion
    }
}
