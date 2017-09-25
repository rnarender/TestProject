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
    class WorkerOne
    {

        #region Fields
        readonly int BatchSize;
        int ConfigBatchSize;
        string ConnectionString;
        long ProcessedRecords = 0;
        long[] ExcludedInstanceIds = new long[0];
        long CurrentInstanceId = 0;
        long LastProcessedInstanceID;
        bool IsFinished = false;
        List<string> FailedInstanceIDs = new List<string>();
        private static Logger logger = LogManager.GetCurrentClassLogger();
        string InstanceDataFile;
        string newColumnName;
        CurrentInstanceData instData = null;
        #endregion

        public WorkerOne()
        {
            BatchSize = int.Parse(ConfigurationManager.AppSettings["BatchSize"]);
            ConfigBatchSize = int.Parse(ConfigurationManager.AppSettings["ConfigBatchSize"]);
            newColumnName = ConfigurationManager.AppSettings["NewColumnName"];
            InstanceDataFile = ConfigurationManager.AppSettings["InstanceDataFile"];
            var TargetConnection = ConfigurationManager.ConnectionStrings["Default"];
            var ExcludedIds = ConfigurationManager.AppSettings["ExcludeInstanceIds"];

            if (TargetConnection != null && !string.IsNullOrWhiteSpace(TargetConnection.ConnectionString))
                ConnectionString = TargetConnection.ConnectionString;

            if (!string.IsNullOrWhiteSpace(ExcludedIds))
                ExcludedInstanceIds = Array.ConvertAll(ExcludedIds.Split(','), long.Parse);
        }

        public bool CompressAllRecords()
        {
            var Result = false;
            try
            {
                // check if the new column is added or not
                if (!IsSimulationRecordStoreNewColumnAdded())
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
                    IsFinished = true; LastProcessedInstanceID = instData.LastInstanceID; FailedInstanceIDs = instData.FailedIntanceIDs.Split(',').ToList<string>();
                    return true;
                }

                if (instData != null && instData.LastInstanceID != 0)
                    CurrentInstanceId = instData.LastInstanceID;

                // cache all error out instance ID's
                if (instData != null)
                {
                    FailedInstanceIDs.Clear();
                    FailedInstanceIDs.AddRange(instData.FailedIntanceIDs.Split(',').ToList<string>());
                }

                while (ProcessedRecords < ConfigBatchSize)
                {
                    var BatchRecords = FetchRecords();
                    var BatchUpdates = BatchRecords.Select(r => ProcessRecords(r)).ToArray();
                    Task.WaitAll(BatchUpdates);
                    ProcessedRecords += BatchSize;
                }
                if (IsCompressionFinished())
                {
                    IsFinished = true;
                    logger.Log(LogLevel.Trace, " The Compression of the all the DB records in done");
                    ProcessFailedIntanceIDs(); // once all the records done migrating, then we can try with Failed records
                }
                Result = true;
            }
            catch (Exception ex)
            {
                logger.Log(LogLevel.Error, ex.Message);
                logger.Log(LogLevel.Error, ex.StackTrace);
            }
            finally
            {
                // log the InstanceData to file..
                CurrentInstanceData instData = new CurrentInstanceData { FailedIntanceIDs = String.Join(",", FailedInstanceIDs.ToArray()), LastInstanceID = LastProcessedInstanceID, IsFinished = IsFinished };
                var fileContent = JsonConvert.SerializeObject(instData);
                File.WriteAllText(InstanceDataFile, fileContent);
            }
            return Result;
        }

        IEnumerable<Tuple<long, string>> FetchRecords(bool isProcessFailed = false)
        {
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
            return Data;
        }

        async Task ProcessRecords(Tuple<long, string> record)
        {
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
                        logger.Log(LogLevel.Trace, "Processed InstanceId : " + record.Item1);
                    }
                }
                catch (Exception ex)
                {
                    FailedInstanceIDs.Add(CurrentInstanceId.ToString());
                    logger.Log(LogLevel.Error, ex.Message);
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
    }
}
