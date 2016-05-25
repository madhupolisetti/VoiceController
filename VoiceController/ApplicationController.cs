﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;

namespace VoiceController
{
    internal class ApplicationController
    {
        private bool isIamPolling = false;
        private Thread pollThread = null;

        public ApplicationController()
        {
            this.LoadConfig();
        }

        public void Start()
        {
            SharedClass.IsServiceCleaned = false;
            this.LoadGateways();
            if (SharedClass.RabbitMQClient != null) {
                SharedClass.Logger.Info("Starting RabbitMQ Client");
                SharedClass.RabbitMQClient.Start();
            }
            if (SharedClass.HangupProcessor != null) {
                SharedClass.Logger.Info("Starting Hangup Processor");
                SharedClass.HangupProcessor.Start();
            }
            if (SharedClass.Listener.Ip.Length > 7 && SharedClass.Listener.Port > 0)
            {
                SharedClass.Logger.Info("Starting Listener");
                SharedClass.Listener.Initialize();
            } 
            this.pollThread = new Thread(new ThreadStart(this.StartDbPoll));
            this.pollThread.Name = "BulkPoller";
            SharedClass.Logger.Info("Starting BulkPoller");
            this.pollThread.Start();
        }

        public void Stop()
        {
            if (SharedClass.RabbitMQClient != null) {
                SharedClass.RabbitMQClient.Stop();
            }
            while (SharedClass.IsHangupConsumerRunning)
            {
                SharedClass.Logger.Info("Hangup Subscriber Not Yet Stopped");
                Thread.Sleep(1000);
            }
            while (SharedClass.IsCallFlowsConsumerRunning)
            {
                SharedClass.Logger.Info("CallFlows Subscriber Not Yet Stopped");
                Thread.Sleep(1000);
            }
            if (SharedClass.HangupProcessor != null) {
                SharedClass.HangupProcessor.Stop();
            }            
            foreach (KeyValuePair<long, AccountProcessor> keyValuePair in SharedClass.ActiveAccountProcessors)
            {
                keyValuePair.Value.Stop();
            }
            foreach (KeyValuePair<int, Gateway> keyValuePair in SharedClass.GatewayMap) {
                Thread gatewayStopThread = new Thread(new ThreadStart(keyValuePair.Value.Stop));
                gatewayStopThread.Name = keyValuePair.Value.Name + "_Stop";
                gatewayStopThread.Start();
            }
            while (SharedClass.GatewayMap.Count > 0)
            {
                SharedClass.Logger.Info(("Gateways Object Not Yet Cleaned, Active Gateways : " + SharedClass.GatewayMap.Count.ToString()));
                SharedClass.Logger.Info("GatewayId : " + SharedClass.GatewayMap.First().Value.Id);
                SharedClass.Logger.Info("Push Threads Running : " + SharedClass.GatewayMap.First().Value.PushThreadsRunning);                
                Thread.Sleep(1000);
            }
            if (SharedClass.Listener != null) {
                SharedClass.Listener.Destroy();
            }
            while (this.isIamPolling)
            {
                SharedClass.Logger.Info("DbPoller Is Still Running, Thread State : " + this.pollThread.ThreadState.ToString());
                if (this.pollThread.ThreadState == ThreadState.WaitSleepJoin)
                    this.pollThread.Interrupt();
                Thread.Sleep(100);
            }
            SharedClass.IsServiceCleaned = true;
        }

        public void StartDbPoll()
        {
            long lastRequestId = 0;
            SharedClass.Logger.Info("Started");
            SqlCommand sqlCommand = new SqlCommand("VC_Get_PendingBulkVoiceRequests", new SqlConnection(SharedClass.ConnectionString));
            sqlCommand.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter sqlDataAdapter = null;
            DataSet dataSet = null;
            while (!SharedClass.HasStopSignal)
            {
                try
                {
                    this.isIamPolling = true;
                    sqlCommand.Parameters.Clear();
                    sqlCommand.Parameters.Add("@LastRequestId", SqlDbType.BigInt).Value = lastRequestId;
                    sqlDataAdapter = new SqlDataAdapter();
                    sqlDataAdapter.SelectCommand = sqlCommand;
                    dataSet = new DataSet();
                    sqlDataAdapter.Fill(dataSet);
                    if (dataSet.Tables.Count > 0 && dataSet.Tables[0].Rows.Count > 0)
                    {
                        BulkRequest bulkRequest = null;
                        foreach (DataRow dataRow in dataSet.Tables[0].Rows)
                        {
                            try
                            {
                                bulkRequest = new BulkRequest();
                                bulkRequest.Id = Convert.ToInt64(dataRow["Id"].ToString());
                                bulkRequest.Xml = dataRow["Xml"].ToString();
                                bulkRequest.Ip = dataRow["Ip"].ToString();
                                bulkRequest.Destinations = new StringBuilder(dataRow["MobileNumbersList"].ToString());
                                bulkRequest.UUIDs = new StringBuilder(dataRow["UUIDsList"].ToString());
                                bulkRequest.RingUrl = dataRow["RingUrl"].ToString();
                                bulkRequest.AnswerUrl = dataRow["AnswerUrl"].ToString();
                                bulkRequest.HangupUrl = dataRow["HangupUrl"].ToString();
                                bulkRequest.Retries = (short)Convert.ToSByte(dataRow["Retries"].ToString());
                                bulkRequest.CallerId = dataRow["CallerId"].ToString();
                                bulkRequest.Status = (short)Convert.ToSByte(dataRow["Status"].ToString());
                                if (dataRow["ProcessedCount"] != DBNull.Value)
                                    bulkRequest.ProcessedCount = Convert.ToInt32(dataRow["ProcessedCount"].ToString());
                                if (dataRow["RequestId"] != DBNull.Value)
                                    bulkRequest.VoiceRequestId = Convert.ToInt64(dataRow["RequestId"].ToString());
                                AccountProcessor accountProcessor = null;
                                lock (SharedClass.ActiveAccountProcessors)
                                {
                                    SharedClass.ActiveAccountProcessors.TryGetValue(Convert.ToInt64(dataRow["AccountId"].ToString()), out accountProcessor);
                                    if (accountProcessor == null)
                                    {
                                        accountProcessor = new AccountProcessor();
                                        accountProcessor.AccountId = Convert.ToInt64(dataRow["AccountId"].ToString());
                                        Thread accountProcessorThread = new Thread(new ThreadStart(accountProcessor.Start));
                                        accountProcessorThread.Name = "Account_" + dataRow["AccountId"];
                                        accountProcessorThread.Start();
                                    }
                                    accountProcessor.EnQueue(bulkRequest);
                                }
                            }
                            catch (Exception ex1)
                            {
                                SharedClass.Logger.Error("Error In BulkPoll For Loop : " + ex1.ToString());
                                PropertyInfo[] properties = bulkRequest.GetType().GetProperties();
                                try
                                {
                                    foreach (PropertyInfo propertyInfo in properties)
                                    {
                                        if (propertyInfo.CanRead)
                                            SharedClass.DumpLogger.Error(propertyInfo.Name + " : " + propertyInfo.GetValue(bulkRequest).ToString());
                                    }
                                }
                                catch (Exception ex2)
                                {
                                    SharedClass.Logger.Error("Error Dumping Info : " + ex2.ToString());
                                }
                            }
                        }
                    }
                }
                catch (Exception ex)
                {
                    SharedClass.Logger.Error("Error In BulkPoller, " + ex.ToString());
                }
                finally
                {
                    NullReferenceException referenceException;
                    try
                    {
                        sqlDataAdapter.Dispose();
                    }
                    catch (NullReferenceException ex)
                    {
                        referenceException = ex;
                    }
                    try
                    {
                        dataSet.Dispose();
                    }
                    catch (NullReferenceException ex)
                    {
                        referenceException = ex;
                    }
                    sqlDataAdapter = null;
                    dataSet = null;
                }
                this.isIamPolling = false;
                try
                {
                    Thread.Sleep(5000);
                }
                catch (ThreadAbortException ex)
                {
                    SharedClass.Logger.Error(ex);
                }
                catch (ThreadInterruptedException ex)
                {
                    SharedClass.Logger.Error(ex);
                }
            }
        }

        public void LoadGateways()
        {
            SharedClass.Logger.Info("Getting Gateways");
            SqlCommand sqlCommand = (SqlCommand)null;
            try
            {
                sqlCommand = new SqlCommand("Get_VoiceGateways", new SqlConnection(SharedClass.ConnectionString));
                sqlCommand.CommandType = CommandType.StoredProcedure;
                SqlDataAdapter sqlDataAdapter = new SqlDataAdapter();
                DataSet dataSet = new DataSet();
                sqlDataAdapter.SelectCommand = sqlCommand;
                sqlDataAdapter.Fill(dataSet);
                if (dataSet.Tables.Count > 0 && dataSet.Tables[0].Rows.Count > 0)
                {
                    foreach (DataRow dataRow in dataSet.Tables[0].Rows)
                    {
                        Gateway gateway = new Gateway();
                        Thread gatewayThread = null;
                        try
                        {
                            gateway.Id = Convert.ToInt16(dataRow["Id"]);
                            gateway.Name = dataRow["Name"].ToString().Replace(" ", "");
                            gateway.ConnectUrl = dataRow["ConnectUrl"].ToString();
                            gateway.Ip = dataRow["Ip"].ToString();
                            gateway.Port = Convert.ToInt32(dataRow["Port"]);
                            gateway.MaximumConcurrency = Convert.ToInt32(dataRow["MaximumConcurrency"]);
                            gateway.CurrenctConcurrency = Convert.ToInt32(dataRow["CurrenctConcurrency"]);
                            gateway.OriginationUrl = dataRow["OriginationUrl"].ToString();
                            if (!gateway.OriginationUrl.EndsWith("/"))
                                gateway.OriginationUrl += "/";
                            gateway.ExtraDialString = dataRow["ExtraDialString"].ToString();
                            gateway.CountryPrefix = dataRow["CountryPrefix"].ToString();
                            gateway.IsCountryPrefixAllowed = Convert.ToBoolean(dataRow["IsCountryPrefixAllowed"].ToString());
                            gateway.DialPrefix = dataRow["DialPrefix"].ToString();
                            gateway.HighPriorityQueueLastSlno = Convert.ToInt64(dataRow["HighPriorityQueueLastSlno"]);
                            gateway.MediumPriorityQueueLastSlno = Convert.ToInt64(dataRow["MediumPriorityQueueLastSlno"]);
                            gateway.LowPriorityQueueLastSlno = Convert.ToInt64(dataRow["LowPriorityQueueLastSlno"]);
                            gateway.PushThreadsTotal = Convert.ToSByte(dataRow["PushThreadsTotal"]);
                            gatewayThread = new Thread(new ThreadStart(gateway.Start));
                            gatewayThread.Name = gateway.Name.Replace(" ", "");
                            SharedClass.Logger.Info("Starting Gateway " + gateway.Name);
                            gatewayThread.Start();
                        }
                        catch (Exception ex)
                        {
                            SharedClass.Logger.Error("Error Starting Gateway, " + ex.ToString());
                        }
                    }
                }
                else
                    SharedClass.Logger.Error("No Gateways Returned From DataBase");
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error Getting Gateways List, Reason : " + ex.ToString());
            }
        }

        private void LoadConfig()
        {
            SharedClass.ConnectionString = ConfigurationManager.ConnectionStrings["DbConnectionString"].ConnectionString;
            if (ConfigurationManager.AppSettings["GatewayHeartBeatSpan"] == null)
            {
                SharedClass.GatewayHeartBeatSpan = 60;
            }
            else
            {
                SharedClass.GatewayHeartBeatSpan = (int)Convert.ToInt16(ConfigurationManager.AppSettings["GatewayHeartBeatSpan"].ToString());
            }
            if (ConfigurationManager.AppSettings["HP"] == null)
            {
                Priority.HpFloor = 1;
                Priority.HpCeil = 3;
            }
            else
            {
                Priority.HpFloor = Convert.ToSByte(ConfigurationManager.AppSettings["HP"].ToString().Split('-')[0].ToString());
                Priority.HpCeil = Convert.ToSByte(ConfigurationManager.AppSettings["HP"].ToString().Split('-')[1].ToString());
            }
            if (ConfigurationManager.AppSettings["MP"] == null)
            {
                Priority.MpFloor = 4;
                Priority.MpCeil = 7;
            }
            else
            {
                Priority.MpFloor = Convert.ToSByte(ConfigurationManager.AppSettings["MP"].ToString().Split('-')[0].ToString());
                Priority.MpCeil = Convert.ToSByte(ConfigurationManager.AppSettings["MP"].ToString().Split('-')[1].ToString());
            }
            if (ConfigurationManager.AppSettings["LP"] == null)
            {
                Priority.LpFloor = 8;
                Priority.LpCeil = 10;
            }
            else
            {
                Priority.LpFloor = Convert.ToSByte(ConfigurationManager.AppSettings["LP"].ToString().Split('-')[0].ToString());
                Priority.LpCeil = Convert.ToSByte(ConfigurationManager.AppSettings["LP"].ToString().Split('-')[1].ToString());
            }
            if (ConfigurationManager.AppSettings["RabbitMQHost"] != null)
            {
                SharedClass.RabbitMQClient = new RabbitMQClient();
                SharedClass.HangupProcessor = new HangupProcessor();
                SharedClass.RabbitMQClient.Host = ConfigurationManager.AppSettings["RabbitMQHost"];
                if (ConfigurationManager.AppSettings["RabbitMQPort"] != null)
                {
                    SharedClass.RabbitMQClient.Port = Convert.ToInt16(ConfigurationManager.AppSettings["RabbitMQPort"]);
                }
                SharedClass.RabbitMQClient.User = ConfigurationManager.AppSettings["RabbitMQUser"] == null ? "guest" : ConfigurationManager.AppSettings["RabbitMQUser"].ToString();
                SharedClass.RabbitMQClient.Password = ConfigurationManager.AppSettings["RabbitMQPassword"] == null ? "guest" : ConfigurationManager.AppSettings["RabbitMQPassword"].ToString();
            }
            if (ConfigurationManager.AppSettings["AuthKey"] != null)
            {
                SharedClass.Notifier.AuthKey = ConfigurationManager.AppSettings["AuthKey"];
                if (ConfigurationManager.AppSettings["AuthToken"] != null)
                {
                    SharedClass.Notifier.AuthToken = ConfigurationManager.AppSettings["AuthToken"];
                }
            }
            if (ConfigurationManager.AppSettings["SendAlertsTo"] != null)
            {
                SharedClass.Notifier.SendAlertsTo = ConfigurationManager.AppSettings["SendAlertsTo"];
            }
            if (ConfigurationManager.AppSettings["SenderId"] != null)
            {
                SharedClass.Notifier.SenderId = ConfigurationManager.AppSettings["SenderId"];
            }

            SharedClass.Notifier.ApiUrl = ConfigurationManager.AppSettings["ApiUrl"] == null ? "" : ConfigurationManager.AppSettings["ApiUrl"].ToString();
            SharedClass.IsHangupProcessInMemory = ConfigurationManager.AppSettings["IsHangupProcessInMemory"] == null ? false : Convert.ToBoolean(ConfigurationManager.AppSettings["IsHangupProcessInMemory"]);
            SharedClass.Listener.Ip = ConfigurationManager.AppSettings["ListenerIp"] == null ? "" : ConfigurationManager.AppSettings["ListenerIp"].ToString();
            SharedClass.Listener.Port = ConfigurationManager.AppSettings["ListenerPort"] == null ? 0 : Convert.ToInt16(ConfigurationManager.AppSettings["ListenerPort"]);
            if (ConfigurationManager.AppSettings["BulkRequestBatchCount"] != null) {
                SharedClass.BulkRequestBatchCount = Convert.ToInt16(ConfigurationManager.AppSettings["BulkRequestBatchCount"].ToString());
            }
        }
    }
}
