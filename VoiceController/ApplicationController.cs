using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Configuration;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using HeartBeat;

namespace VoiceController
{
    internal class ApplicationController
    {
        private Thread _pollThread = null;
        private Thread _pollThreadStaging = null;
        private Client heartBeatClient = null;

        public ApplicationController()
        {
            this.LoadConfig();
        }

        public void Start()
        {
            SharedClass.IsServiceCleaned = false;
            this.LoadGateways();
            if (SharedClass.RabbitMQClient != null)
            {
                SharedClass.Logger.Info("Starting RabbitMQ Client");
                SharedClass.RabbitMQClient.Start();
            }
            if (SharedClass.HangupProcessor != null)
            {
                SharedClass.Logger.Info("Starting Hangup Processor");
                SharedClass.HangupProcessor.Start();
            }
            if (SharedClass.Listener.Ip.Length > 7 && SharedClass.Listener.Port > 0)
            {
                SharedClass.Logger.Info("Starting Listener");
                SharedClass.Listener.Initialize();
            }
            this._pollThread = new Thread(new ParameterizedThreadStart(this.StartDbPoll));
            this._pollThread.Name = "BulkPoller";
            SharedClass.Logger.Info("Starting BulkPoller");
            this._pollThread.Start(Environment.PRODUCTION);
            if (SharedClass.PollStaging)
            {
                this._pollThreadStaging = new Thread(new ParameterizedThreadStart(this.StartDbPoll));
                this._pollThreadStaging.Name = "BulkPollerStaging";
                this._pollThreadStaging.Start(Environment.STAGING);
            }
            this.heartBeatClient = new Client(Convert.ToString(SharedClass.GetConnectionString(Environment.PRODUCTION)), 6);
            this.heartBeatClient.Initialize();
        }

        public void Stop()
        {
            this.heartBeatClient.Stop();
            // Stop BulkVoiceRequests Polling Threads
            while (this._pollThread.ThreadState != ThreadState.Stopped)
            {
                SharedClass.Logger.Info("DbPoller Is Still Running, Thread State : " + this._pollThread.ThreadState.ToString());
                if (this._pollThread.ThreadState == ThreadState.WaitSleepJoin)
                    this._pollThread.Interrupt();
                Thread.Sleep(1000);
            }
            while (this._pollThreadStaging.ThreadState != ThreadState.Stopped)
            {
                SharedClass.Logger.Info("DbPoller Is Still Running, Thread State : " + this._pollThreadStaging.ThreadState.ToString());
                if (this._pollThreadStaging.ThreadState == ThreadState.WaitSleepJoin)
                    this._pollThreadStaging.Interrupt();
                Thread.Sleep(1000);
            }

            if (SharedClass.RabbitMQClient != null) {
                SharedClass.Logger.Info("Stopping RabbitMQ Client");
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
                SharedClass.Logger.Info("Stopping HangupProcessor");
                SharedClass.HangupProcessor.Stop();
            }
            while(SharedClass.ActiveAccountProcessors.Count > 0)
            {
                try
                {
                    SharedClass.ActiveAccountProcessors.First().Value.Stop();
                }
                catch (Exception e)
                {
                    SharedClass.Logger.Error("Exception Stopping Active Account Processors. Reason : " + e.ToString());
                }
            }
            while(SharedClass.GatewayMap.Count > 0)
            {
                try
                {
                    SharedClass.GatewayMap.First().Value.Stop();
                }
                catch(Exception e)
                {
                    SharedClass.Logger.Error(string.Format("Exception Initiating Gateway_Stop. {0}", e.ToString()));
                }
            }
            //foreach (KeyValuePair<int, Gateway> keyValuePair in SharedClass.GatewayMap) {
            //    Thread gatewayStopThread = new Thread(new ThreadStart(keyValuePair.Value.Stop));
            //    gatewayStopThread.Name = keyValuePair.Value.Name + "_Stop";
            //    gatewayStopThread.Start();
            //}
            //byte waitCount = 0;
            //while (SharedClass.GatewayMap.Count > 0)
            //{
            //    SharedClass.Logger.Info(("Gateways Object Not Yet Cleaned, Active Gateways : " + SharedClass.GatewayMap.Count.ToString()));
            //    if (waitCount == 10)
            //    {
            //        try
            //        {
            //            foreach (KeyValuePair<int, Gateway> keyValuePair in SharedClass.GatewayMap)
            //            {
            //                SharedClass.Logger.Info("GatewayId : " + keyValuePair.Key.ToString() + ", RunningPushThreadCount : " + keyValuePair.Value.PushThreadsRunning);
            //            }
            //        }
            //        catch (Exception e)
            //        { }
            //        waitCount = 0;
            //    }
            //    ++waitCount;
            //    Thread.Sleep(1000);
            //}
            if (SharedClass.Listener != null)
            {
                SharedClass.Logger.Info("Stopping Listener");
                SharedClass.Listener.Destroy();
            }
            SharedClass.IsServiceCleaned = true;
        }

        public void StartDbPoll(object input)
        {
            Environment environment = (Environment)input;            
            SharedClass.Logger.Info("Started");
            SqlCommand sqlCommand = new SqlCommand("VC_Get_PendingBulkVoiceRequests", new SqlConnection(SharedClass.GetConnectionString(environment)));
            sqlCommand.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter sqlDataAdapter = null;
            DataSet dataSet = null;
            while (!SharedClass.HasStopSignal)
            {
                try
                {
                    //if (environment == Environment.STAGING)
                    ////_isIamPollingS = true;
                    //{
                    //    while (this._pollThreadStaging.ThreadState != ThreadState.Stopped)
                    //    {
                    //        SharedClass.Logger.Info(string.Format("Staging Poll Thread Not Yet Stopped. ThreadState:{0}", this._pollThreadStaging.ThreadState));
                    //        Thread.Sleep(2000);
                    //        if (this._pollThreadStaging.ThreadState == ThreadState.WaitSleepJoin)
                    //            this._pollThreadStaging.Interrupt();
                    //    }
                    //}
                    //else
                    //{
                    //    while (this._pollThread.ThreadState != ThreadState.Stopped)
                    //    {
                    //        SharedClass.Logger.Info(string.Format("Staging Poll Thread Not Yet Stopped. ThreadState:{0}", this._pollThread.ThreadState));
                    //        Thread.Sleep(2000);
                    //        if (this._pollThread.ThreadState == ThreadState.WaitSleepJoin)
                    //            this._pollThread.Interrupt();
                    //    }
                    //}
                    sqlCommand.Parameters.Clear();
                    sqlCommand.Parameters.Add("@LastRequestId", SqlDbType.BigInt).Value = BulkRequestId.GetLastId(environment);
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
                                bulkRequest.Environment = environment;
                                bulkRequest.Id = Convert.ToInt64(dataRow["Id"].ToString());
                                bulkRequest.Xml = dataRow["Xml"].ToString();
                                bulkRequest.Ip = dataRow["Ip"].ToString();
                                bulkRequest.ToolId = Convert.ToByte(dataRow["ToolId"]);
                                bulkRequest.Destinations = new StringBuilder(dataRow["MobileNumbersList"].ToString());
                                bulkRequest.UUIDs = new StringBuilder(dataRow["UUIDsList"].ToString());
                                if (!dataRow["RingUrl"].IsDBNull())
                                    bulkRequest.RingUrl = dataRow["RingUrl"].ToString();
                                else
                                    bulkRequest.RingUrl = "";
                                if (!dataRow["AnswerUrl"].IsDBNull())
                                    bulkRequest.AnswerUrl = dataRow["AnswerUrl"].ToString();
                                else
                                    bulkRequest.AnswerUrl = "";
                                if (!dataRow["HangupUrl"].IsDBNull())
                                    bulkRequest.HangupUrl = dataRow["HangupUrl"].ToString();
                                else
                                    bulkRequest.HangupUrl = "";
                                if (!dataRow["Retries"].IsDBNull())
                                    bulkRequest.Retries = Convert.ToByte(dataRow["Retries"].ToString());
                                else
                                    bulkRequest.Retries = 0;
                                if (!dataRow["CallerId"].IsDBNull())
                                    bulkRequest.CallerId = dataRow["CallerId"].ToString();
                                else
                                    bulkRequest.CallerId = "";

                                if (!dataRow["Status"].IsDBNull())
                                    bulkRequest.Status = Convert.ToByte(dataRow["Status"].ToString());
                                else
                                    bulkRequest.Status = 0;
                                if (dataRow["ProcessedCount"] != DBNull.Value)
                                    bulkRequest.ProcessedCount = Convert.ToInt32(dataRow["ProcessedCount"].ToString());
                                if (!dataRow["TotalCount"].IsDBNull())
                                    bulkRequest.TotalCount = Convert.ToInt32(dataRow["TotalCount"].ToString());
                                if (dataRow["RequestId"] != DBNull.Value)
                                    bulkRequest.VoiceRequestId = Convert.ToInt64(dataRow["RequestId"].ToString());
                                bulkRequest.CampaignScheduleId = Convert.ToInt64(dataRow["CampaignScheduleId"]);
                                AccountProcessor accountProcessor = null;
                                lock (SharedClass.ActiveAccountProcessors)
                                {
                                    SharedClass.ActiveAccountProcessors.TryGetValue(Convert.ToInt32(dataRow["AccountId"].ToString()), out accountProcessor);
                                    if (accountProcessor == null)
                                    {
                                        accountProcessor = new AccountProcessor();
                                        accountProcessor.AccountId = Convert.ToInt32(dataRow["AccountId"].ToString());
                                        Thread accountProcessorThread = new Thread(new ThreadStart(accountProcessor.Start));
                                        accountProcessorThread.Name = "Account_" + dataRow["AccountId"];
                                        accountProcessorThread.Start();
                                    }
                                    accountProcessor.EnQueue(bulkRequest);
                                }
                                BulkRequestId.SetLastId(bulkRequest.Id, environment);
                            }
                            catch (Exception ex1)
                            {
                                SharedClass.Logger.Error("Error In BulkPoll For Loop : " + ex1.ToString());                                
                                try
                                {
                                    PropertyInfo[] properties = bulkRequest.GetType().GetProperties();
                                    foreach (PropertyInfo propertyInfo in properties)
                                    {
                                        if (propertyInfo.CanRead)
                                        { 
                                            if(propertyInfo.GetValue(bulkRequest) == DBNull.Value)
                                                SharedClass.DumpLogger.Error(propertyInfo.Name + " : NULL");
                                            else
                                                SharedClass.DumpLogger.Error(propertyInfo.Name + " : " + propertyInfo.GetValue(bulkRequest).ToString());
                                        }   
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

        private void SetLastQueuedSlno()
        {
            SharedClass.Logger.Info("Getting Last Queued Slno's");
            SqlCommand sqlCmd = (SqlCommand)null;
            SqlConnection sqlCon = (SqlConnection)null; 
            try
            {
                sqlCon = new SqlConnection(SharedClass.GetConnectionString(Environment.PRODUCTION));
                sqlCmd = new SqlCommand("SELECT Id,ISNULL(HPQLastSlno,0) as HPSlno, ISNULL(MPQLastSlno,0) as MPSlno, ISNULL(LPQLastSlno,0) as LPSlno From VoiceGateways WITH(NOLOCK)", sqlCon);
                sqlCon.Open();
                SqlDataReader reader = sqlCmd.ExecuteReader();
                while(reader.Read())
                {
                    CallsQueueSlno.SetSlno(Convert.ToInt32(reader["Id"]), Environment.PRODUCTION, Priority.PriorityMode.High, Convert.ToInt64(reader["HPSlno"]));
                    CallsQueueSlno.SetSlno(Convert.ToInt32(reader["Id"]), Environment.PRODUCTION, Priority.PriorityMode.Medium, Convert.ToInt64(reader["MPSlno"]));
                    CallsQueueSlno.SetSlno(Convert.ToInt32(reader["Id"]), Environment.PRODUCTION, Priority.PriorityMode.Low, Convert.ToInt64(reader["LPSlno"]));
                }
                sqlCon.Close();

            }
            catch(Exception ex)
            {
                SharedClass.Logger.Error("Exception in Getting Last Queued Slno Reason : " + ex.ToString());
            }

            if(SharedClass.PollStaging)
            {
                try
                {
                    sqlCon = new SqlConnection(SharedClass.GetConnectionString(Environment.STAGING));
                    sqlCmd = new SqlCommand("SELECT Id,ISNULL(HPQLastSlno,0) as HPSlno, ISNULL(MPQLastSlno,0) as MPSlno, ISNULL(LPQLastSlno,0) as LPSlno From VoiceGateways WITH(NOLOCK)", sqlCon);
                    sqlCon.Open();
                    SqlDataReader reader = sqlCmd.ExecuteReader();
                    while (reader.Read())
                    {
                        CallsQueueSlno.SetSlno(Convert.ToInt32(reader["Id"]), Environment.STAGING, Priority.PriorityMode.High, Convert.ToInt64(reader["HPSlno"]));
                        CallsQueueSlno.SetSlno(Convert.ToInt32(reader["Id"]), Environment.STAGING, Priority.PriorityMode.Medium, Convert.ToInt64(reader["MPSlno"]));
                        CallsQueueSlno.SetSlno(Convert.ToInt32(reader["Id"]), Environment.STAGING, Priority.PriorityMode.Low, Convert.ToInt64(reader["LPSlno"]));
                    }
                    sqlCon.Close();
                }
                catch (Exception ex)
                {
                    SharedClass.Logger.Error("Exception in Getting Last Queued Slno Of Staging Reason : " + ex.ToString());
                }

            }


            
        }

        public void LoadGateways()
        {
            SharedClass.Logger.Info("Fetching Gateways List From Database");
            SqlCommand sqlCommand = null;
            try
            {
                sqlCommand = new SqlCommand("Get_VoiceGateways", new SqlConnection(SharedClass.GetConnectionString(Environment.PRODUCTION)));
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
                            gateway.Name = dataRow["Code"].ToString().Replace(" ", "");
                            gateway.ConnectUrl = dataRow["ConnectUrl"].ToString();
                            gateway.Ip = dataRow["Ip"].ToString();
                            gateway.Port = Convert.ToInt32(dataRow["Port"]);
                            gateway.MaximumConcurrency = Convert.ToInt32(dataRow["MaximumConcurrency"]);
                            gateway.CurrenctConcurrency = Convert.ToInt32(dataRow["CurrenctConcurrency"]);
                            gateway.OriginationUrl = dataRow["OriginationUrl"].ToString();
                            if (!gateway.OriginationUrl.EndsWith("/"))
                                gateway.OriginationUrl += "/";
                            if(dataRow["ExtraDialString"] != DBNull.Value)
                                gateway.ExtraDialString = dataRow["ExtraDialString"].ToString();
                            gateway.CountryPrefix = dataRow["CountryPrefix"].ToString();
                            gateway.IsCountryPrefixAllowed = Convert.ToBoolean(dataRow["IsCountryPrefixAllowed"].ToString());
                            gateway.DialPrefix = dataRow["DialPrefix"].ToString();
                            if(dataRow.Table.Columns.Contains("StartingHour") && !dataRow["StartingHour"].IsDBNull())
                            {
                                byte tempByte = 0;
                                if (byte.TryParse(dataRow["StartingHour"].ToString(), out tempByte))
                                    gateway.StartingHour = tempByte;
                            }
                            if(dataRow.Table.Columns.Contains("StoppingHour") && !dataRow["StoppingHour"].IsDBNull())
                            {
                                byte tempByte = 23;
                                if (byte.TryParse(dataRow["StoppingHour"].ToString(), out tempByte))
                                    gateway.StoppingHour = tempByte;
                            }
                            if(dataRow.Table.Columns.Contains("UrgentPriorityQueueLastSlno") && !dataRow["UrgentPriorityQueueLastSlno"].IsDBNull())
                            {
                                long tempLong = 0;
                                if (long.TryParse(dataRow["UrgentPriorityQueueLastSlno"].ToString(), out tempLong))
                                    gateway.UrgentPriorityQueueLastSlno = tempLong;
                            }
                            gateway.HighPriorityQueueLastSlno = Convert.ToInt64(dataRow["HighPriorityQueueLastSlno"]);
                            gateway.MediumPriorityQueueLastSlno = Convert.ToInt64(dataRow["MediumPriorityQueueLastSlno"]);
                            gateway.LowPriorityQueueLastSlno = Convert.ToInt64(dataRow["LowPriorityQueueLastSlno"]);
                            gateway.NumberOfPushThreads = Convert.ToByte(dataRow["PushThreadsTotal"]);
                            gatewayThread = new Thread(new ThreadStart(gateway.Start));                            
                            gatewayThread.Name = gateway.Name.Replace(" ", "");
                            SharedClass.Logger.Info("Starting Gateway " + gateway.Name);
                            gatewayThread.Start();
                        }
                        catch (Exception ex)
                        {
                            SharedClass.Logger.Error("Error Starting Gateway, " + ex.ToString());
                            SharedClass.DumpLogger.Error(dataRow.ToString());
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
            try
            {
                SharedClass.Logger.Info("Loading Configuration Variables into Application Memory");
                int tempInt = 0;
                byte tempByte = 0;
                bool tempBoolean = false;
                SharedClass.SetConnectionString(ConfigurationManager.ConnectionStrings["DbConnectionString"].ConnectionString, Environment.PRODUCTION);
                if (ConfigurationManager.ConnectionStrings["DbConnectionStringStaging"] != null)
                {
                    SharedClass.Logger.Info(string.Format("Setting DbConnectionStringStaging To {0}", ConfigurationManager.ConnectionStrings["DbConnectionStringStaging"].ConnectionString));
                    SharedClass.SetConnectionString(ConfigurationManager.ConnectionStrings["DbConnectionStringStaging"].ConnectionString, Environment.STAGING);
                    SharedClass.PollStaging = true;
                }
                tempByte = SharedClass.GatewayHeartBeatSpan;
                if (ConfigurationManager.AppSettings["GatewayHeartBeatSpan"] != null)
                    if (byte.TryParse(ConfigurationManager.AppSettings["GatewayHeartBeatSpan"].Trim(), out tempByte))
                    {
                        SharedClass.Logger.Info(string.Format("Setting GatewayHeartBeatSpan To {0}", ConfigurationManager.AppSettings["GatewayHeartBeatSpan"]));
                        SharedClass.GatewayHeartBeatSpan = tempByte;
                    }

                if (ConfigurationManager.AppSettings["HP"] != null)
                {
                    tempByte = Priority.HpFloor;
                    if (byte.TryParse(ConfigurationManager.AppSettings["HP"].Split('-')[0].Trim(), out tempByte))
                    {
                        SharedClass.Logger.Info(string.Format("Setting HpFloor To {0}", tempByte));
                        Priority.HpFloor = tempByte;
                    }
                    tempByte = Priority.HpCeil;
                    if (byte.TryParse(ConfigurationManager.AppSettings["HP"].Split('-')[1].Trim(), out tempByte))
                    {
                        SharedClass.Logger.Info(string.Format("Setting HpCeil To {0}", tempByte));
                        Priority.HpCeil = tempByte;
                    }
                }
                if (ConfigurationManager.AppSettings["MP"] != null)
                {
                    tempByte = Priority.MpFloor;
                    if (byte.TryParse(ConfigurationManager.AppSettings["MP"].Split('-')[0].Trim(), out tempByte))
                    {
                        SharedClass.Logger.Info(string.Format("Setting MpFloor To {0}", tempByte));
                        Priority.MpFloor = tempByte;
                    }
                    tempByte = Priority.MpCeil;
                    if (byte.TryParse(ConfigurationManager.AppSettings["MP"].Split('-')[1].Trim(), out tempByte))
                    {
                        SharedClass.Logger.Info(string.Format("Setting MpCeil To {0}", tempByte));
                        Priority.MpCeil = tempByte;
                    }
                }
                if (ConfigurationManager.AppSettings["LP"] != null)
                {
                    tempByte = Priority.HpFloor;
                    if (byte.TryParse(ConfigurationManager.AppSettings["LP"].Split('-')[0].Trim(), out tempByte))
                    {
                        SharedClass.Logger.Info(string.Format("Setting LpFloor To {0}", tempByte));
                        Priority.LpFloor = tempByte;
                    }
                    tempByte = Priority.LpCeil;
                    if (byte.TryParse(ConfigurationManager.AppSettings["LP"].Split('-')[1].Trim(), out tempByte))
                    {
                        SharedClass.Logger.Info(string.Format("Setting LpCeil To {0}", tempByte));
                        Priority.LpCeil = tempByte;
                    }
                }
                if (ConfigurationManager.AppSettings["RabbitMQHost"] != null)
                {
                    SharedClass.RabbitMQClient = new RabbitMQClient();
                    SharedClass.HangupProcessor = new HangupProcessor();
                    SharedClass.RabbitMQClient.Host = ConfigurationManager.AppSettings["RabbitMQHost"].Trim();
                    SharedClass.Logger.Info(string.Format("Setting RabbitMQHost To {0}", SharedClass.RabbitMQClient.Host));
                    if (ConfigurationManager.AppSettings["RabbitMQPort"] != null)
                    {
                        tempInt = SharedClass.RabbitMQClient.Port;
                        if (int.TryParse(ConfigurationManager.AppSettings["RabbitMQPort"].Trim(), out tempInt))
                        {
                            SharedClass.Logger.Info(string.Format("Setting RabbitMQPort To {0}", tempInt));
                            SharedClass.RabbitMQClient.Port = tempInt;
                        }
                    }
                    if (ConfigurationManager.AppSettings["RabbitMQUser"].Trim().Length > 0)
                    {
                        SharedClass.Logger.Info(string.Format("Setting RabbitMQUser To {0}", ConfigurationManager.AppSettings["RabbitMQUser"]));
                        SharedClass.RabbitMQClient.User = ConfigurationManager.AppSettings["RabbitMQUser"];
                    }
                    if (ConfigurationManager.AppSettings["RabbitMQPassword"].Trim().Length > 0)
                    {
                        SharedClass.Logger.Info(string.Format("Setting RabbitMQPassword To {0}", ConfigurationManager.AppSettings["RabbitMQPasasword"]));
                        SharedClass.RabbitMQClient.Password = ConfigurationManager.AppSettings["RabbitMQPassword"];
                    }
                }
                if (ConfigurationManager.AppSettings["AuthKey"] != null)
                {
                    SharedClass.Notifier.AuthKey = ConfigurationManager.AppSettings["AuthKey"];
                    if (ConfigurationManager.AppSettings["AuthToken"] != null)
                        SharedClass.Notifier.AuthToken = ConfigurationManager.AppSettings["AuthToken"];
                }
                if (ConfigurationManager.AppSettings["SendAlertsTo"] != null)
                    SharedClass.Notifier.SendAlertsTo = ConfigurationManager.AppSettings["SendAlertsTo"];
                if (ConfigurationManager.AppSettings["SenderId"] != null)
                    SharedClass.Notifier.SenderId = ConfigurationManager.AppSettings["SenderId"];

                SharedClass.Notifier.ApiUrl = ConfigurationManager.AppSettings["ApiUrl"] == null ? "" : ConfigurationManager.AppSettings["ApiUrl"].ToString();
                if (ConfigurationManager.AppSettings["IsHangupProcessInMemory"] != null)
                {
                    tempBoolean = false;
                    if (bool.TryParse(ConfigurationManager.AppSettings["IsHangupProcessInMemory"].Trim(), out tempBoolean))
                        SharedClass.IsHangupProcessInMemory = tempBoolean;
                }

                SharedClass.Listener.Ip = ConfigurationManager.AppSettings["ListenerIp"] == null ? "" : ConfigurationManager.AppSettings["ListenerIp"].ToString();
                SharedClass.Listener.Port = ConfigurationManager.AppSettings["ListenerPort"] == null ? 0 : Convert.ToInt16(ConfigurationManager.AppSettings["ListenerPort"]);
                if (ConfigurationManager.AppSettings["BulkRequestBatchCount"] != null)
                {
                    tempInt = SharedClass.BulkRequestBatchCount;
                    if (int.TryParse(ConfigurationManager.AppSettings["BulkRequestBatchCount"].Trim(), out tempInt))
                        SharedClass.BulkRequestBatchCount = tempInt;
                }
            }
            catch(Exception e)
            {
                SharedClass.Logger.Info(e.ToString());
                throw new ConfigurationException("Unable to load configuration from file. Please checck application log file for exception details");
            }
            
        }
    }
}
