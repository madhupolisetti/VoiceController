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

namespace VoiceController
{
    internal class ApplicationController
    {
        private Thread _pollThread = null;
        private Thread _pollThreadStaging = null;
        private bool _isIamPollingS = false;
        private bool _isIamPollingP = false;

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
        }

        public void Stop()
        {
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
            try
            {
                foreach (KeyValuePair<int, AccountProcessor> keyValuePair in SharedClass.ActiveAccountProcessors)
                {
                    keyValuePair.Value.Stop();
                }
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception Stopping Active Account Processors. Reason : " + e.ToString());
            }            
            foreach (KeyValuePair<int, Gateway> keyValuePair in SharedClass.GatewayMap) {
                Thread gatewayStopThread = new Thread(new ThreadStart(keyValuePair.Value.Stop));
                gatewayStopThread.Name = keyValuePair.Value.Name + "_Stop";
                gatewayStopThread.Start();
            }
            byte waitCount = 0;
            while (SharedClass.GatewayMap.Count > 0)
            {
                SharedClass.Logger.Info(("Gateways Object Not Yet Cleaned, Active Gateways : " + SharedClass.GatewayMap.Count.ToString()));
                if (waitCount == 10)
                {
                    try
                    {
                        foreach (KeyValuePair<int, Gateway> keyValuePair in SharedClass.GatewayMap)
                        {
                            SharedClass.Logger.Info("GatewayId : " + keyValuePair.Key.ToString() + ", RunningPushThreadCount : " + keyValuePair.Value.PushThreadsRunning);
                        }
                    }
                    catch (Exception e)
                    { }
                    waitCount = 0;
                }
                ++waitCount;
                Thread.Sleep(1000);
            }
            if (SharedClass.Listener != null)
            {
                SharedClass.Logger.Info("Stopping Listener");
                SharedClass.Listener.Destroy();
            }
            while(this._isIamPollingP)
            {
                SharedClass.Logger.Info("DbPoller Is Still Running, Thread State : " + this._pollThread.ThreadState.ToString());
                if (this._pollThread.ThreadState == ThreadState.WaitSleepJoin)
                    this._pollThread.Interrupt();
                Thread.Sleep(100);
            }

            while (this._isIamPollingS)
            {
                SharedClass.Logger.Info("DbPollerStaging Is Still Running, Thread State : " + this._pollThread.ThreadState.ToString());
                if (this._pollThreadStaging.ThreadState == ThreadState.WaitSleepJoin)
                    this._pollThreadStaging.Interrupt();
                Thread.Sleep(100);
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
                    if (environment == Environment.STAGING)
                        _isIamPollingS = true;
                    else
                        _isIamPollingP = true;
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
                if (environment == Environment.STAGING)
                    _isIamPollingS = false;
                else
                    _isIamPollingP = false;
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
            SharedClass.Logger.Info("Getting Gateways List From Database");
            SqlCommand sqlCommand = (SqlCommand)null;
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
                            gateway.HighPriorityQueueLastSlno = Convert.ToInt64(dataRow["HighPriorityQueueLastSlno"]);
                            gateway.MediumPriorityQueueLastSlno = Convert.ToInt64(dataRow["MediumPriorityQueueLastSlno"]);
                            gateway.LowPriorityQueueLastSlno = Convert.ToInt64(dataRow["LowPriorityQueueLastSlno"]);
                            gateway.PushThreadsTotal = Convert.ToByte(dataRow["PushThreadsTotal"]);
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
            SharedClass.SetConnectionString(ConfigurationManager.ConnectionStrings["DbConnectionString"].ConnectionString, Environment.PRODUCTION);
            if(ConfigurationManager.ConnectionStrings["DbConnectionStringStaging"] != null)
            {
                SharedClass.SetConnectionString(ConfigurationManager.ConnectionStrings["DbConnectionStringStaging"].ConnectionString, Environment.STAGING);
                SharedClass.PollStaging = true;
            }
            
            if (ConfigurationManager.AppSettings["GatewayHeartBeatSpan"] == null)            
                SharedClass.GatewayHeartBeatSpan = 60;
            else
                SharedClass.GatewayHeartBeatSpan = (int)Convert.ToInt16(ConfigurationManager.AppSettings["GatewayHeartBeatSpan"].ToString());
            if (ConfigurationManager.AppSettings["HP"] == null)
            {
                Priority.HpFloor = 1;
                Priority.HpCeil = 3;
            }
            else
            {
                Priority.HpFloor = Convert.ToByte(ConfigurationManager.AppSettings["HP"].ToString().Split('-')[0].ToString());
                Priority.HpCeil = Convert.ToByte(ConfigurationManager.AppSettings["HP"].ToString().Split('-')[1].ToString());
            }
            if (ConfigurationManager.AppSettings["MP"] == null)
            {
                Priority.MpFloor = 4;
                Priority.MpCeil = 7;
            }
            else
            {
                Priority.MpFloor = Convert.ToByte(ConfigurationManager.AppSettings["MP"].ToString().Split('-')[0].ToString());
                Priority.MpCeil = Convert.ToByte(ConfigurationManager.AppSettings["MP"].ToString().Split('-')[1].ToString());
            }
            if (ConfigurationManager.AppSettings["LP"] == null)
            {
                Priority.LpFloor = 8;
                Priority.LpCeil = 10;
            }
            else
            {
                Priority.LpFloor = Convert.ToByte(ConfigurationManager.AppSettings["LP"].ToString().Split('-')[0].ToString());
                Priority.LpCeil = Convert.ToByte(ConfigurationManager.AppSettings["LP"].ToString().Split('-')[1].ToString());
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
