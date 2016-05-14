using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Net;
using Newtonsoft.Json.Linq;

namespace VoiceController
{
    [Serializable]
    public class Gateway
    {
        private int id = 0;
        private string name = "";
        private string connectUrl = "";
        private string ip = "";
        private int port = 0;
        private int maximumConcurrency = 0;
        private int currentConcurrency = 0;
        private string originationUrl = string.Empty;
        private string extraDialString = string.Empty;
        private string countryPrefix = string.Empty;
        private bool isCountryPrefixAllowed = true;
        private string dialPrefix = string.Empty;
        private long urgentPriorityQueueLastSlno = 0;
        private long highPriorityQueueLastSlno = 0;
        private long mediumPriorityQueueLastSlno = 0;
        private long lowPriorityQueueLastSlno = 0L;
        private short pushThreadsTotal = (short)1;
        private short pushThreadsRunning = (short)0;
        private short pollThreadsRunning = 0;
        private bool shouldIPoll = true;
        private bool shouldIProcess = true;
        private Mutex concurrencyMutex = new Mutex();
        private Mutex queueCountMutex = new Mutex();
        private Mutex pushThreadMutex = new Mutex();
        private Thread heartBeatThread = null;
        private Thread upPollThread = null;
        private Thread hpPollThread = null;
        private Thread mpPollThread = null;
        private Thread lpPollThread = null;
        private Thread[] pushThreads = null;
        private long minTimeTaken = 2000;
        private long maxTimeTaken = 0;
        public CallsQueue CallsQueue = null;
        public void Start()
        {
            SharedClass.Logger.Info("Loading Into GatewayMap, " + this.GetDisplayString());
            lock (SharedClass.GatewayMap) {
                SharedClass.GatewayMap.Add(this.id, this);
            }
            try
            {
                if (File.Exists(this.name + ".ser"))
                {
                    System.Runtime.Serialization.IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                    Stream stream = new FileStream(this.name + ".ser", FileMode.Open, FileAccess.Read, FileShare.Read);
                    CallsQueue = (CallsQueue)formatter.Deserialize(stream);
                    stream.Close();
                    SharedClass.Logger.Info("Queue DeSerialized. UpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Urgent) + ", HpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.High) + ", MpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Medium) + ", LpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Low));
                    try
                    {
                        File.Delete(this.name.Replace(" ", "") + ".ser");
                    }
                    catch (Exception e) {
                        SharedClass.Logger.Error("Error Deleting File : " + e.ToString());
                    }
                }
                else {
                    CallsQueue = new CallsQueue();
                }
            }
            catch (Exception e) {
                SharedClass.Logger.Error("Error DeSerializing Queue : " + e.ToString());
                CallsQueue = new CallsQueue();
            }
            this.upPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this.hpPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this.mpPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this.lpPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this.upPollThread.Name = this.name + "_UP_Poller";
            this.hpPollThread.Name = this.name + "_HP_Poller";
            this.mpPollThread.Name = this.name + "_MP_Poller";
            this.lpPollThread.Name = this.name + "_LP_Poller";
            this.upPollThread.Start(Priority.PriorityMode.Urgent);
            this.hpPollThread.Start(Priority.PriorityMode.High);
            this.mpPollThread.Start(Priority.PriorityMode.Medium);
            this.lpPollThread.Start(Priority.PriorityMode.Low);
            this.pushThreads = new Thread[this.pushThreadsTotal];
            for (short index = 1; index <= this.pushThreadsTotal; ++index)
            {
                this.pushThreads[index - 1] = new Thread(new ThreadStart(this.StartPushing));
                this.pushThreads[index - 1].Name = this.name + "_Push_" + index;
                this.pushThreads[index - 1].Start();
            }
            heartBeatThread = new Thread(new ThreadStart(HeartBeat));
            heartBeatThread.Name = this.name + "_HB";
            heartBeatThread.Start();
        }

        public void Stop()
        {
            SharedClass.Logger.Info("Stopping");
            this.shouldIPoll = false;
            this.shouldIProcess = false;
            Thread.Sleep(500);
            for (short index = 0; index < this.pushThreads.Count(); ++index)
            {
                //Enumerable.Count<Thread>((IEnumerable<Thread>)this.pushThreads
                while (this.pushThreads[index].ThreadState == ThreadState.WaitSleepJoin)
                {
                    this.pushThreads[index].Interrupt();
                    SharedClass.Logger.Info("PushThread : " + this.pushThreads[index].Name + " Is Running, ThreadState : " + this.pushThreads[index].ThreadState.ToString());
                    Thread.Sleep(2000);
                }                
                Thread.Sleep(2000);
            }
            while (this.pushThreadsRunning > 0) {
                SharedClass.Logger.Info("Still " + this.pushThreadsRunning + " Push Threads Running");
                Thread.Sleep(2000);
            }
            while (this.pollThreadsRunning > 0) {
                SharedClass.Logger.Info("Still " + this.pollThreadsRunning + " Pollers Running");
                Thread.Sleep(2000);
            }
            try
            {
                SharedClass.Logger.Info("Serializing Queue. UpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Urgent) + ", HpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.High) + ", MpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Medium) + ", LpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Low));
                System.Runtime.Serialization.IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                Stream stream = new FileStream(this.name + ".ser", FileMode.Create, FileAccess.Write, FileShare.None);
                formatter.Serialize(stream, this.CallsQueue);
                stream.Close();
                SharedClass.Logger.Info("Queue Serialization Successful");
            }
            catch (Exception e) {
                SharedClass.Logger.Error("Error Serializing Queue : " + e.ToString());
            }
            lock (SharedClass.GatewayMap) {
                SharedClass.GatewayMap.Remove(this.id);
            }
        }
        private void UpdateLastSlno(Priority.PriorityMode mode, long slno)
        {
            switch (mode)
            {
                case Priority.PriorityMode.Urgent:
                    this.urgentPriorityQueueLastSlno = slno;
                    break;
                case Priority.PriorityMode.High:
                    this.HighPriorityQueueLastSlno = slno;                    
                    break;
                case Priority.PriorityMode.Medium:
                    this.MediumPriorityQueueLastSlno = slno;                    
                    break;
                default:
                    this.LowPriorityQueueLastSlno = slno;                    
                    break;
            }
        }
        private long GetLastSlno(Priority.PriorityMode mode)
        {
            long num;
            switch (mode)
            {
                case Priority.PriorityMode.Urgent:
                    num = this.urgentPriorityQueueLastSlno;
                    break;
                case Priority.PriorityMode.High:
                    num = this.highPriorityQueueLastSlno;
                    break;
                case Priority.PriorityMode.Medium:
                    num = this.MediumPriorityQueueLastSlno;
                    break;
                default:
                    num = this.LowPriorityQueueLastSlno;
                    break;
            }
            return num;
        }
        private void GetPendingCallsFromDataBase(object input)
        {
            Priority.PriorityMode priorityMode = (Priority.PriorityMode)input;
            SqlConnection connection = new SqlConnection(SharedClass.ConnectionString);
            SqlCommand sqlCommand = new SqlCommand("GetCallingData", connection);
            sqlCommand.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter da = null;
            DataSet ds = null;
            Call call = null;
            short floorValue = 0;
            short ceilValue = 10;
            switch (priorityMode) { 
                case Priority.PriorityMode.Urgent:
                    floorValue = 0;
                    ceilValue = 0;
                    break;
                case Priority.PriorityMode.High:
                    floorValue = Priority.HpFloor;
                    ceilValue = Priority.HpCeil;
                    break;
                case Priority.PriorityMode.Medium:
                    floorValue = Priority.MpFloor;
                    ceilValue = Priority.MpCeil;
                    break;
                default:
                    floorValue = Priority.LpFloor;
                    ceilValue = Priority.LpCeil;
                    break;
            }
            while (!this.pushThreadMutex.WaitOne())
            {
                Thread.Sleep(100);
                SharedClass.Logger.Info("Waiting for pushThreadMutex to increase pollthreadscount");
            } 
            ++this.pollThreadsRunning;
            this.pushThreadMutex.ReleaseMutex();
            SharedClass.Logger.Info("Started");
            while (this.shouldIPoll && !SharedClass.HasStopSignal)
            {
                try
                {
                    sqlCommand.Parameters.Clear();
                    sqlCommand.Parameters.Add("@GatewayId", SqlDbType.Int).Value = this.id;
                    sqlCommand.Parameters.Add("@LastSlno", SqlDbType.BigInt).Value = this.GetLastSlno(priorityMode);
                    sqlCommand.Parameters.Add("@FloorValue", SqlDbType.TinyInt).Value = floorValue;
                    sqlCommand.Parameters.Add("@CeilValue", SqlDbType.TinyInt).Value = ceilValue;
                    if (connection.State != ConnectionState.Open)
                        connection.Open();
                    da = new SqlDataAdapter();
                    da.SelectCommand = sqlCommand;
                    ds = new DataSet();
                    da.Fill(ds);
                    if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
                    {
                        foreach (DataRow dataRow in ds.Tables[0].Rows)
                        {
                            call = new Call();
                            call.QueueTableSlno = Convert.ToInt64(dataRow["Id"]);
                            call.CallId = Convert.ToInt64(dataRow["CallId"]);
                            call.AccountId = Convert.ToInt64(dataRow["AccountId"]);
                            call.UUID = dataRow["UUID"].ToString();
                            call.CallerId = dataRow["CallerId"].ToString();
                            call.Destination = dataRow["Destination"].ToString();
                            call.Xml = dataRow["Xml"].ToString();
                            call.RingUrl = dataRow["RingUrl"].ToString();
                            call.AnswerUrl = dataRow["AnswerUrl"].ToString();
                            call.HangupUrl = dataRow["HangupUrl"].ToString();
                            call.Pulse = Convert.ToSByte(dataRow["Pulse"]);
                            call.PricePerPulse = float.Parse(dataRow["PricePerPulse"].ToString());
                            call.PriorityValue = Convert.ToSByte(dataRow["Priority"]);
                            this.CallsQueue.EnQueue(call, priorityMode);
                        }
                        this.UpdateLastSlno(priorityMode, call.QueueTableSlno);
                        Thread.Sleep(10000);
                    }
                    else {
                        try
                        {
                            Thread.Sleep(2000);
                        }
                        catch (ThreadInterruptedException) { 

                        }
                    }
                }
                catch (Exception ex)
                {
                    SharedClass.Logger.Error(("Error In Polling, " + ex.ToString()));
                }
                finally
                {
                    da = null;
                    ds = null;
                }
            }
            SharedClass.Logger.Info("Stopped Polling, ShouldIPoll : " + this.shouldIPoll + ", Has Stop Signal : " + SharedClass.HasStopSignal);
            while (!this.pushThreadMutex.WaitOne()) {
                Thread.Sleep(100);
                SharedClass.Logger.Info("Waiting for pushThreadMutex to decrease pollThreadsRunning count");
            }
            --this.pollThreadsRunning;
            this.pushThreadMutex.ReleaseMutex();
            if (SharedClass.HasStopSignal) {
                return;
            }   
            string str = this.GetDisplayString() + " ";
            string text;
            switch (priorityMode)
            {
                case Priority.PriorityMode.High:
                    text = str + "HP Poller Stopped";
                    break;
                case Priority.PriorityMode.Medium:
                    text = str + "MP Poller Stopped";
                    break;
                default:
                    text = str + "LP Poller Stopped";
                    break;
            }
            lock (SharedClass.Notifier) {
                SharedClass.Notifier.SendSms(text);
            }
        }
        private void StartPushing()
        {
            int loopCount = 0;
            long startTime = 0;
            long timeTaken = 0;
            while (!this.pushThreadMutex.WaitOne()) {
                Thread.Sleep(10);
            }
            ++this.pushThreadsRunning;
            this.pushThreadMutex.ReleaseMutex();
            SharedClass.Logger.Info("Started");
            Call call = null;
            while (this.shouldIProcess)
            {   
                try
                {   
                    if ((call = this.CallsQueue.DeQueue()) == null)
                    {
                        try
                        {
                            Thread.Sleep(2000);
                        }
                        catch (ThreadInterruptedException e) { }
                    }
                    else
                    {
                        this.WaitForLines();
                        if (!this.ShouldIPushCall()) {
                            this.CallsQueue.EnQueue(call, Priority.GetPriority(call.PriorityValue));
                            call = null;
                            continue;
                        }
                        startTime = SharedClass.CurrentTimeStamp();
                        this.ProcessCall(call);
                        timeTaken = SharedClass.CurrentTimeStamp() - startTime;
                        call = null;
                        if (timeTaken < this.minTimeTaken)
                            this.minTimeTaken = timeTaken;
                        if (timeTaken > this.maxTimeTaken)
                            this.maxTimeTaken = timeTaken;
                        ++loopCount;
                        if (loopCount == 50)
                        {
                            SharedClass.Logger.Info("Processed " + loopCount + " Calls : minTime => " + minTimeTaken + ", maxTime => " + maxTimeTaken);
                            loopCount = 0;
                        }
                    }
                }
                catch (Exception ex)
                {
                    SharedClass.Logger.Error("Error In While Loop, " + ex.ToString());
                }
                finally {
                    if (call != null) {
                        this.CallsQueue.EnQueue(call, Priority.GetPriority(call.PriorityValue));
                    }
                }
            }
            SharedClass.Logger.Info("Exited From While Loop and is about to die");
            while (!this.pushThreadMutex.WaitOne()) {
                Thread.Sleep(10);
            }   
            --this.pushThreadsRunning;
            this.pushThreadMutex.ReleaseMutex();
        }
        public void ProcessCall(Call call)
        {
            HttpWebRequest request = null;
            HttpWebResponse response = null;
            StreamReader streamReader = null;
            StreamWriter streamWriter = null;
            try
            {
                if (!this.IsCountryPrefixAllowed && this.dialPrefix.Length > 0 && this.countryPrefix.Length > 0 && call.Destination.StartsWith(this.countryPrefix))
                    call.Destination = this.dialPrefix + call.Destination.Substring(this.countryPrefix.Length);
                string payload = "From=" + call.CallerId + "&To=" + call.Destination + "&OriginationUUID=" + call.UUID + "&Gateways=" + this.OriginationUrl;
                payload += "&SequenceNumber=" + call.CallId + "&AnswerUrl=" + call.AnswerUrl + "&HangupUrl=" + ((call.HangupUrl != null && call.HangupUrl.Trim().Length > 0) ? call.HangupUrl : call.AnswerUrl);
                payload += "&NotifyCallFlow=RMQ&CallBackByRMQ=1&GwID=" + this.id.ToString() + "&";
                if (call.RingUrl.Length > 0)
                    payload = payload + "&RingUrl=" + call.RingUrl;
                payload += "&ActionMethod=POST";
                if (call.Xml.Length > 0)
                    payload += "&AnswerXml=" + call.Xml;                
                if (this.ExtraDialString.Length > 0)
                    payload += "&ExtraDialString=" + this.ExtraDialString;                
                SharedClass.Logger.Info(payload);
                request = (HttpWebRequest)WebRequest.Create(this.ConnectUrl + "Call/");
                request.Method = "POST";
                request.Proxy = null;
                request.KeepAlive = true;
                request.ContentType = "application/x-www-form-urlencoded";
                streamWriter = new StreamWriter(request.GetRequestStream());
                streamWriter.Write(payload);
                streamWriter.Flush();
                streamWriter.Close();
                response = (HttpWebResponse)request.GetResponse();
                streamReader = new StreamReader(response.GetResponseStream());
                if (!Convert.ToBoolean(JObject.Parse(streamReader.ReadToEnd()).SelectToken("Success").ToString()))
                    return;
                this.UpdateConcurrency(false, 1);
            }
            catch (Exception ex)
            {   
                SharedClass.Logger.Info("Error Processing Call : " + call.PrintMe() + ", Reason : " + ex.ToString());
            }
            finally
            {
                try
                {
                    response.Close();
                    response.Dispose();
                    request = null;
                    streamReader.Dispose();
                    streamWriter.Dispose();
                }
                catch (Exception ex)
                {
                    
                }
            }
        }
        public void UpdateConcurrency(bool isHangup, int count)
        {
            try
            {
                while (!this.concurrencyMutex.WaitOne())
                    Thread.Sleep(10);
                if (isHangup)
                    this.currentConcurrency -= count;
                else
                    this.currentConcurrency += count;
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error Updating Gateway Concurrency, Id " + this.id + ", IsHangup : " + isHangup + ", Count : " + count + ", Reason : " + ex.ToString());
            }
            finally
            {
                this.concurrencyMutex.ReleaseMutex();
            }
        }
        public bool ShouldIPushCall()
        {
            return this.shouldIPoll && this.shouldIProcess;
        }
        public void WaitForLines()
        {
            long waitLoopCount = 0;
            while (this.currentConcurrency >= this.maximumConcurrency)
            {   
                Thread.Sleep(2000);
                SharedClass.Logger.Info("Waiting For Lines. Max : " + this.maximumConcurrency.ToString() + ", Current : " + this.currentConcurrency.ToString());
                ++waitLoopCount;
                if (waitLoopCount == 90) {
                    SharedClass.Logger.Info("Waiting For Lines. Max : " + this.maximumConcurrency.ToString() + ", Current : " + this.currentConcurrency.ToString());
                    waitLoopCount = 1;
                }
            }
        }
        protected void HeartBeat() {
            while (!SharedClass.HasStopSignal) {
                SharedClass.Logger.Info("Max : " + this.maximumConcurrency.ToString() + ", Current : " + this.currentConcurrency.ToString() + ", Available : " + (this.maximumConcurrency - this.currentConcurrency).ToString());
                Thread.Sleep(SharedClass.GatewayHeartBeatSpan * 1000);
            }
        }
        public string GetDisplayString()
        {
            return " GatewayID : " + this.id.ToString() + ", Name : " + this.name;
        }
        
        #region "PROPERTIES"
        public int Id { get { return this.id; } set { this.id = value; } } 
        public string Name { get { return this.name; } set { this.name = value; } } 
        public string ConnectUrl { get { return this.connectUrl; } set { this.connectUrl = value; } } 
        public string Ip { get { return this.ip; } set { this.ip = value; } } 
        public int Port { get { return this.port; } set { this.port = value; } }

        public int MaximumConcurrency
        {
            get
            {
                return this.maximumConcurrency;
            }
            set
            {
                this.maximumConcurrency = value;
            }
        }

        public int CurrenctConcurrency
        {
            get
            {
                return this.currentConcurrency;
            }
            set
            {
                this.currentConcurrency = value;
            }
        }

        public string OriginationUrl
        {
            get
            {
                return this.originationUrl;
            }
            set
            {
                this.originationUrl = value;
            }
        }

        public string ExtraDialString
        {
            get
            {
                return this.extraDialString;
            }
            set
            {
                this.extraDialString = value;
            }
        }
        public string CountryPrefix { get { return countryPrefix; } set { countryPrefix = value; } }
        public bool IsCountryPrefixAllowed { get { return isCountryPrefixAllowed; } set { isCountryPrefixAllowed = value; } }
        public string DialPrefix { get { return dialPrefix; } set { dialPrefix = value; } }
        public long HighPriorityQueueLastSlno { get { return this.highPriorityQueueLastSlno; } set { this.highPriorityQueueLastSlno = value; } }
        public long MediumPriorityQueueLastSlno { get { return this.mediumPriorityQueueLastSlno; } set { this.mediumPriorityQueueLastSlno = value; } }
        public long LowPriorityQueueLastSlno { get { return this.lowPriorityQueueLastSlno; } set { this.lowPriorityQueueLastSlno = value; } }
        public short PushThreadsTotal
        {
            get
            {
                return this.pushThreadsTotal;
            }
            set
            {
                this.pushThreadsTotal = value;
            }
        }

        public short PushThreadsRunning
        {
            get
            {
                return this.pushThreadsRunning;
            }
            set
            {
                this.pushThreadsRunning = value;
            }
        }
        #endregion
    }
}
