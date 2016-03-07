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
        private string originationUrl = "";
        private string extraDialString = "";
        private long highPriorityQueueLastSlno = 0L;
        private long mediumPriorityQueueLastSlno = 0L;
        private long lowPriorityQueueLastSlno = 0L;
        private short pushThreadsTotal = (short)1;
        private short pushThreadsRunning = (short)0;
        private short pollThreadsRunning = 0;
        private bool shouldIPoll = true;
        private bool shouldIProcess = true;
        private Mutex concurrencyMutex = new Mutex();
        private Mutex queueCountMutex = new Mutex();
        private Mutex pushThreadMutex = new Mutex();
        //private Thread heartBeatThread = (Thread)null;
        private Thread hpPollThread = (Thread)null;
        private Thread mpPollThread = (Thread)null;
        private Thread lpPollThread = (Thread)null;
        private Thread[] pushThreads = (Thread[])null;
        private long minTimeTaken = 2000L;
        private long maxTimeTaken = 0L;
        public CallsQueue CallsQueue = (CallsQueue)null;
        public void Start()
        {
            SharedClass.Logger.Info((object)("Loading Into GatewayMap, " + this.GetDisplayString()));
            lock (SharedClass.GatewayMap)
                SharedClass.GatewayMap.Add(this.id, this);
            try
            {
                if (File.Exists(this.name + ".bin"))
                {
                    System.Runtime.Serialization.IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                    Stream stream = new FileStream(this.name + ".bin", FileMode.Open, FileAccess.Read, FileShare.Read);
                    CallsQueue = (CallsQueue)formatter.Deserialize(stream);
                    stream.Close();
                    SharedClass.Logger.Info("Queue DeSerialized. HpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.High) + ", MpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Medium) + ", LpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Low));
                    try
                    {
                        File.Delete(this.name.Replace(" ", "") + ".bin");
                    }
                    catch (Exception e) {
                        SharedClass.Logger.Error("Error Deleting Bin File");
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
            this.hpPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this.mpPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this.lpPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this.hpPollThread.Name = this.name + "_HP_Poller";
            this.mpPollThread.Name = this.name + "_MP_Poller";
            this.lpPollThread.Name = this.name + "_LP_Poller";
            this.hpPollThread.Start(Priority.PriorityMode.High);
            this.mpPollThread.Start(Priority.PriorityMode.Medium);
            this.lpPollThread.Start(Priority.PriorityMode.Low);
            this.pushThreads = new Thread[this.pushThreadsTotal];
            for (short index = 1; index <= this.pushThreadsTotal; ++index)
            {
                this.pushThreads[index - 1] = new Thread(new ThreadStart(this.StartPushing));
                this.pushThreads[index - 1].Name = this.name + "_Push_" + index;
                //this.pushThreads[index - 1].Start();
            }
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
                    this.pushThreads[index].Interrupt();
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
                System.Runtime.Serialization.IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                Stream stream = new FileStream(this.name + ".bin", FileMode.Create, FileAccess.Write, FileShare.None);
                formatter.Serialize(stream, this.CallsQueue);
                stream.Close();
                SharedClass.Logger.Info("Queue Serialized. HpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.High) + ", MpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Medium) + ", LpQ : " + this.CallsQueue.QueueCount(Priority.PriorityMode.Low));
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
                case Priority.PriorityMode.High:
                    this.HighPriorityQueueLastSlno = slno;
                    SharedClass.Logger.Info("HP Last Slno : " + slno);
                    break;
                case Priority.PriorityMode.Medium:
                    this.MediumPriorityQueueLastSlno = slno;
                    SharedClass.Logger.Info("MP Last Slno : " + slno);
                    break;
                default:
                    this.LowPriorityQueueLastSlno = slno;
                    SharedClass.Logger.Info("LP Last Slno : " + slno);
                    break;
            }
        }
        private long GetLastSlno(Priority.PriorityMode mode)
        {
            long num;
            switch (mode)
            {
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
            SqlDataAdapter da = (SqlDataAdapter)null;
            DataSet ds = (DataSet)null;
            Call call = (Call)null;
            short floorValue = 0;
            short ceilValue = 10;
            lock (SharedClass.PriorityObj)
            {
                switch (priorityMode)
                {
                    case Priority.PriorityMode.High:
                        floorValue = (short)SharedClass.PriorityObj.HpFloor;
                        ceilValue = (short)SharedClass.PriorityObj.HpCeil;
                        break;
                    case Priority.PriorityMode.Medium:
                        floorValue = (short)SharedClass.PriorityObj.MpFloor;
                        ceilValue = (short)SharedClass.PriorityObj.MpCeil;
                        break;
                    default:
                        floorValue = (short)SharedClass.PriorityObj.LpFloor;
                        ceilValue = (short)SharedClass.PriorityObj.LpCeil;
                        break;
                }
            }
            while (!this.pushThreadMutex.WaitOne())
                Thread.Sleep(10);
            ++this.pollThreadsRunning;
            this.pushThreadMutex.ReleaseMutex();
            SharedClass.Logger.Info((object)"Started");
            while (this.shouldIPoll && !SharedClass.HasStopSignal)
            {
                try
                {
                    sqlCommand.Parameters.Clear();
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
                            call.MobileId = Convert.ToInt64(dataRow["MobileId"]);
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
                        this.UpdateLastSlno(Priority.GetPriority(call.PriorityValue), call.QueueTableSlno);
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
                    SharedClass.Logger.Error((object)("Error In Polling, " + ex.ToString()));
                }
                finally
                {
                    da = null;
                    ds = null;
                }
            }
            SharedClass.Logger.Info("Stopped Polling, ShouldIPoll : " + this.shouldIPoll + ", Has Stop Signal : " + SharedClass.HasStopSignal);
            while (!this.pushThreadMutex.WaitOne())
                Thread.Sleep(10);
            --this.pollThreadsRunning;
            this.pushThreadMutex.ReleaseMutex();
            if (SharedClass.HasStopSignal)
                return;
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
            lock (SharedClass.Notifier)
                SharedClass.Notifier.SendSms(text);
        }
        private void StartPushing()
        {
            int loopCount = 0;
            while (!this.pushThreadMutex.WaitOne())
                Thread.Sleep(10);
            ++this.pushThreadsRunning;
            this.pushThreadMutex.ReleaseMutex();
            SharedClass.Logger.Info("Started");
            while (this.shouldIProcess)
            {
                try
                {
                    Call call;
                    if ((call = this.CallsQueue.DeQueue()) == null)
                    {
                        try
                        {
                            Thread.Sleep(2000);
                        }
                        catch (ThreadInterruptedException e) { }
                    }
                    else if (this.ShouldIPushCall())
                    {
                        this.WaitForLines();
                        long startTime = SharedClass.CurrentTimeStamp();
                        this.ProcessCall(call);
                        long timeTaken = SharedClass.CurrentTimeStamp() - startTime;
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
            }
            SharedClass.Logger.Info("Exited From While Loop and is about to die");
            while (!this.pushThreadMutex.WaitOne())
                Thread.Sleep(10);
            --this.pushThreadsRunning;
            this.pushThreadMutex.ReleaseMutex();
        }
        public void ProcessCall(Call call)
        {
            HttpWebRequest request = (HttpWebRequest)null;
            HttpWebResponse response = (HttpWebResponse)null;
            StreamReader streamReader = (StreamReader)null;
            StreamWriter streamWriter = (StreamWriter)null;
            Exception exception;
            try
            {
                string payload = "From=" + call.CallerId + "&To=" + call.Destination + "&OriginationUUID=" + call.UUID + "&Gateways=" + this.OriginationUrl + "&SequenceNumber=" + call.MobileId + "&AnswerUrl=" + call.AnswerUrl + "&HangupUrl=" + call.HangupUrl;
                if (call.RingUrl.Length > 0)
                    payload = payload + "&RingUrl=" + call.RingUrl;
                payload += "&ActionMethod=POST";
                if (call.Xml.Length > 0)
                    payload += "&AnswerXml=" + call.Xml;
                if (this.ExtraDialString.Length > 0)
                    payload += "&ExtraDialString=" + this.ExtraDialString;
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
                exception = ex;
                SharedClass.Logger.Info("Error Processing Call : " + call.PrintMe());
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
                    exception = ex;
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
            while (this.currentConcurrency >= this.maximumConcurrency)
            {
                SharedClass.Logger.Info((object)string.Concat(new object[4]
        {
          (object) "Waiting For Lines. Max ",
          (object) this.maximumConcurrency,
          (object) ", Current : ",
          (object) this.currentConcurrency
        }));
                Thread.Sleep(2000);
            }
        }
        public string GetDisplayString()
        {
            return " GatewayID : " + this.id.ToString() + ", Name : " + this.name;
        }
        
        #region "PROPERTIES"
        public int Id
        {
            get
            {
                return this.id;
            }
            set
            {
                this.id = value;
            }
        }

        public string Name
        {
            get
            {
                return this.name;
            }
            set
            {
                this.name = value;
            }
        }

        public string ConnectUrl
        {
            get
            {
                return this.connectUrl;
            }
            set
            {
                this.connectUrl = value;
            }
        }

        public string Ip
        {
            get
            {
                return this.ip;
            }
            set
            {
                this.ip = value;
            }
        }

        public int Port
        {
            get
            {
                return this.port;
            }
            set
            {
                this.port = value;
            }
        }

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

        public long HighPriorityQueueLastSlno
        {
            get
            {
                return this.highPriorityQueueLastSlno;
            }
            set
            {
                this.highPriorityQueueLastSlno = value;
            }
        }

        public long MediumPriorityQueueLastSlno
        {
            get
            {
                return this.mediumPriorityQueueLastSlno;
            }
            set
            {
                this.mediumPriorityQueueLastSlno = value;
            }
        }

        public long LowPriorityQueueLastSlno
        {
            get
            {
                return this.lowPriorityQueueLastSlno;
            }
            set
            {
                this.lowPriorityQueueLastSlno = value;
            }
        }

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
