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
using System.Reflection;
using System.Xml;

namespace VoiceController
{
    [Serializable]
    public class Gateway
    {
        private int _id = 0;
        private string _name = string.Empty;
        private string _connectUrl = string.Empty;
        private string _ip = string.Empty;
        private int _port = 0;
        private int _maximumConcurrency = 0;
        private int _currentConcurrency = 0;
        private string _originationUrl = string.Empty;
        private string _extraDialString = string.Empty;
        private string _countryPrefix = string.Empty;
        private bool _isCountryPrefixAllowed = true;
        private string _dialPrefix = string.Empty;
        private long _urgentPriorityQueueLastSlno = 0;
        private long _highPriorityQueueLastSlno = 0;
        private long _mediumPriorityQueueLastSlno = 0;
        private long _lowPriorityQueueLastSlno = 0;
        private byte _numberOfPushThreads = 1;
        private byte _startingHour = 0;
        private byte _stoppingHour = 23;
        private byte _pushThreadsRunning = 0;
        private byte _pollThreadsRunning = 0;
        private bool _shouldIPoll = true;
        private bool _shouldIProcess = true;
        private Mutex _concurrencyMutex = new Mutex();
        private Mutex _queueCountMutex = new Mutex();
        private Mutex _pushThreadMutex = new Mutex();
        //private Thread _heartBeatThread = null;
        private Thread _upPollThread = null;
        private Thread _hpPollThread = null;
        private Thread _mpPollThread = null;
        private Thread _lpPollThread = null;
        private Thread _upPollThreadStaging = null;
        private Thread _hpPollThreadStaging = null;
        private Thread _mpPollThreadStaging = null;
        private Thread _lpPollThreadStaging = null;
        private Thread[] _pushThreads = null;
        private long _minTimeTaken = 2000;
        private long _maxTimeTaken = 0;
        private XmlDocument xmlDoc = null;
        private List<XmlElement> xmlElementList = new List<XmlElement>();
        private string numbersString = string.Empty;
        private CallsQueue _callsQueue = null;
        public void Start()
        {
            //System.Diagnostics.ProcessThreadCollection c = System.Diagnostics.Process.GetCurrentProcess().Threads;
            //foreach(System.Diagnostics.ProcessThread pt in c)
            //{
            //    pt.
            //}
            SharedClass.Logger.Info("Loading Into GatewayMap, " + this.GetDisplayString());
            lock (SharedClass.GatewayMap) {
                SharedClass.GatewayMap.Add(this._id, this);
            }
            try
            {
                if (File.Exists(this._name + ".ser"))
                {
                    System.Runtime.Serialization.IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                    Stream stream = new FileStream(this._name + ".ser", FileMode.Open, FileAccess.Read, FileShare.Read);
                    _callsQueue = (CallsQueue)formatter.Deserialize(stream);
                    stream.Close();
                    SharedClass.Logger.Info("Queue DeSerialized. UpQ : " + this._callsQueue.QueueCount(Priority.PriorityMode.Urgent) + ", HpQ : " + this._callsQueue.QueueCount(Priority.PriorityMode.High) + ", MpQ : " + this._callsQueue.QueueCount(Priority.PriorityMode.Medium) + ", LpQ : " + this._callsQueue.QueueCount(Priority.PriorityMode.Low));
                    try
                    {
                        File.Delete(this._name.Replace(" ", "") + ".ser");
                    }
                    catch (Exception e) {
                        SharedClass.Logger.Error("Error Deleting File : " + e.ToString());
                    }
                }
                else {
                    _callsQueue = new CallsQueue();
                }
            }
            catch (Exception e) {
                SharedClass.Logger.Error("Error DeSerializing Queue : " + e.ToString());
                _callsQueue = new CallsQueue();
            }
            ////For GroupCalls 
            this._upPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingGroupCallsFromDataBase));
            this._upPollThread.Name = this._name + "_UP_Poller";
            this._upPollThread.Start(new PollingInput(Priority.PriorityMode.Urgent, Environment.PRODUCTION));



            
            this._hpPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this._mpPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this._lpPollThread = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
            this._hpPollThread.Name = this._name + "_HP_Poller";
            this._mpPollThread.Name = this._name + "_MP_Poller";
            this._lpPollThread.Name = this._name + "_LP_Poller";
            this._hpPollThread.Start(new PollingInput(Priority.PriorityMode.High, Environment.PRODUCTION));
            this._mpPollThread.Start(new PollingInput(Priority.PriorityMode.Medium, Environment.PRODUCTION));
            this._lpPollThread.Start(new PollingInput(Priority.PriorityMode.Low, Environment.PRODUCTION));

            if (SharedClass.PollStaging)
            {
                this._upPollThreadStaging = new Thread(new ParameterizedThreadStart(this.GetPendingGroupCallsFromDataBase));
                this._upPollThreadStaging.Name = this._name + "_UP_Poller_Staging";
                this._upPollThreadStaging.Start(new PollingInput(Priority.PriorityMode.Urgent, Environment.STAGING));
                
                this._hpPollThreadStaging = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
                this._mpPollThreadStaging = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
                this._lpPollThreadStaging = new Thread(new ParameterizedThreadStart(this.GetPendingCallsFromDataBase));
                this._hpPollThreadStaging.Name = this._name + "_HP_Poller_Staging";
                this._mpPollThreadStaging.Name = this._name + "_MP_Poller_Staging";
                this._lpPollThreadStaging.Name = this._name + "_LP_Poller_Staging";
                this._hpPollThreadStaging.Start(new PollingInput(Priority.PriorityMode.High, Environment.STAGING));
                this._mpPollThreadStaging.Start(new PollingInput(Priority.PriorityMode.Medium, Environment.STAGING));
                this._lpPollThreadStaging.Start(new PollingInput(Priority.PriorityMode.Low, Environment.STAGING));
            }
            
            this._pushThreads = new Thread[this._numberOfPushThreads];
            for (byte index = 1; index <= this._numberOfPushThreads; ++index)
            {
                this._pushThreads[index - 1] = new Thread(new ThreadStart(this.StartPushing));
                this._pushThreads[index - 1].Name = this._name + "_Push_" + index;
                this._pushThreads[index - 1].Start();
            }
            //_heartBeatThread = new Thread(new ThreadStart(HeartBeat));
            //_heartBeatThread.Name = this._name + "_HB";
            //_heartBeatThread.Start();
        }

        public void Stop()
        {
            SharedClass.Logger.Info("Stopping");
            this._shouldIPoll = false;
            this._shouldIProcess = false;
            Thread.Sleep(500);
            for (byte index = 0; index < this._pushThreads.Count(); ++index)
            {
                //Enumerable.Count<Thread>((IEnumerable<Thread>)this.pushThreads
                while (this._pushThreads[index].ThreadState == ThreadState.WaitSleepJoin)
                {
                    this._pushThreads[index].Interrupt();
                    SharedClass.Logger.Info("PushThread : " + this._pushThreads[index].Name + " Is Running, ThreadState : " + this._pushThreads[index].ThreadState.ToString());
                    Thread.Sleep(2000);
                }                
                Thread.Sleep(2000);
            }
            while (this._pushThreadsRunning > 0) {
                SharedClass.Logger.Info("Still " + this._pushThreadsRunning + " Push Threads Running");
                Thread.Sleep(2000);
            }
            while (this._pollThreadsRunning > 0) {
                SharedClass.Logger.Info("Still " + this._pollThreadsRunning + " Pollers Running");
                Thread.Sleep(2000);
            }
            UpdateLastProcessedSlno();
            try
            {
                SharedClass.Logger.Info("Serializing Queue. UpQ : " + this._callsQueue.QueueCount(Priority.PriorityMode.Urgent) + ", HpQ : " + this._callsQueue.QueueCount(Priority.PriorityMode.High) + ", MpQ : " + this._callsQueue.QueueCount(Priority.PriorityMode.Medium) + ", LpQ : " + this._callsQueue.QueueCount(Priority.PriorityMode.Low));
                System.Runtime.Serialization.IFormatter formatter = new System.Runtime.Serialization.Formatters.Binary.BinaryFormatter();
                Stream stream = new FileStream(this._name + ".ser", FileMode.OpenOrCreate, FileAccess.Write, FileShare.None);
                formatter.Serialize(stream, this._callsQueue);
                stream.Close();
                SharedClass.Logger.Info("Queue Serialization Successful");
            }
            catch (Exception e) {
                SharedClass.Logger.Error("Error Serializing Queue : " + e.ToString());
            }
            lock (SharedClass.GatewayMap) {
                SharedClass.GatewayMap.Remove(this._id);
            }
        }
        //private void UpdateLastSlno(Priority.PriorityMode mode, long slno)
        //{
        //    switch (mode)
        //    {
        //        case Priority.PriorityMode.Urgent:
        //            this._urgentPriorityQueueLastSlno = slno;
        //            break;
        //        case Priority.PriorityMode.High:
        //            this.HighPriorityQueueLastSlno = slno;                    
        //            break;
        //        case Priority.PriorityMode.Medium:
        //            this.MediumPriorityQueueLastSlno = slno;                    
        //            break;
        //        default:
        //            this.LowPriorityQueueLastSlno = slno;                    
        //            break;
        //    }
        //}
        //private long GetLastSlno(Priority.PriorityMode mode)
        //{
        //    long num;
        //    switch (mode)
        //    {
        //        case Priority.PriorityMode.Urgent:
        //            num = this._urgentPriorityQueueLastSlno;
        //            break;
        //        case Priority.PriorityMode.High:
        //            num = this._highPriorityQueueLastSlno;
        //            break;
        //        case Priority.PriorityMode.Medium:
        //            num = this._mediumPriorityQueueLastSlno;
        //            break;
        //        default:
        //            num = this._lowPriorityQueueLastSlno;
        //            break;
        //    }
        //    return num;
        //}

        private void UpdateLastProcessedSlno()
        {
            SqlConnection sqlCon = (SqlConnection)null;
            SqlCommand sqlCmd = (SqlCommand)null;
            try
            {
                sqlCon = new SqlConnection(SharedClass.GetConnectionString(Environment.PRODUCTION));
                sqlCmd = new SqlCommand("Update VoiceGateways With(Rowlock) Set UPQLastSlno = @UPQLastSlno, HPQLastSlno = @HPQLastSlno, MPQLastSlno = @MPQLastSlno , LPQLastSlno = @LPQLastSlno Where ID = @GatewayId",sqlCon);
                sqlCmd.Parameters.Add("@HPQLastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this.Id, Environment.PRODUCTION, Priority.PriorityMode.High);
                sqlCmd.Parameters.Add("@MPQLastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this.Id, Environment.PRODUCTION, Priority.PriorityMode.Medium);
                sqlCmd.Parameters.Add("@LPQLastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this.Id, Environment.PRODUCTION, Priority.PriorityMode.Low);
                sqlCmd.Parameters.Add("@UPQLastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this.Id, Environment.PRODUCTION, Priority.PriorityMode.Urgent);
                sqlCmd.Parameters.Add("@GatewayId", SqlDbType.Int).Value = this.Id;
                sqlCon.Open();
                sqlCmd.ExecuteNonQuery();
                sqlCon.Close();
            }
            catch(Exception ex)
            {
                SharedClass.Logger.Error("Exception in Updating Last Processed Slno's Reason : " + ex.ToString());
            }

            if(SharedClass.PollStaging)
            {
                try
                {
                    sqlCon = new SqlConnection(SharedClass.GetConnectionString(Environment.STAGING));
                    sqlCmd = new SqlCommand("Update VoiceGateways With(Rowlock) Set UPQLastSlno = @UPQLastSlno, HPQLastSlno = @HPQLastSlno, MPQLastSlno = @MPQLastSlno , LPQLastSlno = @LPQLastSlno Where ID = @GatewayId", sqlCon);
                    sqlCmd.Parameters.Add("@HPQLastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this.Id, Environment.STAGING, Priority.PriorityMode.High);
                    sqlCmd.Parameters.Add("@MPQLastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this.Id, Environment.STAGING, Priority.PriorityMode.Medium);
                    sqlCmd.Parameters.Add("@LPQLastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this.Id, Environment.STAGING, Priority.PriorityMode.Low);
                    sqlCmd.Parameters.Add("@UPQLastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this.Id, Environment.STAGING, Priority.PriorityMode.Urgent);
                    sqlCmd.Parameters.Add("@GatewayId", SqlDbType.Int).Value = this.Id;
                    sqlCon.Open();
                    sqlCmd.ExecuteNonQuery();
                    sqlCon.Close();
                }
                catch (Exception ex)
                {
                    SharedClass.Logger.Error("Exception in Updating Last Processed Staging Slno's  Reason : " + ex.ToString());
                }
            }
        }

        private void GetPendingCallsFromDataBase(object input)
        {
            PollingInput pollingInput = (PollingInput)input;
            SqlConnection connection = new SqlConnection(SharedClass.GetConnectionString(pollingInput.Environment));
            SqlCommand sqlCommand = new SqlCommand("VC_Get_PendingCalls", connection);
            sqlCommand.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter da = null;
            DataSet ds = null;
            Call call = null;
            byte floorValue = 0;
            byte ceilValue = 10;
            //byte emptyCount = 0;
            switch (pollingInput.PriorityMode) { 
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
            while (!this._pushThreadMutex.WaitOne())
            {
                Thread.Sleep(100);
            } 
            ++this._pollThreadsRunning;
            this._pushThreadMutex.ReleaseMutex();
            SharedClass.Logger.Info("Started");
            while (this._shouldIPoll && !SharedClass.HasStopSignal)
            {
                try
                {
                    sqlCommand.Parameters.Clear();
                    sqlCommand.Parameters.Add("@GatewayId", SqlDbType.Int).Value = this._id;
                    sqlCommand.Parameters.Add("@LastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this._id, pollingInput.Environment, pollingInput.PriorityMode); //this.GetLastSlno(priorityMode);
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
                            try
                            {
                                call = new Call();
                                call.Environment = pollingInput.Environment;
                                call.QueueTableSlno = Convert.ToInt64(dataRow["Id"]);
                                CallsQueueSlno.SetSlno(this.Id, pollingInput.Environment, pollingInput.PriorityMode, Convert.ToInt64(dataRow["Id"]));
                                call.CallId = Convert.ToInt64(dataRow["CallId"]);
                                call.AccountId = Convert.ToInt64(dataRow["AccountId"]);
                                call.UUID = dataRow["UUID"].ToString();
                                call.CallerId = dataRow["CallerId"].ToString();
                                call.Destination = dataRow["Destination"].ToString();
                                call.Xml = dataRow["Xml"].ToString();
                                call.RingUrl = dataRow["RingUrl"].ToString();
                                call.AnswerUrl = dataRow["AnswerUrl"].ToString();
                                call.HangupUrl = dataRow["HangupUrl"].ToString();
                                call.Pulse = Convert.ToByte(dataRow["Pulse"]);
                                call.PricePerPulse = float.Parse(dataRow["PricePerPulse"].ToString());
                                call.PriorityValue = Convert.ToByte(dataRow["Priority"]);
                                this._callsQueue.EnQueue(call, pollingInput.PriorityMode);
                            }
                            catch (Exception e)
                            {
                                SharedClass.Logger.Error("Error In Constructing Call Object : " + e.ToString());                                
                                //try
                                //{
                                //    PropertyInfo[] properties = call.GetType().GetProperties();
                                //    foreach (PropertyInfo propertyInfo in properties)
                                //    {
                                //        if (propertyInfo.CanRead)
                                //        {
                                //            if (propertyInfo.GetValue(call) == DBNull.Value)
                                //                SharedClass.DumpLogger.Error(propertyInfo.Name + " : NULL");
                                //            else
                                //                SharedClass.DumpLogger.Error(propertyInfo.Name + " : " + propertyInfo.GetValue(call).ToString());
                                //        }
                                //    }
                                //}
                                //catch (Exception e1)
                                //{ }
                                //finally
                                //{

                                //}
                            }
                        }
                        //this.UpdateLastSlno(priorityMode, call.QueueTableSlno);
                        CallsQueueSlno.SetSlno(this._id, pollingInput.Environment, pollingInput.PriorityMode, call.QueueTableSlno);
                    }
                    else 
                    {
                        try
                        {
                            Thread.Sleep(2000);
                        }
                        catch (ThreadInterruptedException) 
                        { 

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
            SharedClass.Logger.Info("Stopped Polling, ShouldIPoll : " + this._shouldIPoll + ", Has Stop Signal : " + SharedClass.HasStopSignal);
            while (!this._pushThreadMutex.WaitOne()) {
                Thread.Sleep(100);
                SharedClass.Logger.Info("Waiting for pushThreadMutex to decrease pollThreadsRunning count");
            }
            --this._pollThreadsRunning;
            this._pushThreadMutex.ReleaseMutex();
            if (SharedClass.HasStopSignal) {
                return;
            }   
            string str = this.GetDisplayString() + " ";
            string text;
            switch (pollingInput.PriorityMode)
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

        private void GetPendingGroupCallsFromDataBase(object input)
        {
            PollingInput pollingInput = (PollingInput)input;
            SqlConnection connection = new SqlConnection(SharedClass.GetConnectionString(pollingInput.Environment));
            SqlCommand sqlCommand = new SqlCommand("VC_Get_PendingGroupCalls", connection);
            sqlCommand.CommandType = CommandType.StoredProcedure;
            SqlDataAdapter da = null;
            DataSet ds = null;
            Call call = null;
            while (!this._pushThreadMutex.WaitOne())
            {
                Thread.Sleep(100);
            }
            ++this._pollThreadsRunning;
            this._pushThreadMutex.ReleaseMutex();
            SharedClass.Logger.Info("Started");

            while (this._shouldIPoll && !SharedClass.HasStopSignal)
            {
                try
                {
                    sqlCommand.Parameters.Clear();
                    sqlCommand.Parameters.Add("@GatewayId", SqlDbType.Int).Value = this._id;
                    sqlCommand.Parameters.Add("@LastSlno", SqlDbType.BigInt).Value = CallsQueueSlno.GetSlno(this._id, pollingInput.Environment, pollingInput.PriorityMode); 
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
                            try
                            {
                                call = new Call();
                                call.Environment = pollingInput.Environment;
                                call.QueueTableSlno = Convert.ToInt64(dataRow["Id"]);
                                CallsQueueSlno.SetSlno(this.Id, pollingInput.Environment, pollingInput.PriorityMode, Convert.ToInt64(dataRow["Id"]));
                                call.CallId = Convert.ToInt64(dataRow["CallId"]);
                                call.AccountId = Convert.ToInt64(dataRow["AccountId"]);
                                call.UUID = dataRow["UUID"].ToString();
                                call.CallerId = dataRow["CallerId"].ToString();
                                call.Destination = dataRow["Destination"].ToString();
                                call.Xml = dataRow["Xml"].ToString();
                                call.RingUrl = dataRow["RingUrl"].ToString();
                                call.AnswerUrl = dataRow["AnswerUrl"].ToString();
                                call.HangupUrl = dataRow["HangupUrl"].ToString();
                                call.Pulse = Convert.ToByte(dataRow["Pulse"]);
                                call.PricePerPulse = float.Parse(dataRow["PricePerPulse"].ToString());
                                call.PriorityValue = Convert.ToByte(dataRow["Priority"]);
                                call.IsGroupCall = true;
                                this._callsQueue.EnQueue(call, pollingInput.PriorityMode);
                            }
                            catch (Exception e)
                            {
                                SharedClass.Logger.Error("Error In Constructing Call Object : " + e.ToString());
                                try
                                {
                                    PropertyInfo[] properties = call.GetType().GetProperties();
                                    foreach (PropertyInfo propertyInfo in properties)
                                    {
                                        if (propertyInfo.CanRead)
                                        {
                                            if (propertyInfo.GetValue(call) == DBNull.Value)
                                                SharedClass.DumpLogger.Error(propertyInfo.Name + " : NULL");
                                            else
                                                SharedClass.DumpLogger.Error(propertyInfo.Name + " : " + propertyInfo.GetValue(call).ToString());
                                        }
                                    }
                                }
                                catch (Exception e1)
                                { }
                                finally
                                {

                                }
                            }
                        }
                        //this.UpdateLastSlno(priorityMode, call.QueueTableSlno);
                        CallsQueueSlno.SetSlno(this._id, pollingInput.Environment, pollingInput.PriorityMode, call.QueueTableSlno);
                    }
                    else
                    {
                        try
                        {
                            Thread.Sleep(2000);
                        }
                        catch(ThreadAbortException)
                        {

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

            SharedClass.Logger.Info("Stopped Polling, ShouldIPoll : " + this._shouldIPoll + ", Has Stop Signal : " + SharedClass.HasStopSignal);
            while (!this._pushThreadMutex.WaitOne())
            {
                Thread.Sleep(100);
                SharedClass.Logger.Info("Waiting for pushThreadMutex to decrease pollThreadsRunning count");
            }
            --this._pollThreadsRunning;
            this._pushThreadMutex.ReleaseMutex();
            if (SharedClass.HasStopSignal)
            {
                return;
            }
            string str = this.GetDisplayString() + " ";
            string text;
            text = str + "UP Poller Stopped";

            
            lock (SharedClass.Notifier)
            {
                SharedClass.Notifier.SendSms(text);
            }
        }

        private void StartPushing()
        {
            int loopCount = 0;
            long startTime = 0;
            long timeTaken = 0;
            while (!this._pushThreadMutex.WaitOne()) {
                Thread.Sleep(10);
            }
            ++this._pushThreadsRunning;
            this._pushThreadMutex.ReleaseMutex();
            SharedClass.Logger.Info("Started");
            Call call = null;
            while (this._shouldIProcess && !SharedClass.HasStopSignal)
            {   
                try
                {
                    if ((call = this._callsQueue.DeQueue()) == null)
                    {
                        try
                        {
                            Thread.Sleep(2000);
                        }
                        catch (ThreadInterruptedException e) { }
                    }
                    else
                    {
                        SharedClass.Logger.Info("Dequeued Call of CallId : " + call.QueueTableSlno.ToString() + " Source: " + call.Environment.ToString());
                        //this.WaitForLines();
                        
                        if (!this.ShouldIPushCall()) 
                        {
                            
                            this._callsQueue.EnQueue(call, Priority.GetPriority(call.PriorityValue));
                            call = null;
                            continue;
                        }
                        
                        startTime = SharedClass.CurrentTimeStamp();
                        this.ProcessCall(call);
                        timeTaken = SharedClass.CurrentTimeStamp() - startTime;
                        call = null;
                        if (timeTaken < this._minTimeTaken)
                            this._minTimeTaken = timeTaken;
                        if (timeTaken > this._maxTimeTaken)
                            this._maxTimeTaken = timeTaken;
                        ++loopCount;
                        if (loopCount == 50)
                        {
                            SharedClass.Logger.Info("Processed " + loopCount + " Calls : minTime => " + _minTimeTaken + ", maxTime => " + _maxTimeTaken);
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
                        this._callsQueue.EnQueue(call, Priority.GetPriority(call.PriorityValue));
                    }
                }
            }
            SharedClass.Logger.Info("Exited From While Loop and is about to die");
            while (!this._pushThreadMutex.WaitOne()) {
                Thread.Sleep(10);
            }   
            --this._pushThreadsRunning;
            this._pushThreadMutex.ReleaseMutex();
        }
        public void ProcessCall(Call call)
        {
            SharedClass.Logger.Info("Processing Call of CallId : " + call.QueueTableSlno.ToString() + " Source: " + call.Environment.ToString());
            HttpWebRequest request = null;
            HttpWebResponse response = null;
            StreamReader streamReader = null;
            StreamWriter streamWriter = null;
            try
            {
                if (!this._isCountryPrefixAllowed && this._countryPrefix.Length > 0 && call.Destination.StartsWith(this._countryPrefix))
                    call.Destination = call.Destination.Substring(this._countryPrefix.Length);
                if (this._dialPrefix.Length > 0)
                    call.Destination = this._dialPrefix + call.Destination;
                string payload = "From=" + call.CallerId.UrlEncode() + "&To=" + call.Destination.UrlEncode() + "&OriginationUUID=" + call.UUID.UrlEncode() + "&Gateways=" + this.OriginationUrl.UrlEncode();
                payload += "&SequenceNumber=" + call.CallId + "&AnswerUrl=" + call.AnswerUrl.UrlEncode() + "&HangupUrl=" + ((call.HangupUrl != null && call.HangupUrl.Trim().Length > 0) ? call.HangupUrl : call.AnswerUrl).UrlEncode();
                //payload += "&CallBackByRMQ=1";
                payload += "&NotifyCallFlow=RMQ&CallBackByRMQ=1";
                payload += "&GwID=" + this._id.ToString();
                if (call.Environment == Environment.STAGING)
                {
                    if (call.IsGroupCall)
                        payload += "&Source=" + Environment.STAGINGGROUPCALL.ToString();
                    else
                        payload += "&Source=" + Environment.STAGING.ToString();
                    //payload += "&Source=" + Environment.STAGING.ToString();
                }   
                else
                {
                    if (call.IsGroupCall)
                        payload += "&Source=" + Environment.PRODUCTIONGROUPCALL.ToString();
                    else
                        payload += "&Source=" + Environment.PRODUCTION.ToString();
                    //payload += "&Source=" + Environment.PRODUCTION.ToString();
                }
                    
                if (call.RingUrl.Length > 0)
                    payload = payload + "&RingUrl=" + call.RingUrl.UrlEncode();
                payload += "&ActionMethod=POST";
                //if (call.Xml.Length > 0)
                //    payload += "&AnswerXml=" + call.Xml.UrlEncode();
                if(call.Xml.Length > 0)
                {
                    try
                    {
                        this.xmlDoc = new XmlDocument();
                        xmlDoc.LoadXml(call.Xml);
                        string tempNumber = string.Empty;
                        if (this.xmlDoc.SelectNodes("/Response/Dial").Count > 0)
                        {
                            this.xmlElementList = new List<XmlElement>();
                            foreach (XmlNode node in xmlDoc.SelectNodes("/Response/Dial/Number"))
                            {
                                tempNumber = node.InnerText;

                                if (!this._isCountryPrefixAllowed && this._countryPrefix.Length > 0 && tempNumber.StartsWith(this._countryPrefix))
                                    tempNumber = tempNumber.Substring(this._countryPrefix.Length);
                                if (this._dialPrefix.Length > 0)
                                {
                                    if(!tempNumber.StartsWith(this._dialPrefix))
                                        tempNumber = this._dialPrefix + tempNumber;
                                }
                                    

                                XmlElement tempXmlElement = xmlDoc.CreateElement("Number");
                                tempXmlElement.InnerText = tempNumber;
                                tempXmlElement.SetAttribute("gateways", this.OriginationUrl);
                                xmlElementList.Add(tempXmlElement);
                            }
                            xmlDoc.SelectSingleNode("/Response/Dial").InnerXml = "";
                            foreach (XmlElement element in xmlElementList)
                            {
                                xmlDoc.SelectSingleNode("/Response/Dial").AppendChild(element);
                            }
                        }
                        else if (xmlDoc.SelectNodes("/Response/dial").Count > 0)
                        {
                            numbersString = string.Empty;
                            foreach (string number in xmlDoc.SelectSingleNode("/Response/dial").InnerText.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries))
                            {
                                tempNumber = number;

                                if (!this._isCountryPrefixAllowed && this._countryPrefix.Length > 0 && tempNumber.StartsWith(this._countryPrefix))
                                    tempNumber = tempNumber.Substring(this._countryPrefix.Length);
                                if (this._dialPrefix.Length > 0)
                                    tempNumber = this._dialPrefix + tempNumber;

                                numbersString = numbersString + ",";
                            }
                            xmlDoc.SelectSingleNode("/Response/dial").InnerText = "";
                            numbersString = numbersString.Substring(0, numbersString.Length - 1);
                            xmlDoc.SelectSingleNode("/Response/dial").InnerText = numbersString;
                        }

                        payload += "&AnswerXml=" + Convert.ToString(xmlDoc.InnerXml).UrlEncode();
                    }
                    catch(Exception ex)
                    {
                        SharedClass.Logger.Error("Error in removing prefix in AnswerXML Reason: " + ex.ToString());
                        payload += "&AnswerXml=" + call.Xml.UrlEncode();
                    }
                    finally
                    {
                        this.xmlDoc = null;
                        this.xmlElementList = null;
                    }
                    
                }

                if (this.ExtraDialString.Length > 0)
                    payload += "&ExtraDialString=" + this.ExtraDialString.UrlEncode();
                SharedClass.Logger.Info("Payload for call of Id : "+ call.QueueTableSlno.ToString() +" is : " + payload);
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
                    streamWriter.Dispose();
                    streamReader.Dispose();                    
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
                while (!this._concurrencyMutex.WaitOne())
                    Thread.Sleep(10);
                if (isHangup)
                    this._currentConcurrency -= count;
                else
                    this._currentConcurrency += count;
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error Updating Gateway Concurrency, Id " + this._id + ", IsHangup : " + isHangup + ", Count : " + count + ", Reason : " + ex.ToString());
            }
            finally
            {
                this._concurrencyMutex.ReleaseMutex();
            }
        }
        public bool ShouldIPushCall()
        {
            return this._shouldIPoll && this._shouldIProcess && !SharedClass.HasStopSignal;
        }
        public void WaitForLines()
        {
            long waitCount = 0;
            while (this._currentConcurrency >= this._maximumConcurrency)
            {   
                Thread.Sleep(1000);                
                ++waitCount;
                if (waitCount == 30) {
                    SharedClass.Logger.Info("Waiting For Lines. Max : " + this._maximumConcurrency.ToString() + ", Current : " + this._currentConcurrency.ToString());
                    waitCount = 0;
                }
            }
        }
        protected void HeartBeat() {
            sbyte loopCount = 0;
            while (!SharedClass.HasStopSignal) {                
                ++loopCount;
                if (loopCount == 5)
                {
                    SharedClass.HeartBeatLogger.Info("Max : " + this._maximumConcurrency.ToString() + ", Current : " + this._currentConcurrency.ToString() + ", Available : " + (this._maximumConcurrency - this._currentConcurrency).ToString());
                    loopCount = 0;
                }
                Thread.Sleep(SharedClass.GatewayHeartBeatSpan * 1000);
            }
        }
        public string GetDisplayString()
        {
            return " GatewayID : " + this._id.ToString() + ", Name : " + this._name;
        }
        private class PollingInput
        {
            private Priority.PriorityMode _priorityMode = Priority.PriorityMode.Low;
            private Environment _environment = Environment.PRODUCTION;
            public PollingInput(Priority.PriorityMode priorityMode, Environment environment)
            {
                this._priorityMode = priorityMode;
                this._environment = environment;
            }
            public Priority.PriorityMode PriorityMode
            {
                get { return this._priorityMode; }
                set { this._priorityMode = value; }
            }
            public Environment Environment
            {
                get { return this._environment; }
                set { this._environment = value; }
            }
        }
        
        #region "PROPERTIES"
        public int Id { get { return this._id; } set { this._id = value; } } 
        public string Name { get { return this._name; } set { this._name = value; } } 
        public string ConnectUrl { get { return this._connectUrl; } set { this._connectUrl = value; } } 
        public string Ip { get { return this._ip; } set { this._ip = value; } } 
        public int Port { get { return this._port; } set { this._port = value; } } 
        public int MaximumConcurrency { get { return this._maximumConcurrency; } set { this._maximumConcurrency = value; } } 
        public int CurrenctConcurrency { get { return this._currentConcurrency; } set { this._currentConcurrency = value; } } 
        public string OriginationUrl { get { return this._originationUrl; } set { this._originationUrl = value; } }
        public string ExtraDialString { get { return this._extraDialString; } set { this._extraDialString = value; } }
        public string CountryPrefix { get { return _countryPrefix; } set { _countryPrefix = value; } }
        public bool IsCountryPrefixAllowed { get { return _isCountryPrefixAllowed; } set { _isCountryPrefixAllowed = value; } }
        public string DialPrefix { get { return _dialPrefix; } set { _dialPrefix = value; } }
        public byte StartingHour { get { return this._startingHour; } set { this._startingHour = value; } }
        public byte StoppingHour { get { return this._stoppingHour; } set { this._stoppingHour = value; } }
        public long UrgentPriorityQueueLastSlno { get { return this._urgentPriorityQueueLastSlno; } set { this._urgentPriorityQueueLastSlno = value; } }
        public long HighPriorityQueueLastSlno { get { return this._highPriorityQueueLastSlno; } set { this._highPriorityQueueLastSlno = value; } }
        public long MediumPriorityQueueLastSlno { get { return this._mediumPriorityQueueLastSlno; } set { this._mediumPriorityQueueLastSlno = value; } }
        public long LowPriorityQueueLastSlno { get { return this._lowPriorityQueueLastSlno; } set { this._lowPriorityQueueLastSlno = value; } }
        public byte NumberOfPushThreads { get { return this._numberOfPushThreads; } set { this._numberOfPushThreads = value; } } 
        public byte PushThreadsRunning { get { return this._pushThreadsRunning; } set { this._pushThreadsRunning = value; } }
        #endregion
    }
}
