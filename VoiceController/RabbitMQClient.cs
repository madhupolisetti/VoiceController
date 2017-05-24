using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Newtonsoft.Json.Linq;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.Threading;
using System.IO;
using System.Data;
using System.Data.SqlClient;

namespace VoiceController
{
    public class RabbitMQClient
    {
        private string _host = string.Empty;
        private int _port = 0;
        private string _user = "guest";
        private string _password = "guest";
        private bool _isConnected = false;
        private bool _isConnectSignalInProgress = false;
        private Thread _connectThread = null;
        private Thread _hangupDeQueueThread = null;
        private Thread _callFlowsDeQueueThread = null;
        private Thread _callBacksDeQueueThread = null;
        public ConnectionFactory connectionFactory = null;
        public IConnection connection = null;
        public IModel channel = null;
        public QueueingBasicConsumer hangupConsumer = null;
        public QueueingBasicConsumer callFlowsConsumer = null;
        public QueueingBasicConsumer callBacksConsumer = null;
        public QueueingBasicConsumer hangupLazyConsumer = null;
        public string hangupConsumerTag = null;
        public string callFlowsConsumerTag = null;
        public string callBacksConsumerTag = null;
        public string hangupLazyConsumerTag = null;
        public IBasicProperties channelProperties = null;
        
        #region CALLFLOW_VARIABLES
        
        private JObject _callFlowObject = null;
        private JObject _callBackObject = null;
        private SqlConnection _callFlowsSqlConnection = null;
        private SqlConnection _callFlowsSqlConnectionStaging = null;
        private SqlConnection _groupCallFlowsSqlConnectionStaging = null;
        private SqlConnection _groupCallFlowsSqlConnectionProduction = null;
        private SqlCommand _callFlowsSqlCommandStaging = null;
        private SqlCommand _callFlowsSqlCommand = null;
        private SqlCommand _groupCallFlowsSqlCommandProduction = null;
        private SqlCommand _groupCallFlowsSqlCommandStaging = null;
        
        private SqlCommand _callBacksSqlCommandStaging = null;
        private SqlCommand _callBacksSqlCommandProduction = null;
        private SqlConnection _callBacksSqlConnectionProduction = null;
        private SqlConnection _callBacksSqlConnectionStaging = null;


        private SqlCommand _groupCallBacksSqlCommandStaging = null;
        private SqlCommand _groupCallBacksSqlCommandProduction = null;
        private SqlConnection _groupCallBacksSqlConnectionStaging = null;
        private SqlConnection _groupCallBacksSqlConnectionProduction = null;
        
        
        private JObject _paramObject = null;
        private string[] _paramArray = null;
        private Dictionary<string, object> _paramPairs = null;
        private long _callId = 0;
        private string _eventName = string.Empty;
        private long _ringTimeStamp = 0;
        private long _answerTimeStamp = 0;
        private long _endTimeStamp = 0;
        private byte _retryAttempt = 0;
        private string _caller = string.Empty;
        private string _callee = string.Empty;
        private string _recordingUrl = string.Empty;
        private string _digits = string.Empty;
        private string _endBy = string.Empty;
        private string _callStatus = string.Empty;
        
        #endregion
        public void Start()
        {
            _callFlowsSqlConnection = new SqlConnection(SharedClass.GetConnectionString(Environment.PRODUCTION));
            _groupCallFlowsSqlConnectionProduction = new SqlConnection(SharedClass.GetConnectionString(Environment.PRODUCTION));
            _groupCallBacksSqlConnectionProduction = new SqlConnection(SharedClass.GetConnectionString(Environment.PRODUCTION));
            _callBacksSqlConnectionProduction = new SqlConnection(SharedClass.GetConnectionString(Environment.PRODUCTION));

            if(SharedClass.PollStaging)
            {
                _callFlowsSqlConnectionStaging = new SqlConnection(SharedClass.GetConnectionString(Environment.STAGING));
                _callFlowsSqlCommandStaging = new SqlCommand("VC_Create_CallFlow", _callFlowsSqlConnectionStaging);
                _callFlowsSqlCommandStaging.CommandType = CommandType.StoredProcedure;
                _groupCallFlowsSqlConnectionStaging = new SqlConnection(SharedClass.GetConnectionString(Environment.STAGING));
                _groupCallFlowsSqlCommandStaging = new SqlCommand("VC_Create_GroupCallFlow", _groupCallFlowsSqlConnectionStaging);
                _groupCallFlowsSqlCommandStaging.CommandType = CommandType.StoredProcedure;
                _groupCallBacksSqlConnectionStaging = new SqlConnection(SharedClass.GetConnectionString(Environment.STAGING));
                _groupCallBacksSqlCommandStaging = new SqlCommand("VC_Create_GroupCallBacks", _groupCallBacksSqlConnectionStaging);
                _groupCallBacksSqlCommandStaging.CommandType = CommandType.StoredProcedure;
                _callBacksSqlConnectionStaging = new SqlConnection(SharedClass.GetConnectionString(Environment.STAGING));
                _callBacksSqlCommandStaging = new SqlCommand("VC_Create_CallBack", _callBacksSqlConnectionStaging);
                _callBacksSqlCommandStaging.CommandType = CommandType.StoredProcedure;
            }
                
            _callFlowsSqlCommand = new SqlCommand("VC_Create_CallFlow", _callFlowsSqlConnection);
            _groupCallFlowsSqlCommandProduction = new SqlCommand("VC_Create_GroupCallFlow", _groupCallFlowsSqlConnectionProduction);
            _groupCallBacksSqlCommandProduction = new SqlCommand("VC_Create_GroupCallBacks", _groupCallBacksSqlConnectionProduction);
            _callBacksSqlCommandProduction = new SqlCommand("VC_Create_CallBack",_callBacksSqlConnectionProduction);
            _callFlowsSqlCommand.CommandType = CommandType.StoredProcedure;
            _groupCallFlowsSqlCommandProduction.CommandType = CommandType.StoredProcedure;
            _groupCallBacksSqlCommandProduction.CommandType = CommandType.StoredProcedure; 
            _callBacksSqlCommandProduction.CommandType = CommandType.StoredProcedure;
            this._connectThread = new Thread(new ThreadStart(this.ConnectToServer));
            this._connectThread.Name = "RMQConnector";
            this._connectThread.Start();
            this._hangupDeQueueThread = new Thread(new ThreadStart(this.DeQueueHangups));
            this._hangupDeQueueThread.Name = "HangupListener";
            this._hangupDeQueueThread.Start();
            this._callFlowsDeQueueThread = new Thread(new ThreadStart(this.DeQueueCallFlows));
            this._callFlowsDeQueueThread.Name = "CallFlows";
            this._callFlowsDeQueueThread.Start();
            this._callBacksDeQueueThread = new Thread(new ThreadStart(this.DeQueueCallBacks));
            this._callBacksDeQueueThread.Name = "CallBacks";
            this._callBacksDeQueueThread.Start();
            
        }

        public void Stop()
        {
            try
            {
                if (this.connection.IsOpen)
                {
                    this.connection.Close();
                    SharedClass.Logger.Info("RabbitMQ Connection Closed");
                }
                else
                    SharedClass.Logger.Info("RabbitMQ Connection is not in open state. Unable to issue Close command");
                this.connection = null;
                this.connectionFactory = null;
                this.channel = null;
                this.channelProperties = null;
                this.hangupConsumer = null;
                this.hangupLazyConsumer = null;
                this.callFlowsConsumer = null;
                if (this._hangupDeQueueThread.ThreadState == ThreadState.WaitSleepJoin)
                    this._hangupDeQueueThread.Interrupt();
                if (this._callFlowsDeQueueThread.ThreadState == ThreadState.WaitSleepJoin)
                    this._callFlowsDeQueueThread.Interrupt();                    
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error Stopping RabbitMQClient, " + ex.ToString());
            }
            finally
            {
                SharedClass.RabbitMQClient = null;
            }
        }

        public void ConnectToServer()
        {
            int millisecondsTimeout = 3000;
            if (this._isConnectSignalInProgress)
            {
                SharedClass.Logger.Info("A connect signal already in progress. Termination connect signal");
            }
            else
            {
                this._isConnectSignalInProgress = true;
                this._isConnected = false;
                SharedClass.Logger.Info("Initializing RabbitMQ Connection, Host " + this._host + ", Port : " + this._port.ToString() + ", User : " + this._user + ", Password : " + this._password);
                while (!SharedClass.HasStopSignal)
                {
                    try
                    {
                        this.connectionFactory = new ConnectionFactory();
                        this.connectionFactory.HostName = this._host;
                        this.connectionFactory.Port = this._port;
                        this.connectionFactory.UserName = this._user;
                        this.connectionFactory.Password = this._password;
                        this.connection = this.connectionFactory.CreateConnection();
                        this.channel = this.connection.CreateModel();
                        this.channel.QueueDeclare("Hangups", true, false, false, null);
                        this.channel.QueueDeclare("CallFlows", true, false, false, null);
                        this.channel.QueueDeclare("CallBacks", true, false, false, null);
                        this.channel.QueueDeclare("HangupData", true, false, false, null);
                        this.channel.BasicQos(0U, 1, false);
                        this.channelProperties = this.channel.CreateBasicProperties();
                        this.channelProperties.DeliveryMode = (byte)2;
                        this.hangupConsumer = new QueueingBasicConsumer(this.channel);
                        this.hangupConsumerTag = this.channel.BasicConsume("Hangups", false, this.hangupConsumer);
                        SharedClass.Logger.Info("Hangup Queue Consumer Created, ConsumerTag : " + this.hangupConsumerTag);
                        this.callFlowsConsumer = new QueueingBasicConsumer(this.channel);
                        this.callFlowsConsumerTag = this.channel.BasicConsume("CallFlows", false, this.callFlowsConsumer);
                        SharedClass.Logger.Info("CallFlows Queue Consumer Created, ConsumerTag : " + this.callFlowsConsumerTag);
                        this.callBacksConsumer = new QueueingBasicConsumer(this.channel);
                        this.callBacksConsumerTag = this.channel.BasicConsume("CallBacks", false, this.callBacksConsumer);
                        SharedClass.Logger.Info("CallBacks Queue Consumer Created, ConsumerTag : " + this.callBacksConsumerTag);
                        if (!SharedClass.IsHangupProcessInMemory)
                        {
                            this.hangupLazyConsumer = new QueueingBasicConsumer(this.channel);
                            this.hangupLazyConsumerTag = this.channel.BasicConsume("HangupData", false, (IBasicConsumer)this.hangupLazyConsumer);
                            SharedClass.Logger.Info("HangupLazzy Queue Consumer Created, ConsumerTag : " + this.hangupLazyConsumerTag);
                        }
                        SharedClass.Logger.Info("Connected To RabbitMQ Succesfully");
                        this._isConnected = true;
                        this._isConnectSignalInProgress = false;
                        break;
                    }
                    catch (Exception ex1)
                    {
                        SharedClass.Logger.Error("Unable to connect to RabbitMQ, Reason : " + ex1.ToString());
                        if (millisecondsTimeout > 12000)
                            millisecondsTimeout = 3000;
                        else
                            millisecondsTimeout *= 2;
                        try
                        {
                            Thread.Sleep(millisecondsTimeout);
                        }
                        catch (Exception ex2)
                        {
                            SharedClass.Logger.Error("Error In Thread Sleep Block Of RMQConnection Intialization, Reason : " + ex2.ToString());
                        }
                    }
                }
                if (SharedClass.HasStopSignal)
                    SharedClass.Logger.Info("RMQConnector Exited From While Loop With IsPoll False. No More Connection Tries Will Happen");
            }
        }

        public void DeQueueHangups()
        {
            while (!this._isConnected && !SharedClass.HasStopSignal)
            {
                SharedClass.Logger.Info("Waiting for a connect signal");
                Thread.Sleep(5000);
            }
            if (SharedClass.HasStopSignal)
            {
                SharedClass.Logger.Info("Service has stop signal, not dequeuing, terminating DeQueue Thread");
            }
            else
            {
                SharedClass.Logger.Info("Started");                
                byte gatewayId = 0;
                SharedClass.IsHangupConsumerRunning = true;
                BasicDeliverEventArgs deliverEventArgs = null;
                ThreadInterruptedException interruptedException;
                ThreadStateException threadStateException;
                ThreadAbortException threadAbortException;
                while (!SharedClass.HasStopSignal)
                {
                    while (true)
                    {
                        try
                        {
                            deliverEventArgs = null;
                            this.hangupConsumer.Queue.Dequeue(5000, out deliverEventArgs);
                            if (deliverEventArgs != null)
                            {
                                string hangupMessage = Encoding.UTF8.GetString(deliverEventArgs.Body);
                                SharedClass.Logger.Info("HangUpData is : " + hangupMessage);
                                //SharedClass.Logger.Debug(hangupMessage);
                                JObject hangupJson = JObject.Parse(hangupMessage);
                                if (hangupJson.SelectToken("gwid") != null && Byte.TryParse(hangupJson.SelectToken("gwid").ToString(), out gatewayId))
                                {
                                    lock (SharedClass.GatewayMap)
                                    {
                                        if (SharedClass.GatewayMap.ContainsKey((int)gatewayId))
                                            SharedClass.GatewayMap[(int)gatewayId].UpdateConcurrency(true, 1);
                                        else
                                            SharedClass.DumpLogger.Info("HangupForNonExistingGateway : " + hangupMessage);
                                    }
                                }
                                else
                                    SharedClass.DumpLogger.Error("Invalid Hangup Data : " + hangupJson.ToString());
                                this.channel.BasicAck(deliverEventArgs.DeliveryTag, false);
                                deliverEventArgs = null;
                                if (SharedClass.IsHangupProcessInMemory)
                                {
                                    lock (SharedClass.HangupProcessor)
                                        SharedClass.HangupProcessor.EnQueue(hangupJson);
                                }
                                else
                                    this.EnQueue("HangupData", hangupMessage);
                                break;
                            }
                            break;
                        }
                        catch (EndOfStreamException ex1)
                        {
                            SharedClass.Logger.Error("EndOfStream Exception, Reason : " + ex1.Message + ", HasStopSignal : " + SharedClass.HasStopSignal + ", IsConnected : " + this.IsConnected);
                            if (!SharedClass.HasStopSignal)
                            {
                                this.ConnectToServer();
                                while (!this._isConnected && !SharedClass.HasStopSignal)
                                {
                                    try
                                    {
                                        Thread.Sleep(5000);
                                    }
                                    catch (ThreadInterruptedException ex2)
                                    {
                                        interruptedException = ex2;
                                    }
                                    catch (ThreadStateException ex2)
                                    {
                                        threadStateException = ex2;
                                    }
                                    catch (ThreadAbortException ex2)
                                    {
                                        threadAbortException = ex2;
                                    }
                                }
                                if (!SharedClass.HasStopSignal)
                                    SharedClass.Logger.Info("Started DeQueuing Again");
                                else
                                    break;
                            }
                            else
                                break;
                        }
                        catch (Exception ex1)
                        {
                            SharedClass.Logger.Error(ex1.ToString());
                            if (deliverEventArgs != null)
                                this.channel.BasicAck(deliverEventArgs.DeliveryTag, false);
                            if (!SharedClass.HasStopSignal)
                            {
                                try
                                {
                                    Thread.Sleep(5000);
                                }
                                catch (ThreadInterruptedException ex2)
                                {
                                    interruptedException = ex2;
                                }
                                catch (ThreadStateException ex2)
                                {
                                    threadStateException = ex2;
                                }
                                catch (ThreadAbortException ex2)
                                {
                                    threadAbortException = ex2;
                                }
                            }
                            else
                                break;
                        }
                    }
                }
                SharedClass.Logger.Info("Exited From Loop");
                SharedClass.IsHangupConsumerRunning = false;
                if (SharedClass.HasStopSignal)
                    return;
                SharedClass.Notifier.SendSms("Hangup Subscriber Stopped WithOut Service Stop Signal");
            }
        }

        public void DeQueueCallFlows()
        {
            while (!this.IsConnected && !SharedClass.HasStopSignal)
            {
                SharedClass.Logger.Info("Waiting For a Connect Signal");
                Thread.Sleep(5000);
            }
            if (SharedClass.HasStopSignal)
            {
                SharedClass.Logger.Info("Service has stop signal, not dequeuing, terminating DeQueue Thread");
            }
            else
            {
                SharedClass.Logger.Info("Started");                
                SharedClass.IsCallFlowsConsumerRunning = true;
                BasicDeliverEventArgs deliverEventArgs = null;
                string message = null;
                while (!SharedClass.HasStopSignal)
                {
                    while (true) {
                        try
                        {
                            deliverEventArgs = null;
                            this.callFlowsConsumer.Queue.Dequeue(5000, out deliverEventArgs);
                            if (deliverEventArgs != null)
                            {
                                message = System.Text.Encoding.UTF8.GetString(deliverEventArgs.Body);
                                SharedClass.Logger.Info(message);
                                this.CreateCallFlow(message);
                                this.channel.BasicAck(deliverEventArgs.DeliveryTag, false);
                                break;
                            }
                            break;
                        }
                        catch (EndOfStreamException ex)
                        {
                            SharedClass.Logger.Error("EndOfStream Exception, Reason : " + ex.Message + ", HasStopSignal : " + SharedClass.HasStopSignal + ", IsConnected : " + this.IsConnected);
                            if (!SharedClass.HasStopSignal)
                            {
                                this.ConnectToServer();
                                while (!this.IsConnected && !SharedClass.HasStopSignal)
                                    Thread.Sleep(5000);
                                if (!SharedClass.HasStopSignal)
                                    SharedClass.Logger.Info("Started DeQueuing Again");
                                else
                                    break;
                            }
                            else
                                break;
                        }
                        catch (Exception ex)
                        {
                            SharedClass.Logger.Error(ex.ToString());
                            if (deliverEventArgs != null)
                                this.channel.BasicAck(deliverEventArgs.DeliveryTag, false);
                            if (!SharedClass.HasStopSignal)
                                Thread.Sleep(5000);
                            else
                                break;
                        }
                    }
                }
                SharedClass.Logger.Info("Exited From While Loop");
                SharedClass.IsCallFlowsConsumerRunning = false;
                if (SharedClass.HasStopSignal)
                    return;
                lock (SharedClass.Notifier)
                    SharedClass.Notifier.SendSms("CallFlows Thread Stopped with out receiving Service Stop Signal");
            }
        }

        private void DeQueueCallBacks()
        {
            while(!this.IsConnected && !SharedClass.HasStopSignal)
            {
                SharedClass.Logger.Info("Waiting for connect signal");
                Thread.Sleep(5000);
            }
            if(SharedClass.HasStopSignal)
            {
                SharedClass.Logger.Info("Service has stop signal, not dequeuing, terminating DeQueue Thread");
            }
            else
            {
                SharedClass.Logger.Info("Started");
                SharedClass.IsCallBacksConsumerRunning = true;
                BasicDeliverEventArgs deliverEventArgs = null;
                string message = null;
                while (!SharedClass.HasStopSignal)
                {
                    while (true)
                    {
                        try
                        {
                            deliverEventArgs = null;
                            this.callBacksConsumer.Queue.Dequeue(5000, out deliverEventArgs);
                            if (deliverEventArgs != null)
                            {
                                message = System.Text.Encoding.UTF8.GetString(deliverEventArgs.Body);
                                SharedClass.Logger.Info("CallBackData is : " + message );
                                this.CreateCallBacks(message);
                                this.channel.BasicAck(deliverEventArgs.DeliveryTag, false);
                                break;
                            }
                            break;
                        }
                        catch (EndOfStreamException ex)
                        {
                            SharedClass.Logger.Error("EndOfStream Exception, Reason : " + ex.Message + ", HasStopSignal : " + SharedClass.HasStopSignal + ", IsConnected : " + this.IsConnected);
                            if (!SharedClass.HasStopSignal)
                            {
                                this.ConnectToServer();
                                while (!this.IsConnected && !SharedClass.HasStopSignal)
                                    Thread.Sleep(5000);
                                if (!SharedClass.HasStopSignal)
                                    SharedClass.Logger.Info("Started DeQueuing Again");
                                else
                                    break;
                            }
                            else
                                break;
                        }
                        catch (Exception ex)
                        {
                            SharedClass.Logger.Error(ex.ToString());
                            if (deliverEventArgs != null)
                                this.channel.BasicAck(deliverEventArgs.DeliveryTag, false);
                            if (!SharedClass.HasStopSignal)
                                Thread.Sleep(5000);
                            else
                                break;
                        }
                    }
                }
                SharedClass.Logger.Info("Exited From While Loop");
                SharedClass.IsCallBacksConsumerRunning = false;
                if (SharedClass.HasStopSignal)
                    return;
                lock (SharedClass.Notifier)
                    SharedClass.Notifier.SendSms("CallBacks Thread Stopped with out receiving Service Stop Signal");
            }
        }
        private void CreateCallFlow(string callData)
        {
            SharedClass.Logger.Info("CallFlow Obj is :" + callData);
            try
            {
                _callFlowObject = null;
                _callFlowObject = JObject.Parse((JObject.Parse(callData)).SelectToken("smscresponse").ToString());
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallFlow (Message Parsing Failed) : " + callData + ", Reason : " + e.ToString());
            }
            if (_callFlowObject != null)
            {   
                _eventName = _callFlowObject.SelectToken("event").ToString();
                if (_eventName.Equals(Label.POST, StringComparison.CurrentCultureIgnoreCase))
                {
                    _paramObject = JObject.Parse(_callFlowObject.SelectToken("parameters").ToString());
                }

                JToken token;
                if(_callFlowObject.SelectToken("source") != null && _callFlowObject.TryGetValue("source",out token))
                {
                    if (token.ToString().Equals(Environment.STAGING.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.InsertCallFlowStaging();
                    else if (token.ToString().Equals(Environment.PRODUCTION.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.InsertCallFlowProduction();
                    else if (token.ToString().Equals(Environment.STAGINGGROUPCALL.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.InsertGroupCallFlowStaging();
                    else if (token.ToString().Equals(Environment.PRODUCTIONGROUPCALL.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.InsertGroupCallFlowProduction();
                    else
                        InsertCallFlowProduction();
                }
                else
                {
                    InsertCallFlowProduction();
                }
            }
        }


        private void CreateCallBacks(string callData)
        {
            try
            {
                _callBackObject = new JObject();
                _callBackObject = JObject.Parse((JObject.Parse(callData)).SelectToken("smscresponse").ToString());
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallBack (Message Parsing Failed) : " + callData + ", Reason : " + e.ToString());
            }
            if (_callBackObject != null)
            {



                //_eventName = _callBackObject.SelectToken("event").ToString();
                //if (_eventName.Equals(Label.POST, StringComparison.CurrentCultureIgnoreCase))
                //{
                //    _paramObject = JObject.Parse(_callFlowObject.SelectToken("parameters").ToString());
                //}

                SharedClass.Logger.Info("CallBackObj is : " + _callBackObject.ToString());
                JToken token;
                if (_callBackObject.SelectToken("source") != null && _callBackObject.TryGetValue("source", out token))
                {
                    SharedClass.Logger.Info("Environment : " + token.ToString());
                    if (Convert.ToString(token).Equals(Environment.PRODUCTION.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.InsertCallBackProduction();
                    else if (Convert.ToString(token).Equals(Environment.STAGING.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.InsertCallBackStaging();
                    else if (token.ToString().Equals(Environment.STAGINGGROUPCALL.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.InsertGroupCallBacksStaging();
                    else if (token.ToString().Equals(Environment.PRODUCTIONGROUPCALL.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.InsertGroupCallBacksProduction();
                    else
                        this.InsertCallBackProduction();
                }
                else
                {
                    this.InsertCallBackProduction();
                }
            }
        }

        
        private void InsertGroupCallBacksProduction()
        {
            SharedClass.Logger.Info("Creating Call Back For Production :" + _callBackObject.ToString());
            try
            {
                _groupCallBacksSqlCommandProduction.Parameters.Clear();
                _groupCallBacksSqlCommandProduction.Parameters.Add("@CallId", SqlDbType.BigInt).Value = _callBackObject.SelectToken("sequencenumber") == null ? 0 : Convert.ToInt64(_callBackObject.SelectToken("sequencenumber").ToString());
                _groupCallBacksSqlCommandProduction.Parameters.Add("@ConferenceAction", SqlDbType.VarChar, 100).Value = _callBackObject.SelectToken("ConferenceAction") == null ? "" : _callBackObject.SelectToken("ConferenceAction").ToString();
                _groupCallBacksSqlCommandProduction.Parameters.Add("@CallStatus", SqlDbType.VarChar, 200).Value = _callBackObject.SelectToken("callstatus") == null ? "" : _callBackObject.SelectToken("callstatus").ToString();
                if (_callBackObject.SelectToken("ConferenceMemberID") != null && !(_callBackObject.SelectToken("ConferenceMemberID").Equals(System.DBNull.Value)))
                    _groupCallBacksSqlCommandProduction.Parameters.Add("@MemberIdInNode", SqlDbType.BigInt).Value = Convert.ToInt64(_callBackObject.SelectToken("ConferenceMemberID").ToString());
                else
                    _groupCallBacksSqlCommandProduction.Parameters.Add("@MemberIdInNode", SqlDbType.BigInt).Value = 0;

                _groupCallBacksSqlCommandProduction.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                _groupCallBacksSqlCommandProduction.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                if (_groupCallBacksSqlConnectionProduction.State != ConnectionState.Open)
                    _groupCallBacksSqlConnectionProduction.Open();
                _groupCallBacksSqlCommandProduction.ExecuteNonQuery();
                if (!Convert.ToBoolean(_groupCallBacksSqlCommandProduction.Parameters["@Success"].Value))
                    SharedClass.Logger.Error("Exception while inserting CallBack (False From DB) : " + _callBackObject.ToString() + ", Reason : " + _groupCallBacksSqlCommandProduction.Parameters["@Message"].Value);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallBack : " + _callBackObject.ToString() + ", Reason : " + e.ToString());
            }
        }

        private void InsertGroupCallBacksStaging()
        {
            try
            {
                _groupCallBacksSqlCommandStaging.Parameters.Clear();
                _groupCallBacksSqlCommandStaging.Parameters.Add("@CallId", SqlDbType.BigInt).Value = _callBackObject.SelectToken("sequencenumber") == null ? 0 : Convert.ToInt64(_callBackObject.SelectToken("sequencenumber").ToString());
                _groupCallBacksSqlCommandStaging.Parameters.Add("@ConferenceAction", SqlDbType.VarChar, 100).Value = _callBackObject.SelectToken("ConferenceAction") == null ? "" : _callBackObject.SelectToken("ConferenceAction").ToString();
                _groupCallBacksSqlCommandStaging.Parameters.Add("@CallStatus", SqlDbType.VarChar, 200).Value = _callBackObject.SelectToken("callstatus") == null ? "" : _callBackObject.SelectToken("callstatus").ToString();
                if (_callBackObject.SelectToken("ConferenceMemberID") != null && !(_callBackObject.SelectToken("ConferenceMemberID").Equals(System.DBNull.Value)))
                    _groupCallBacksSqlCommandStaging.Parameters.Add("@MemberIdInNode", SqlDbType.BigInt).Value = Convert.ToInt64(_callBackObject.SelectToken("ConferenceMemberID").ToString());

                _groupCallBacksSqlCommandStaging.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                _groupCallBacksSqlCommandStaging.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                if (_groupCallBacksSqlConnectionStaging.State != ConnectionState.Open)
                    _groupCallBacksSqlConnectionStaging.Open();
                _groupCallBacksSqlCommandStaging.ExecuteNonQuery();
                if (!Convert.ToBoolean(_groupCallBacksSqlCommandStaging.Parameters["@Success"].Value))
                    SharedClass.Logger.Error("Exception while inserting CallBack (False From DB) : " + _callBackObject.ToString() + ", Reason : " + _groupCallBacksSqlCommandStaging.Parameters["@Message"].Value);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallBack : " + _callBackObject.ToString() + ", Reason : " + e.ToString());
            }
        }

        private void InsertCallBackProduction()
        {
            try
            {
                _callBacksSqlCommandProduction.Parameters.Clear();
                _callBacksSqlCommandProduction.Parameters.Add("@SequenceNumber", SqlDbType.BigInt).Value = _callBackObject.SelectToken("sequencenumber") == null ? 0 : Convert.ToInt64(_callBackObject.SelectToken("sequencenumber").ToString());
                _callBacksSqlCommandProduction.Parameters.Add("@Event", SqlDbType.VarChar, 200).Value = _callBackObject.SelectToken("event") == null ? "" : Convert.ToString(_callBackObject.SelectToken("event"));
                _callBacksSqlCommandProduction.Parameters.Add("@RecordURL", SqlDbType.VarChar, 500).Value = _callBackObject.SelectToken("recordurl") == null ? "" : Convert.ToString(_callBackObject.SelectToken("recordurl"));
                _callBacksSqlCommandProduction.Parameters.Add("@From", SqlDbType.VarChar, 100).Value = _callBackObject.SelectToken("from") == null ? "" : Convert.ToString(_callBackObject.SelectToken("from"));
                _callBacksSqlCommandProduction.Parameters.Add("@To", SqlDbType.VarChar, 100).Value = _callBackObject.SelectToken("to") == null ? "" : Convert.ToString(_callBackObject.SelectToken("to"));
                _callBacksSqlCommandProduction.Parameters.Add("@Digits", SqlDbType.VarChar, 50).Value = _callBackObject.SelectToken("digits") == null ? "" : Convert.ToString(_callBackObject.SelectToken("digits"));
                _callBacksSqlCommandProduction.Parameters.Add("@Success",SqlDbType.Bit).Direction = ParameterDirection.Output;
                _callBacksSqlCommandProduction.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;

                if(_callBacksSqlConnectionProduction.State != ConnectionState.Open)
                    _callBacksSqlConnectionProduction.Open();
                _callBacksSqlCommandProduction.ExecuteNonQuery();

                if (!Convert.ToBoolean(_callBacksSqlCommandProduction.Parameters["@Success"].Value))
                    SharedClass.Logger.Error("Exception while inserting CallBack (False From DB) : " + _callBackObject.ToString() + ", Reason : " + _callBacksSqlCommandProduction.Parameters["@Message"].Value);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallBack : " + _callBackObject.ToString() + ", Reason : " + e.ToString());
            }
        }

        private void InsertCallBackStaging()
        {
            try
            {
                _callBacksSqlCommandStaging.Parameters.Clear();
                _callBacksSqlCommandStaging.Parameters.Add("@SequenceNumber", SqlDbType.BigInt).Value = _callBackObject.SelectToken("sequencenumber") == null ? 0 : Convert.ToInt64(_callBackObject.SelectToken("sequencenumber").ToString());
                _callBacksSqlCommandStaging.Parameters.Add("@Event", SqlDbType.VarChar, 200).Value = _callBackObject.SelectToken("event") == null ? "" : Convert.ToString(_callBackObject.SelectToken("event"));
                _callBacksSqlCommandStaging.Parameters.Add("@RecordURL", SqlDbType.VarChar, 500).Value = _callBackObject.SelectToken("recordurl") == null ? "" : Convert.ToString(_callBackObject.SelectToken("recordurl"));
                _callBacksSqlCommandStaging.Parameters.Add("@From", SqlDbType.VarChar, 100).Value = _callBackObject.SelectToken("from") == null ? "" : Convert.ToString(_callBackObject.SelectToken("from"));
                _callBacksSqlCommandStaging.Parameters.Add("@To", SqlDbType.VarChar, 100).Value = _callBackObject.SelectToken("to") == null ? "" : Convert.ToString(_callBackObject.SelectToken("to"));
                _callBacksSqlCommandStaging.Parameters.Add("@Digits", SqlDbType.VarChar, 50).Value = _callBackObject.SelectToken("digits") == null ? "" : Convert.ToString(_callBackObject.SelectToken("digits"));
                _callBacksSqlCommandStaging.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                _callBacksSqlCommandStaging.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;

                if (_callBacksSqlConnectionStaging.State != ConnectionState.Open)
                    _callBacksSqlConnectionStaging.Open();
                _callBacksSqlCommandStaging.ExecuteNonQuery();

                if (!Convert.ToBoolean(_callBacksSqlCommandStaging.Parameters["@Success"].Value))
                    SharedClass.Logger.Error("Exception while inserting CallBack (False From DB) : " + _callBackObject.ToString() + ", Reason : " + _callBacksSqlCommandStaging.Parameters["@Message"].Value);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallBack : " + _callBackObject.ToString() + ", Reason : " + e.ToString());
            }
        }

        private void InsertCallFlowStaging()
        {
            try
            {
                _callFlowsSqlCommandStaging.Parameters.Clear();
                _callFlowsSqlCommandStaging.Parameters.Add("@CallId", SqlDbType.BigInt).Value = Convert.ToInt64(_callFlowObject.SelectToken("sequencenumber").ToString());
                _callFlowsSqlCommandStaging.Parameters.Add("@Event", SqlDbType.VarChar, 10).Value = _callFlowObject.SelectToken("event").ToString();
                _callFlowsSqlCommandStaging.Parameters.Add("@RingTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("ringtime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("ringtime").ToString());
                _callFlowsSqlCommandStaging.Parameters.Add("@AnswerTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("starttime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("starttime").ToString());
                _callFlowsSqlCommandStaging.Parameters.Add("@EndTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("endtime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("endtime").ToString());
                _callFlowsSqlCommandStaging.Parameters.Add("@EndBy", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("hangupdisposition") == null ? "" : _callFlowObject.SelectToken("hangupdisposition").ToString();
                _callFlowsSqlCommandStaging.Parameters.Add("@Caller", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("caller") == null ? "" : _callFlowObject.SelectToken("caller").ToString();
                _callFlowsSqlCommandStaging.Parameters.Add("@Callee", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("callee") == null ? "" : _callFlowObject.SelectToken("callee").ToString();
                _callFlowsSqlCommandStaging.Parameters.Add("@Digits", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("digits") == null ? "" : _callFlowObject.SelectToken("digits").ToString();
                _callFlowsSqlCommandStaging.Parameters.Add("@RecordingUrl", SqlDbType.VarChar, 200).Value = _callFlowObject.SelectToken("recordurl") == null ? "" : _callFlowObject.SelectToken("recordurl").ToString();
                _callFlowsSqlCommandStaging.Parameters.Add("@CallStatus", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("callstatus").ToString();
                _callFlowsSqlCommandStaging.Parameters.Add("@PostingUrl", SqlDbType.VarChar, 200).Value = _callFlowObject.SelectToken("postingurl").ToString();
                _callFlowsSqlCommandStaging.Parameters.Add("@PostingData", SqlDbType.VarChar, -1);
                _callFlowsSqlCommandStaging.Parameters.Add("@HttpMethod", SqlDbType.TinyInt);
                _callFlowsSqlCommandStaging.Parameters.Add("@ResponseData", SqlDbType.VarChar, -1);
                _callFlowsSqlCommandStaging.Parameters.Add("@TimeTaken", SqlDbType.Int);
                _callFlowsSqlCommandStaging.Parameters.Add("@StatusCode", SqlDbType.Int);
                _callFlowsSqlCommandStaging.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                _callFlowsSqlCommandStaging.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
    
                if (_callFlowsSqlConnectionStaging.State != ConnectionState.Open)
                    _callFlowsSqlConnectionStaging.Open();
                _callFlowsSqlCommandStaging.ExecuteNonQuery();
                if (!Convert.ToBoolean(_callFlowsSqlCommandStaging.Parameters["@Success"].Value))
                    SharedClass.Logger.Error("Exception whil processing CallFlow (False From DB) : " + _callFlowObject.ToString() + ", Reason : " + _callFlowsSqlCommandStaging.Parameters["@Message"].Value);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallFlow : " + _callFlowObject.ToString() + ", Reason : " + e.ToString());
            }
        }

        private void InsertCallFlowProduction()
        {
            try
            {
                _callFlowsSqlCommand.Parameters.Clear();
                
                _callFlowsSqlCommand.Parameters.Add("@CallId", SqlDbType.BigInt).Value = Convert.ToInt64(_callFlowObject.SelectToken("sequencenumber").ToString());
                _callFlowsSqlCommand.Parameters.Add("@Event", SqlDbType.VarChar, 10).Value = _callFlowObject.SelectToken("event").ToString();
                _callFlowsSqlCommand.Parameters.Add("@RingTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("ringtime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("ringtime").ToString());
                _callFlowsSqlCommand.Parameters.Add("@AnswerTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("starttime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("starttime").ToString());
                _callFlowsSqlCommand.Parameters.Add("@EndTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("endtime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("endtime").ToString());
                _callFlowsSqlCommand.Parameters.Add("@EndBy", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("hangupdisposition") == null ? "" : _callFlowObject.SelectToken("hangupdisposition").ToString();
                _callFlowsSqlCommand.Parameters.Add("@Caller", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("caller") == null ? "" : _callFlowObject.SelectToken("caller").ToString();
                _callFlowsSqlCommand.Parameters.Add("@Callee", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("callee") == null ? "" : _callFlowObject.SelectToken("callee").ToString();
                _callFlowsSqlCommand.Parameters.Add("@Digits", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("digits") == null ? "" : _callFlowObject.SelectToken("digits").ToString();
                _callFlowsSqlCommand.Parameters.Add("@RecordingUrl", SqlDbType.VarChar, 200).Value = _callFlowObject.SelectToken("recordurl") == null ? "" : _callFlowObject.SelectToken("recordurl").ToString();
                _callFlowsSqlCommand.Parameters.Add("@CallStatus", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("callstatus").ToString();
                _callFlowsSqlCommand.Parameters.Add("@PostingUrl", SqlDbType.VarChar, 200).Value = _callFlowObject.SelectToken("postingurl").ToString();
                _callFlowsSqlCommand.Parameters.Add("@PostingData", SqlDbType.VarChar, -1);
                _callFlowsSqlCommand.Parameters.Add("@HttpMethod", SqlDbType.TinyInt);
                _callFlowsSqlCommand.Parameters.Add("@ResponseData", SqlDbType.VarChar, -1);
                _callFlowsSqlCommand.Parameters.Add("@TimeTaken", SqlDbType.Int);
                _callFlowsSqlCommand.Parameters.Add("@StatusCode", SqlDbType.Int);
                _callFlowsSqlCommand.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                _callFlowsSqlCommand.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                if (_callFlowsSqlConnection.State != ConnectionState.Open)
                    _callFlowsSqlConnection.Open();
                _callFlowsSqlCommand.ExecuteNonQuery();
                if (!Convert.ToBoolean(_callFlowsSqlCommand.Parameters["@Success"].Value))
                    SharedClass.Logger.Error("Exception whil processing CallFlow (False From DB) : " + _callFlowObject.ToString() + ", Reason : " + _callFlowsSqlCommand.Parameters["@Message"].Value);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallFlow : " + _callFlowObject.ToString() + ", Reason : " + e.ToString());
            }
        }

        private void InsertGroupCallFlowProduction()
        {
            try
            {
                _groupCallFlowsSqlCommandProduction.Parameters.Clear();
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@CallId", SqlDbType.BigInt).Value = Convert.ToInt64(_callFlowObject.SelectToken("sequencenumber").ToString());
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@Event", SqlDbType.VarChar, 10).Value = _callFlowObject.SelectToken("event").ToString();
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@RingTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("ringtime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("ringtime").ToString());
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@AnswerTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("starttime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("starttime").ToString());
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@EndTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("endtime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("endtime").ToString());
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@EndBy", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("hangupdisposition") == null ? "" : _callFlowObject.SelectToken("hangupdisposition").ToString();
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@Caller", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("caller") == null ? "" : _callFlowObject.SelectToken("caller").ToString();
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@Callee", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("callee") == null ? "" : _callFlowObject.SelectToken("callee").ToString();
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@Digits", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("digits") == null ? "" : _callFlowObject.SelectToken("digits").ToString();
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@RecordingUrl", SqlDbType.VarChar, 200).Value = _callFlowObject.SelectToken("recordurl") == null ? "" : _callFlowObject.SelectToken("recordurl").ToString();
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@CallStatus", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("callstatus").ToString();
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@PostingUrl", SqlDbType.VarChar, 200).Value = _callFlowObject.SelectToken("postingurl").ToString();
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@PostingData", SqlDbType.VarChar, -1);
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@HttpMethod", SqlDbType.TinyInt);
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@ResponseData", SqlDbType.VarChar, -1);
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@TimeTaken", SqlDbType.Int);
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@StatusCode", SqlDbType.Int);
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                _groupCallFlowsSqlCommandProduction.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                if (_groupCallFlowsSqlConnectionProduction.State != ConnectionState.Open)
                    _groupCallFlowsSqlConnectionProduction.Open();
                _groupCallFlowsSqlCommandProduction.ExecuteNonQuery();
                if (!Convert.ToBoolean(_groupCallFlowsSqlCommandProduction.Parameters["@Success"].Value))
                    SharedClass.Logger.Error("Exception whil processing CallFlow (False From DB) : " + _callFlowObject.ToString() + ", Reason : " + _groupCallFlowsSqlCommandProduction.Parameters["@Message"].Value);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallFlow : " + _callFlowObject.ToString() + ", Reason : " + e.ToString());
            }
        }

        private void InsertGroupCallFlowStaging()
        {
            try
            {
                _groupCallFlowsSqlCommandStaging.Parameters.Clear();
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@CallId", SqlDbType.BigInt).Value = Convert.ToInt64(_callFlowObject.SelectToken("sequencenumber").ToString());
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@Event", SqlDbType.VarChar, 10).Value = _callFlowObject.SelectToken("event").ToString();
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@RingTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("ringtime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("ringtime").ToString());
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@AnswerTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("starttime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("starttime").ToString());
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@EndTimeStamp", SqlDbType.BigInt).Value = _callFlowObject.SelectToken("endtime") == null ? 0 : Convert.ToInt64(_callFlowObject.SelectToken("endtime").ToString());
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@EndBy", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("hangupdisposition") == null ? "" : _callFlowObject.SelectToken("hangupdisposition").ToString();
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@Caller", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("caller") == null ? "" : _callFlowObject.SelectToken("caller").ToString();
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@Callee", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("callee") == null ? "" : _callFlowObject.SelectToken("callee").ToString();
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@Digits", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("digits") == null ? "" : _callFlowObject.SelectToken("digits").ToString();
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@RecordingUrl", SqlDbType.VarChar, 200).Value = _callFlowObject.SelectToken("recordurl") == null ? "" : _callFlowObject.SelectToken("recordurl").ToString();
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@CallStatus", SqlDbType.VarChar, 20).Value = _callFlowObject.SelectToken("callstatus").ToString();
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@PostingUrl", SqlDbType.VarChar, 200).Value = _callFlowObject.SelectToken("postingurl").ToString();
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@PostingData", SqlDbType.VarChar, -1);
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@HttpMethod", SqlDbType.TinyInt);
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@ResponseData", SqlDbType.VarChar, -1);
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@TimeTaken", SqlDbType.Int);
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@StatusCode", SqlDbType.Int);
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                _groupCallFlowsSqlCommandStaging.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;

                if (_groupCallFlowsSqlConnectionStaging.State != ConnectionState.Open)
                    _groupCallFlowsSqlConnectionStaging.Open();
                _groupCallFlowsSqlCommandStaging.ExecuteNonQuery();
                if (!Convert.ToBoolean(_groupCallFlowsSqlCommandStaging.Parameters["@Success"].Value))
                    SharedClass.Logger.Error("Exception whil processing CallFlow (False From DB) : " + _callFlowObject.ToString() + ", Reason : " + _groupCallFlowsSqlCommandStaging.Parameters["@Message"].Value);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallFlow : " + _callFlowObject.ToString() + ", Reason : " + e.ToString());
            }
        }



        private void EnQueue(string queueName, string message)
        {
        retryLabel:
            try
            {
                this.channel.BasicPublish("", queueName, this.channelProperties, Encoding.UTF8.GetBytes(message));
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error(ex.ToString());
                if (!this.connection.IsOpen)
                    this.ConnectToServer();
                while (!this._isConnected && !SharedClass.HasStopSignal)
                    Thread.Sleep(5000);
                if (!SharedClass.HasStopSignal)
                    goto retryLabel;
            }
        }
        public string Host { get { return this._host; } set { this._host = value; } }
        public int Port { get { return this._port; } set { this._port = value; } }
        public string User { get { return this._user; } set { this._user = value; } }
        public string Password { get { return this._password; } set { this._password = value; } }
        public bool IsConnected { get { return this._isConnected; } set { this._isConnected = value; } }
        public bool IsConnectSignalInProgress { get { return this._isConnectSignalInProgress; } set { this._isConnectSignalInProgress = value; } } 
    }
}
