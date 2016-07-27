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
        private string _user = string.Empty;
        private string _password = string.Empty;
        private bool _isConnected = false;
        private bool _isConnectSignalInProgress = false;
        private Thread _connectThread = null;
        private Thread _hangupDeQueueThread = null;
        private Thread _callFlowsDeQueueThread = null;
        public ConnectionFactory connectionFactory = null;
        public IConnection connection = null;
        public IModel channel = null;
        public QueueingBasicConsumer hangupConsumer = null;
        public QueueingBasicConsumer callFlowsConsumer = null;
        public QueueingBasicConsumer hangupLazyConsumer = null;
        public string hangupConsumerTag = null;
        public string callFlowsConsumerTag = null;
        public string hangupLazyConsumerTag = null;
        public IBasicProperties channelProperties = null;
        
        #region CALLFLOW_VARIABLES
        
        private JObject _callFlowObject = null;
        private SqlConnection _callFlowsSqlConnection = null;
        private SqlConnection _callFlowsSqlConnectionStaging = null;
        private SqlCommand _callFlowsSqlCommandStaging = null;
        private SqlCommand _callFlowsSqlCommand = null;
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
            _callFlowsSqlConnectionStaging = new SqlConnection(SharedClass.GetConnectionString(Environment.STAGING));
            _callFlowsSqlCommand = new SqlCommand("VC_Create_CallFlow", _callFlowsSqlConnection);
            _callFlowsSqlCommandStaging = new SqlCommand("VC_Create_CallFlow", _callFlowsSqlConnectionStaging);
            this._connectThread = new Thread(new ThreadStart(this.ConnectToServer));
            this._connectThread.Name = "RMQConnector";
            this._connectThread.Start();
            this._hangupDeQueueThread = new Thread(new ThreadStart(this.DeQueueHangups));
            this._hangupDeQueueThread.Name = "HangupListener";
            this._hangupDeQueueThread.Start();
            this._callFlowsDeQueueThread = new Thread(new ThreadStart(this.DeQueueCallFlows));
            this._callFlowsDeQueueThread.Name = "CallFlows";
            this._callFlowsDeQueueThread.Start();
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
                                SharedClass.Logger.Debug(hangupMessage);
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
        private void CreateCallFlow(string callData)
        {
            try
            {
                _callFlowObject = null;
                _callFlowObject = JObject.Parse(callData);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Exception whil processing CallFlow (Message Parsing Failed) : " + callData + ", Reason : " + e.ToString());
            }
            if (_callFlowObject != null)
            {   
                _eventName = _callFlowObject.SelectToken("event").ToString();
                if (_eventName.Equals(SharedClass.POST, StringComparison.CurrentCultureIgnoreCase))
                {
                    _paramObject = JObject.Parse(_callFlowObject.SelectToken("parameters").ToString());
                }
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
                    if(!Convert.ToBoolean(_callFlowsSqlCommand.Parameters["@Success"].Value))
                        SharedClass.Logger.Error("Exception whil processing CallFlow (False From DB) : " + callData + ", Reason : " + _callFlowsSqlCommand.Parameters["@Message"].Value);
                }
                catch (Exception e)
                {
                    SharedClass.Logger.Error("Exception whil processing CallFlow : " + callData + ", Reason : " + e.ToString());
                }
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
