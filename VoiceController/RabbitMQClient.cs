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

namespace VoiceController
{
    public class RabbitMQClient
    {
        private string host = null;
        private int port = 0;
        private string user = null;
        private string password = null;
        private bool isConnected = false;
        private bool isConnectSignalInProgress = false;
        private Thread connectThread = null;
        private Thread hangupDeQueueThread = null;
        private Thread callFlowsDeQueueThread = null;
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

        public string Host
        {
            get
            {
                return this.host;
            }
            set
            {
                this.host = value;
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

        public string User
        {
            get
            {
                return this.user;
            }
            set
            {
                this.user = value;
            }
        }

        public string Password
        {
            get
            {
                return this.password;
            }
            set
            {
                this.password = value;
            }
        }

        public bool IsConnected
        {
            get
            {
                return this.isConnected;
            }
            set
            {
                this.isConnected = value;
            }
        }

        public bool IsConnectSignalInProgress
        {
            get
            {
                return this.isConnectSignalInProgress;
            }
            set
            {
                this.isConnectSignalInProgress = value;
            }
        }

        public void Start()
        {
            this.connectThread = new Thread(new ThreadStart(this.ConnectToServer));
            this.connectThread.Name = "RMQConnector";
            this.connectThread.Start();
            this.hangupDeQueueThread = new Thread(new ThreadStart(this.DeQueueHangups));
            this.hangupDeQueueThread.Name = "HangupListener";
            this.hangupDeQueueThread.Start();
            this.callFlowsDeQueueThread = new Thread(new ThreadStart(this.DeQueueCallFlows));
            this.callFlowsDeQueueThread.Name = "CallFlows";
            this.callFlowsDeQueueThread.Start();
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
                if (this.hangupDeQueueThread.ThreadState == ThreadState.WaitSleepJoin)
                    this.hangupDeQueueThread.Interrupt();
                if (this.callFlowsDeQueueThread.ThreadState == ThreadState.WaitSleepJoin)
                    this.callFlowsDeQueueThread.Interrupt();                    
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
            if (this.isConnectSignalInProgress)
            {
                SharedClass.Logger.Info("A connect signal already in progress. Termination connect signal");
            }
            else
            {
                this.isConnectSignalInProgress = true;
                this.isConnected = false;
                SharedClass.Logger.Info("Initializing RabbitMQ Connection, Host " + this.host + ", Port : " + this.port.ToString() + ", User : " + this.user + ", Password : " + this.password);
                while (!SharedClass.HasStopSignal)
                {
                    try
                    {
                        this.connectionFactory = new ConnectionFactory();
                        this.connectionFactory.HostName = this.host;
                        this.connectionFactory.Port = this.port;
                        this.connectionFactory.UserName = this.user;
                        this.connectionFactory.Password = this.password;
                        this.connection = this.connectionFactory.CreateConnection();
                        this.channel = this.connection.CreateModel();
                        this.channel.QueueDeclare("Hangups", true, false, false, null);
                        this.channel.QueueDeclare("CallFlows", true, false, false, null);
                        this.channel.QueueDeclare("HangupData", true, false, false, null);
                        this.channel.BasicQos(0U, (ushort)1, false);
                        this.channelProperties = this.channel.CreateBasicProperties();
                        this.channelProperties.DeliveryMode = (byte)2;
                        this.hangupConsumer = new QueueingBasicConsumer(this.channel);
                        this.hangupConsumerTag = this.channel.BasicConsume("Hangups", false, this.hangupConsumer);
                        SharedClass.Logger.Info("Hangup Queue Consumer Created, ConsumerTag : " + this.hangupConsumerTag);
                        this.callFlowsConsumer = new QueueingBasicConsumer(this.channel);
                        this.callFlowsConsumerTag = this.channel.BasicConsume("CallFlows", false, this.callFlowsConsumer);
                        SharedClass.Logger.Info("CallFlows Queue Consumer Created, ConsumerTag : " + this.callFlowsConsumer);
                        if (!SharedClass.IsHangupProcessInMemory)
                        {
                            this.hangupLazyConsumer = new QueueingBasicConsumer(this.channel);
                            this.hangupLazyConsumerTag = this.channel.BasicConsume("HangupData", false, (IBasicConsumer)this.hangupLazyConsumer);
                            SharedClass.Logger.Info("HangupLazzy Queue Consumer Created, ConsumerTag : " + (object)this.hangupLazyConsumer);
                        }
                        SharedClass.Logger.Info("Connected To RabbitMQ Succesfully");
                        this.isConnected = true;
                        this.isConnectSignalInProgress = false;
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
            while (!this.isConnected && !SharedClass.HasStopSignal)
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
                ushort gatewayId = 0;
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
                                JObject hangupJson = JObject.Parse(hangupMessage);
                                if (hangupJson.SelectToken("gwid") != null && ushort.TryParse(hangupJson.SelectToken("gwid").ToString(), out gatewayId))
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
                                while (!this.isConnected && !SharedClass.HasStopSignal)
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
                string str = null;
                SharedClass.IsCallFlowsConsumerRunning = true;
                BasicDeliverEventArgs deliverEventArgs = null;
                while (!SharedClass.HasStopSignal)
                {
                    while (true)
                    {
                        try
                        {
                            deliverEventArgs = null;
                            this.callFlowsConsumer.Queue.Dequeue(5000, out deliverEventArgs);
                            if (deliverEventArgs != null)
                            {
                                str = Encoding.UTF8.GetString(deliverEventArgs.Body);
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
                while (!this.isConnected && !SharedClass.HasStopSignal)
                    Thread.Sleep(5000);
                if (!SharedClass.HasStopSignal)
                    goto retryLabel;
            }
        }
    }
}
