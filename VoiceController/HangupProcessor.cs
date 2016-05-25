using Newtonsoft.Json.Linq;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;
using System.Threading;
using System.Xml;

namespace VoiceController
{
    public class HangupProcessor
    {
        private Queue<JObject> hangupQueue = new Queue<JObject>();
        private bool isIamRunning = false;
        private SqlConnection sqlCon = null;
        private SqlCommand sqlCmd = null;
        private BasicDeliverEventArgs eventArgs = null;
        private string message = null;
        private JObject hangupData = null;
        private XmlDocument xmlDoc = null;
        private XmlElement rootElement = null;
        private ushort retryAttempt = 0;

        public bool IsRunning
        {
            get
            {
                return this.isIamRunning;
            }
        }

        public void Start()
        {
            this.sqlCon = new SqlConnection(SharedClass.ConnectionString);
            this.sqlCmd = new SqlCommand("VC_Call_Hangup_Processor", this.sqlCon);
            this.xmlDoc = new XmlDocument();
            this.rootElement = this.xmlDoc.CreateElement("Call");
            this.xmlDoc.AppendChild((XmlNode)this.rootElement);
            Thread thread = !SharedClass.IsHangupProcessInMemory ? new Thread(new ThreadStart(this.StartRMQProcessing)) : new Thread(new ThreadStart(this.StartInMemoryProcessing));
            thread.Name = "HangupDataProcessor";
            thread.Start();
        }

        public void Stop()
        {
            while (this.IsRunning)
            {
                SharedClass.Logger.Info("HangupProcesser Not Yet Stopped");
                Thread.Sleep(1000);
            }
            if (!SharedClass.IsHangupProcessInMemory || this.QueueCount() <= 0)
                return;
            //SharedClass.Logger.Info("Dumping " + (object)this.QueueCount() + " Hangup Objects Into DumpLog");
            //while (this.QueueCount() > 0)
            //    SharedClass.Logger.Info((object)("HangupObject : " + this.DeQueue().ToString()));
            //SharedClass.Logger.Info((object)"Dumping Done");

            //This process has to deal with Serialization of Queue.
        }

        private void StartInMemoryProcessing()
        {
            SharedClass.Logger.Info("Started");
            this.isIamRunning = true;
            while (!SharedClass.HasStopSignal)
            {
                if (this.QueueCount() > 0)
                {
                    this.hangupData = this.DeQueue();
                    if (this.hangupData != null)
                    {
                        this.retryAttempt = 0;
                        while (true)
                        {
                            try
                            {
                                this.rootElement.Attributes.RemoveAll();
                                foreach (JProperty jproperty in this.hangupData.Properties())
                                    this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                                if (this.sqlCon.State != ConnectionState.Open)
                                    this.sqlCon.Open();
                                this.sqlCmd.Parameters.Clear();
                                this.sqlCmd.CommandType = CommandType.StoredProcedure;
                                this.sqlCmd.Parameters.Add("@Data", SqlDbType.VarChar, this.xmlDoc.InnerXml.Length).Value = this.xmlDoc.InnerXml;
                                this.sqlCmd.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                                this.sqlCmd.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                                this.sqlCmd.ExecuteNonQuery();
                                this.hangupData = null;
                                break;
                            }
                            catch (Exception ex)
                            {
                                SharedClass.Logger.Error("Error Processing Hangup Data, Reason : " + ex.ToString());
                                ++this.retryAttempt;
                                if (this.retryAttempt < 3)
                                    SharedClass.Logger.Info("Retry Attempt : " + this.retryAttempt);
                                else
                                {
                                    SharedClass.Logger.Info("Max Retry Attempts Reached. Ignoring Object : " + this.hangupData.ToString());
                                    break;
                                }
                            }
                        }
                    }
                }
                else
                    Thread.Sleep(3000);
            }
            this.isIamRunning = false;
        }

        public void StartRMQProcessing()
        {
            while (!SharedClass.RabbitMQClient.IsConnected && !SharedClass.HasStopSignal)
            {
                SharedClass.Logger.Info("Waiting For Connect Signal");
                Thread.Sleep(5000);
            }
            SharedClass.Logger.Info("Started DeQueuing");
            this.isIamRunning = true;
            while (!SharedClass.HasStopSignal)
            {
                while (true)
                {
                    try
                    {
                        this.eventArgs = null;
                        while (SharedClass.RabbitMQClient == null)
                        {
                            SharedClass.Logger.Info("RMQClient Is Nothing");
                            Thread.Sleep(10000);
                        }
                        while (SharedClass.RabbitMQClient.hangupLazyConsumer == null)
                        {
                            SharedClass.Logger.Info("HangupDataConsumer Is Nothing");
                            Thread.Sleep(10000);
                        }
                        while (SharedClass.RabbitMQClient.hangupLazyConsumer.Queue == null)
                        {
                            SharedClass.Logger.Info("HangupDataConsumer Queue Is Nothing");
                            Thread.Sleep(10000);
                        }
                        SharedClass.RabbitMQClient.hangupLazyConsumer.Queue.Dequeue(5000, out this.eventArgs);
                        if (this.eventArgs != null)
                        {
                            this.retryAttempt = 0;
                            this.message = Encoding.UTF8.GetString(this.eventArgs.Body);
                            this.hangupData = JObject.Parse(this.message);
                            while (true)
                            {
                                try
                                {
                                    this.rootElement.Attributes.RemoveAll();
                                    foreach (JProperty jproperty in ((JObject)this.hangupData.SelectToken("smscresponse")).Properties())
                                        this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                                    if (this.sqlCon.State != ConnectionState.Open)
                                        this.sqlCon.Open();
                                    this.sqlCmd.Parameters.Clear();
                                    this.sqlCmd.CommandType = CommandType.StoredProcedure;
                                    SharedClass.Logger.Info("Data to db : " + this.xmlDoc.InnerXml);
                                    SqlParameterCollection parameters = this.sqlCmd.Parameters;
                                    parameters.Add("@Data", SqlDbType.VarChar, -1).Value = this.xmlDoc.InnerXml;
                                    parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                                    parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                                    this.sqlCmd.ExecuteNonQuery();
                                    this.hangupData = null;
                                    break;
                                }
                                catch (Exception ex)
                                {
                                    SharedClass.Logger.Error("Error Processing HangupObj : " + ex.ToString());
                                    ++this.retryAttempt;
                                    if ((int)this.retryAttempt < 3)
                                    {
                                        SharedClass.Logger.Info("Retry Attempt : " + this.retryAttempt);
                                    }
                                    else
                                    {
                                        SharedClass.DumpLogger.Info("Max Retry Attempts Reached. Ignoring Object : " + this.hangupData.ToString());
                                        break;
                                    }
                                }
                            }
                            SharedClass.RabbitMQClient.channel.BasicAck(this.eventArgs.DeliveryTag, false);
                            this.eventArgs = null;
                            break;
                        }
                        break;
                    }
                    catch (EndOfStreamException ex)
                    {
                        SharedClass.Logger.Error("EndOfStream Exception, Reason : " + ex.Message + ", HasStopSignal : " + SharedClass.HasStopSignal + ", IsConnected : " + SharedClass.RabbitMQClient.IsConnected);
                        if (!SharedClass.HasStopSignal)
                        {
                            SharedClass.RabbitMQClient.ConnectToServer();
                            while (!SharedClass.RabbitMQClient.IsConnected && !SharedClass.HasStopSignal)
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
                        if (this.eventArgs != null)
                            SharedClass.RabbitMQClient.channel.BasicAck(this.eventArgs.DeliveryTag, false);
                        if (!SharedClass.HasStopSignal)
                            Thread.Sleep(5000);
                        else
                            break;
                    }
                    finally
                    {
                    }
                }
            }
            this.isIamRunning = false;
            SharedClass.Logger.Info("Exited From While Loop");
        }

        public bool EnQueue(JObject data)
        {
            bool flag = false;
            try
            {
                lock (this.hangupQueue)
                {
                    this.hangupQueue.Enqueue(data);
                    flag = true;
                }
            }
            catch (Exception ex)
            {
                flag = false;
                SharedClass.Logger.Error("Error EnQueuing HangupData Into InMemory Queue, Reason : " + ex.ToString());
            }
            return flag;
        }

        private int QueueCount()
        {
            int queueCount = 0;
            lock (this.hangupQueue)
                queueCount = this.hangupQueue.Count;
            return queueCount;
        }

        private JObject DeQueue()
        {
            JObject jobject = null;
            try
            {
                lock (this.hangupQueue)
                    jobject = this.hangupQueue.Dequeue();
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error DeQueuing Hangup Data From In Memory Queue, Reason : " + ex.ToString());
            }
            return jobject;
        }
    }
}
