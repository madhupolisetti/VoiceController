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
        private SqlConnection sqlConStaging = (SqlConnection)null;
        private SqlCommand sqlCmdStaging = (SqlCommand)null;
        private SqlCommand sqlCmd = null;
        private SqlConnection _groupCallSqlConStaging = (SqlConnection)null;
        private SqlConnection _groupCallSqlConProduction = (SqlConnection)null;
        private SqlCommand _groupCallSqlCmdStaging = (SqlCommand)null;
        private SqlCommand _groupCallSqlCmdProduction = (SqlCommand)null;
        private BasicDeliverEventArgs eventArgs = null;
        private string message = null;
        private JObject hangupData = null;
        private XmlDocument xmlDoc = null;
        private XmlElement rootElement = null;
        private byte retryAttempt = 0;
        private byte _processedCount = 0;

        public bool IsRunning
        {
            get
            {
                return this.isIamRunning;
            }
        }

        public void Start(byte index)
        {
            this.sqlCon = new SqlConnection(SharedClass.GetConnectionString(Environment.PRODUCTION));
            this.sqlCmd = new SqlCommand("VC_Call_Hangup_Processor", this.sqlCon);
            this._groupCallSqlConProduction = new SqlConnection(SharedClass.GetConnectionString(Environment.PRODUCTION));
            this._groupCallSqlCmdProduction = new SqlCommand("VC_GroupCall_Hangup_Processor", this._groupCallSqlConProduction);

            if(SharedClass.PollStaging)
            {
                this.sqlConStaging = new SqlConnection(SharedClass.GetConnectionString(Environment.STAGING));
                this.sqlCmdStaging = new SqlCommand("VC_Call_Hangup_Processor", this.sqlConStaging);

                this._groupCallSqlConStaging = new SqlConnection(SharedClass.GetConnectionString(Environment.STAGING));
                this._groupCallSqlCmdStaging = new SqlCommand("VC_GroupCall_Hangup_Processor", this._groupCallSqlConStaging);

            }
                
            
            this.xmlDoc = new XmlDocument();
            this.rootElement = this.xmlDoc.CreateElement("Call");
            this.xmlDoc.AppendChild((XmlNode)this.rootElement);
            Thread thread = !SharedClass.IsHangupProcessInMemory ? new Thread(new ThreadStart(this.StartRMQProcessing)) : new Thread(new ThreadStart(this.StartInMemoryProcessing));
            thread.Name = "HangupDataProcessor_" + index.ToString();
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
                    SharedClass.Logger.Info("DeQueue HangUp Data :" + this.hangupData.ToString());
                    if (this.hangupData != null)
                    {
                        this.ProcessHangUpObj();
                        ++this._processedCount;
                        if (this._processedCount == 50)
                        {
                            SharedClass.Logger.Info("Processed 50 CDR");
                            this._processedCount = 0;
                        }   
                    }
                }
                else
                    Thread.Sleep(3000);
            }
            this.isIamRunning = false;
        }
        private void ProcessHangupObjProduction()
        {
            this.retryAttempt = 0;
            while (true)
            {
                try
                {
                    this.rootElement.Attributes.RemoveAll();
                    foreach (JProperty jproperty in ((JObject)this.hangupData.SelectToken("smscresponse")).Properties())
                        this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                    //foreach (JProperty jproperty in this.hangupData.Properties())
                    //    this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                    if (this.sqlCon.State != ConnectionState.Open)
                        this.sqlCon.Open();
                    this.sqlCmd.Parameters.Clear();
                    this.sqlCmd.CommandType = CommandType.StoredProcedure;
                    this.sqlCmd.Parameters.Add("@Data", SqlDbType.VarChar, this.xmlDoc.InnerXml.Length).Value = this.xmlDoc.InnerXml;
                    this.sqlCmd.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                    this.sqlCmd.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                    this.sqlCmd.ExecuteNonQuery();
                    if(!Convert.ToBoolean(this.sqlCmd.Parameters["@Success"].Value))
                    {
                        SharedClass.Logger.Error("Error in updating hanuUpObj: " +this.hangupData.ToString() +" Reason :  " + this.sqlCmd.Parameters["@Message"].Value.ToString());
                    }
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
        private void ProcessHangupObjStaging()
        {
            this.retryAttempt = 0;
            while (true)
            {
                try
                {
                    this.rootElement.Attributes.RemoveAll();
                    //foreach (JProperty jproperty in this.hangupData.Properties())
                    //    this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                    foreach (JProperty jproperty in ((JObject)this.hangupData.SelectToken("smscresponse")).Properties())
                        this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                    if (this.sqlConStaging.State != ConnectionState.Open)
                        this.sqlConStaging.Open();
                    this.sqlCmdStaging.Parameters.Clear();
                    this.sqlCmdStaging.CommandType = CommandType.StoredProcedure;
                    this.sqlCmdStaging.Parameters.Add("@Data", SqlDbType.VarChar, this.xmlDoc.InnerXml.Length).Value = this.xmlDoc.InnerXml;
                    this.sqlCmdStaging.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                    this.sqlCmdStaging.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                    this.sqlCmdStaging.ExecuteNonQuery();
                    if (!Convert.ToBoolean(this.sqlCmdStaging.Parameters["@Success"].Value))
                    {
                        SharedClass.Logger.Error("Error in updating hanuUpObj: " + this.hangupData.ToString() + " Reason :  " + this.sqlCmd.Parameters["@Message"].Value.ToString());
                    }
                    
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
        private void ProcessGroupCallHangupObjStaging()
        {
            this.retryAttempt = 0;
            while (true)
            {
                try
                {
                    this.rootElement.Attributes.RemoveAll();
                    //foreach (JProperty jproperty in this.hangupData.Properties())
                    //    this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                    foreach (JProperty jproperty in ((JObject)this.hangupData.SelectToken("smscresponse")).Properties())
                        this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                    if (this._groupCallSqlConStaging.State != ConnectionState.Open)
                        this._groupCallSqlConStaging.Open();
                    this._groupCallSqlCmdStaging.Parameters.Clear();
                    this._groupCallSqlCmdStaging.CommandType = CommandType.StoredProcedure;
                    this._groupCallSqlCmdStaging.Parameters.Add("@Data", SqlDbType.VarChar, this.xmlDoc.InnerXml.Length).Value = this.xmlDoc.InnerXml;
                    this._groupCallSqlCmdStaging.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                    this._groupCallSqlCmdStaging.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                    this._groupCallSqlCmdStaging.ExecuteNonQuery();
                    if (!Convert.ToBoolean(this._groupCallSqlCmdStaging.Parameters["@Success"].Value))
                    {
                        SharedClass.Logger.Error("Error in updating hanuUpObj: " + this.hangupData.ToString() + " Reason :  " + this.sqlCmd.Parameters["@Message"].Value.ToString());
                    }
                    this.hangupData = null;
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
        private void ProcessGroupCallHangupObjProduction()
        {
            this.retryAttempt = 0;
            while (true)
            {
                try
                {
                    //SharedClass.Logger.Info(" hanuUpObj XML Data: " + this.xmlDoc.InnerXml);
                    this.rootElement.Attributes.RemoveAll();
                    //foreach (JProperty jproperty in this.hangupData.Properties())
                    //    this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                    foreach (JProperty jproperty in ((JObject)this.hangupData.SelectToken("smscresponse")).Properties())
                        this.rootElement.SetAttribute(jproperty.Name, jproperty.Value.ToString());
                    if (this._groupCallSqlConProduction.State != ConnectionState.Open)
                        this._groupCallSqlConProduction.Open();
                    this._groupCallSqlCmdProduction.Parameters.Clear();
                    this._groupCallSqlCmdProduction.CommandType = CommandType.StoredProcedure;
                    this._groupCallSqlCmdProduction.Parameters.Add("@Data", SqlDbType.VarChar, this.xmlDoc.InnerXml.Length).Value = this.xmlDoc.InnerXml;
                    this._groupCallSqlCmdProduction.Parameters.Add("@Success", SqlDbType.Bit).Direction = ParameterDirection.Output;
                    this._groupCallSqlCmdProduction.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = ParameterDirection.Output;
                    this._groupCallSqlCmdProduction.ExecuteNonQuery();
                    if (!Convert.ToBoolean(this._groupCallSqlCmdProduction.Parameters["@Success"].Value))
                    {
                        SharedClass.Logger.Error("Error in updating hanuUpObj: " + this.hangupData.ToString() + " Reason :  " + this.sqlCmd.Parameters["@Message"].Value.ToString());
                    }
                    this.hangupData = null;
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
                        //SharedClass.RabbitMQClient.hangupLazyConsumer.Queue.Dequeue(5000, out this.eventArgs);
                        SharedClass.RabbitMQClient.DeQueueHangupData(5000, out this.eventArgs);
                        if (this.eventArgs != null)
                        {
                            this.retryAttempt = 0;
                            this.message = Encoding.UTF8.GetString(this.eventArgs.Body);
                            this.hangupData = JObject.Parse(this.message);

                            this.ProcessHangUpObj();
                            ++this._processedCount;
                            if (this._processedCount == 50)
                            {
                                SharedClass.Logger.Info("Processed 50 CDR");
                                this._processedCount = 0;
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

        private void ProcessHangUpObj()
        {
            //SharedClass.Logger.Info("HangupObj Data is : " + this.hangupData.ToString());
            try
            {
                JToken token;
                if (hangupData.SelectToken("smscresponse").SelectToken("source") != null)
                {
                    token = hangupData.SelectToken("smscresponse").SelectToken("source").ToString();
                    //SharedClass.Logger.Info("source Token: " + token.ToString());
                    if (token.ToString().Equals(Environment.STAGING.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.ProcessHangupObjStaging();
                    else if (token.ToString().Equals(Environment.PRODUCTION.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.ProcessHangupObjProduction();
                    else if (token.ToString().Equals(Environment.STAGINGGROUPCALL.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.ProcessGroupCallHangupObjStaging();
                    else if (token.ToString().Equals(Environment.PRODUCTIONGROUPCALL.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.ProcessGroupCallHangupObjProduction();
                    else
                        this.ProcessHangupObjProduction();
                }
                else if (hangupData.SelectToken("smscresponse").SelectToken("Source") != null)
                {
                    token = hangupData.SelectToken("smscresponse").SelectToken("Source").ToString();
                    //SharedClass.Logger.Info("Source Token: " + token.ToString());
                    if (token.ToString().Equals(Environment.STAGING.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.ProcessHangupObjStaging();
                    else if (token.ToString().Equals(Environment.PRODUCTION.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.ProcessHangupObjProduction();
                    else if (token.ToString().Equals(Environment.STAGINGGROUPCALL.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.ProcessGroupCallHangupObjStaging();
                    else if (token.ToString().Equals(Environment.PRODUCTIONGROUPCALL.ToString(), StringComparison.CurrentCultureIgnoreCase))
                        this.ProcessGroupCallHangupObjProduction();
                    else
                        this.ProcessHangupObjProduction();
                }
                else
                {
                    SharedClass.Logger.Info("Source Token: " );
                    this.ProcessHangupObjProduction();
                }
            }
            catch(Exception ex)
            {
                SharedClass.Logger.Error("Exception in getting the source parameter from HangUpData Reason :" + ex.ToString());
            }

            
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
