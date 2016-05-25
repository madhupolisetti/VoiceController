using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Data;
using System.Data.SqlClient;
using Newtonsoft.Json.Linq;
using System.Xml;

namespace VoiceController
{
    public class AccountProcessor
    {
        private long accountId = 0L;
        private short accountType = 1;
        private Queue<BulkRequest> bulkRequestsQueue = new Queue<BulkRequest>();
        private Mutex queueMutex = new Mutex();
        private bool shouldIProcess = true;
        private short maxThreads = 1;
        private short activeThreads = 0;

        public AccountProcessor() {
            this.maxThreads = GetConcurrentThreads();
        }
        public void Start()
        {
            SharedClass.Logger.Info("Started");
            while (this.shouldIProcess && !SharedClass.HasStopSignal)
            {
                if (this.QueueCount() > 0 && this.ActiveThreads < this.maxThreads)
                {
                    BulkRequest bulkRequest = this.DeQueue();
                    if (bulkRequest != null)
                    {
                        Thread thread = new Thread(new ParameterizedThreadStart(this.StartBulkProcess));
                        SharedClass.Logger.Info("Spawning New Thread For BulkRequest Id : " + bulkRequest.Id.ToString());
                        thread.Name = "Account_" + this.accountId.ToString() + "_Processor_" + (this.activeThreads + 1).ToString();
                        thread.Start(bulkRequest);
                    }
                }
                Thread.Sleep(2000);
                if (this.ActiveThreads == 0 && this.QueueCount() == 0)
                    this.Stop();
            }
        }
        public void Stop()
        {
            SharedClass.Logger.Info("Stopping AccountId : " + this.AccountId.ToString() + " Processor");
            BulkRequest bulkRequest = null;
            this.shouldIProcess = false;
            while (this.QueueCount() > 0)
            {
                bulkRequest = this.DeQueue();
                if (bulkRequest != null) {
                    bulkRequest.ReEnQueueToDataBase(false);
                }
            }
            while ((int)this.activeThreads > 0)
            {
                SharedClass.Logger.Info("AccountId " + this.AccountId.ToString() + " has still " + this.ActiveThreads.ToString() + " active process threads running");
                Thread.Sleep(1000);
            }
            SharedClass.ReleaseAccountProcessor(this.AccountId);
        }
        public void StartBulkProcess(object input)
        {
            BulkRequest bulkRequest = input as BulkRequest;            
            SharedClass.Logger.Info("Started Processing BulkRequest " + bulkRequest.DisplayString());
            ++this.ActiveThreads;            
            System.Data.DataTable mobileUUIDsTable = new System.Data.DataTable();
            string[] destinationsArray = null;
            string[] uuidsArray = null;
            JObject ChunkProcessResponse = null;

            mobileUUIDsTable.Columns.Add("Mobile", typeof(string));
            mobileUUIDsTable.Columns.Add("UUID", typeof(string));
            try
            {
                destinationsArray = bulkRequest.Destinations.ToString().Split(',');
                uuidsArray = bulkRequest.UUIDs.ToString().Split(',');
                if (destinationsArray.Length != uuidsArray.Length)
                {
                    SharedClass.Logger.Error("Destinations Count (" + destinationsArray.Length.ToString() + ") And UUIDs Count (" + uuidsArray.Length.ToString() + ") Mismatch. Terminating Process.");
                    bulkRequest.ReEnQueueToDataBase(true);
                    return;
                }
                for (int iterator = bulkRequest.ProcessedCount; iterator < destinationsArray.Length ; iterator++)
                {
                    mobileUUIDsTable.Rows.Add(destinationsArray[iterator], uuidsArray[iterator]);
                    if (mobileUUIDsTable.Rows.Count == SharedClass.BulkRequestBatchCount) {
                        ChunkProcessResponse = ProcessChunk(bulkRequest, mobileUUIDsTable);
                        if (Convert.ToBoolean(ChunkProcessResponse.SelectToken("Success").ToString()) == false)
                        {
                            SharedClass.Logger.Error("Error Processing BulkRequest : " + bulkRequest.DisplayString() + ", Reason : " + ChunkProcessResponse.SelectToken("Message").ToString());
                            bulkRequest.ReEnQueueToDataBase(true);
                            return;
                        }
                        else {
                            bulkRequest.ProcessedCount += mobileUUIDsTable.Rows.Count;
                            bulkRequest.UpdateProcessedCount();
                        }
                        mobileUUIDsTable.Rows.Clear();
                    }
                }
                if (mobileUUIDsTable.Rows.Count > 0)
                {
                    ChunkProcessResponse = ProcessChunk(bulkRequest, mobileUUIDsTable);
                    if (Convert.ToBoolean(ChunkProcessResponse.SelectToken("Success").ToString()) == false)
                    {
                        SharedClass.Logger.Error("Error Processing BulkRequest : " + bulkRequest.DisplayString() + ", Reason : " + ChunkProcessResponse.SelectToken("Message").ToString());
                        bulkRequest.ReEnQueueToDataBase(true);
                        return;
                    }
                    else
                    {
                        bulkRequest.ProcessedCount += mobileUUIDsTable.Rows.Count;
                        bulkRequest.UpdateProcessedCount();
                    }
                    mobileUUIDsTable.Rows.Clear();
                }
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Error Processing BulkRequest : " + e.ToString());
                bulkRequest.UpdateProcessedCount();
            }
            finally {
                --this.ActiveThreads;   
            }
        }

        private JObject ProcessChunk(BulkRequest bulkRequest, System.Data.DataTable mobileUUIDsTable) {
            SqlConnection sqlCon = new SqlConnection(SharedClass.ConnectionString);
            SqlCommand sqlCmd = new SqlCommand("Create_Call");
            short retryAttempt = 0;
            SqlParameter mobileUUIDParameter = null;
            SqlParameter xmlTagNamesParameter = null;
            XmlDocument xmlDoc = new XmlDocument();
            xmlDoc.LoadXml(bulkRequest.Xml);
            DataTable xmlTagNamesTable = GetXmlTagNames(xmlDoc);
            retryLabel:
            try
            {   
                sqlCmd.Parameters.Add("@AccountId", SqlDbType.BigInt).Value = this.accountId;
                sqlCmd.Parameters.Add("@AccountType", SqlDbType.TinyInt).Value = this.accountType;
                sqlCmd.Parameters.Add("@ToolId", SqlDbType.TinyInt).Value = bulkRequest.ToolId;
                sqlCmd.Parameters.Add("@Xml", SqlDbType.VarChar, bulkRequest.Xml.Length).Value = bulkRequest.Xml;
                mobileUUIDParameter = sqlCmd.Parameters.Add("@MobileNumbersAndUUIDs", SqlDbType.Structured);
                mobileUUIDParameter.TypeName = "dbo.MobileNumberUUIDType";
                mobileUUIDParameter.Value = mobileUUIDsTable;
                xmlTagNamesParameter =  sqlCmd.Parameters.Add("@XmlTagNames", SqlDbType.Structured);
                xmlTagNamesParameter.TypeName = "dbo.XmlTagNames";
                xmlTagNamesParameter.Value = xmlTagNamesTable;
                sqlCmd.Parameters.Add("@IpAddress", SqlDbType.VarChar, bulkRequest.Ip.Length).Value = bulkRequest.Ip;
                sqlCmd.Parameters.Add("@AnswerUrl", SqlDbType.VarChar, bulkRequest.AnswerUrl.Length).Value = bulkRequest.AnswerUrl;
                sqlCmd.Parameters.Add("@RingUrl", SqlDbType.VarChar, bulkRequest.RingUrl.Length).Value = bulkRequest.RingUrl;
                sqlCmd.Parameters.Add("@HangupUrl", SqlDbType.VarChar, bulkRequest.HangupUrl.Length).Value = bulkRequest.HangupUrl;
                sqlCmd.Parameters.Add("@CallerId", SqlDbType.VarChar, bulkRequest.CallerId.Length).Value = bulkRequest.CallerId;
                sqlCmd.Parameters.Add("@Retries", SqlDbType.TinyInt).Value = bulkRequest.Retries;
                sqlCmd.Parameters.Add("@BulkRequestId", SqlDbType.BigInt).Value = bulkRequest.Id;
                sqlCmd.Parameters.Add("@StatusCode", SqlDbType.Int).Direction = System.Data.ParameterDirection.Output;
                sqlCmd.Parameters.Add("@Success", SqlDbType.Bit).Direction = System.Data.ParameterDirection.Output;
                sqlCmd.Parameters.Add("@Message", SqlDbType.VarChar, 1000).Direction = System.Data.ParameterDirection.Output;
                if (sqlCon.State != ConnectionState.Open)
                {
                    sqlCmd.Connection = sqlCon;
                    sqlCmd.CommandType = CommandType.StoredProcedure;
                    sqlCon.ConnectionString = SharedClass.ConnectionString;
                    sqlCon.Open();
                } 
                sqlCmd.ExecuteNonQuery();
                return GetOutputParametersAsJSon(sqlCmd.Parameters);
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Error Processing BulkRequest (retryAttempt : " + retryAttempt + ") : " + e.ToString());
                ++retryAttempt;
                if (retryAttempt <= 3)
                {
                    goto retryLabel;
                }
                else {
                    SharedClass.Logger.Error("Max Retry Attempts Reached with error : " + e.ToString());
                    return new JObject(new JProperty("Success", false), new JProperty("Message", e.ToString()));
                }
            }
            finally {
                if (sqlCon.State == System.Data.ConnectionState.Open) {
                    sqlCon.Close();
                }
                sqlCon.Dispose();
                sqlCmd.Dispose();
            }
        }
        private DataTable GetXmlTagNames(XmlDocument doc)
        {
            DataTable result = new DataTable();
            result.Columns.Add("TagName", typeof(string));
            Stack<XmlElement> stack = new Stack<XmlElement>();
            stack.Push(doc.FirstChild as XmlElement);
            bool exists = false;
            while (stack.Count > 0)
            {
                XmlElement currentElement = stack.Pop();
                try
                {
                    foreach (XmlElement subElement in currentElement.ChildNodes)
                    {
                        stack.Push(subElement);
                        foreach (DataRow row in result.Rows)
                        {
                            if (row["TagName"].ToString() == subElement.Name)
                            {
                                exists = true;
                                break;
                            }
                        }
                        if (!exists)
                            result.Rows.Add(subElement.Name);
                    }
                }
                catch (Exception e)
                {

                }
            }
            return result;
        }
        public JObject GetOutputParametersAsJSon(SqlParameterCollection parameters) { 
            JObject jobj = new JObject();
            foreach (SqlParameter parameter in parameters) {
                if (parameter.Direction == ParameterDirection.Output) {
                    jobj.Add(new JProperty(parameter.ParameterName.Replace("@", ""), parameter.Value));
                }
            }
            return jobj;
        }
        public bool EnQueue(BulkRequest bulkRequest)
        {
            SharedClass.Logger.Info("EnQueuing BulkRequest " + bulkRequest.Id.ToString() + " Into AccountId " + this.accountId.ToString() + " Processor");
            bool flag = false;
            try
            {
                while (!this.queueMutex.WaitOne())
                    Thread.Sleep(200);
                this.bulkRequestsQueue.Enqueue(bulkRequest);
                flag = true;
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error EnQueuing BulkRequest Id : " + bulkRequest.Id.ToString() + " Into AccountId : " + this.accountId.ToString() + " Processor. Reason : " + ex.ToString());
            }
            finally
            {
                this.queueMutex.ReleaseMutex();
            }
            return flag;
        }

        private int QueueCount()
        {
            int num = 0;
            try
            {
                while (!this.queueMutex.WaitOne())
                    Thread.Sleep(200);
                num = this.bulkRequestsQueue.Count;
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error Querying Count In Account Id : " + this.accountId.ToString() + " Processor. Reason : " + ex.ToString());
            }
            finally
            {
                this.queueMutex.ReleaseMutex();
            }
            return num;
        }

        private BulkRequest DeQueue()
        {
            BulkRequest bulkRequest = null;
            try
            {
                while (!this.queueMutex.WaitOne())
                    Thread.Sleep(200);
                bulkRequest = this.bulkRequestsQueue.Dequeue();
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error DeQueuing In AccountId : " + this.accountId.ToString() + " Processor. Reason : " + ex.ToString());
            }
            finally
            {
                this.queueMutex.ReleaseMutex();
            }
            return bulkRequest;
        }

        private short GetConcurrentThreads() {
            short concurrentThreads = 1;            
            return concurrentThreads;
        }
        public long AccountId { get { return this.accountId; } set { this.accountId = value; } } 
        public short MaxThreads { get { return this.maxThreads; } set { this.maxThreads = value; } } 
        public short ActiveThreads { get { return this.activeThreads; } set { this.activeThreads = value; } }
        public short AccountType { get { return this.accountType; } set { this.accountType = value; } }
    }
}
