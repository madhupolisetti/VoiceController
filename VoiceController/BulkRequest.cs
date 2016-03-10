using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public class BulkRequest
    {
        private long id = 0L;
        private string xml = "";
        private string ip = "";
        private StringBuilder destinations = (StringBuilder)null;
        private StringBuilder uuids = (StringBuilder)null;
        private string ringUrl = "";
        private string answerUrl = "";
        private string hangupUrl = "";
        private short retries = (short)0;
        private string callerId = "";
        private short status = (short)0;
        private int processedCount = 0;
        private long voiceRequestId = 0L;
        private short toolId = 0;

        public long Id { get { return this.id; } set { this.id = value; } } 
        public string Xml { get { return this.xml; } set { this.xml = value; } } 
        public string Ip { get { return this.ip; } set { this.ip = value; } } 
        public StringBuilder Destinations { get { if (this.destinations == null) this.destinations = new StringBuilder(); return this.destinations; } set { this.destinations = value; } } 
        public StringBuilder UUIDs { get { if (this.uuids == null) this.uuids = new StringBuilder(); return this.uuids; } set { this.uuids = value; } } 
        public string RingUrl { get { return this.ringUrl; } set { this.ringUrl = value; } } 
        public string AnswerUrl { get { return this.answerUrl; } set { this.answerUrl = value; } } 
        public string HangupUrl { get { return this.hangupUrl; } set { this.hangupUrl = value; } } 
        public short Retries { get { return this.retries; } set { this.retries = value; } } 
        public string CallerId { get { return this.callerId; } set { this.callerId = value; } } 
        public short Status { get { return this.status; } set { this.status = value; } }
        public int ProcessedCount { get { return this.processedCount; } set { this.processedCount = value; } } 
        public long VoiceRequestId { get { return this.voiceRequestId; } set { this.voiceRequestId = value; } }
        public short ToolId { get { return this.toolId; } set { this.toolId = value; } }
        public string DisplayString()
        {
            return " Id : " + (object)this.Id + ", RingUrl : " + this.RingUrl + ", AnswerUrl : " + this.AnswerUrl + ", HangupUrl : " + this.HangupUrl + ", CallerId : " + this.CallerId + ", Status : " + (string)(object)this.Status + ", ProcessedCount : " + (string)(object)this.ProcessedCount + ", VoiceRequestId : " + (string)(object)this.VoiceRequestId;
        }
        public void Update() {
            System.Data.SqlClient.SqlConnection sqlCon = null;
            System.Data.SqlClient.SqlCommand sqlCmd = null;
            try
            {
                sqlCon = new System.Data.SqlClient.SqlConnection(SharedClass.ConnectionString);
                sqlCmd = new System.Data.SqlClient.SqlCommand("Update BulkVoiceRequests with(rowlock) Set ProcessedCount = " + this.processedCount + " Where Id = " + this.id);
                sqlCon.Open();
                sqlCmd.ExecuteNonQuery();
                sqlCon.Close();
            }
            catch (Exception e) {
                SharedClass.Logger.Error("Error Updating BulkRequest : " + e.ToString());
            }
        }
        public void ReEnQueueToDataBase(bool isDeQueued = true) {
            System.Data.SqlClient.SqlConnection sqlCon = null;
            System.Data.SqlClient.SqlCommand sqlCmd = null;
            try
            {
                sqlCon = new System.Data.SqlClient.SqlConnection(SharedClass.ConnectionString);
                sqlCmd = new System.Data.SqlClient.SqlCommand("Update BulkVoiceRequests with(rowlock) Set Status = " + ((isDeQueued == true) ? 9 : 0) + " Where Id = " + this.id);
                sqlCon.Open();
                sqlCmd.ExecuteNonQuery();
                sqlCon.Close();
            }
            catch (Exception e)
            {
                SharedClass.Logger.Error("Error ReEnQueuing BulkRequest : " + e.ToString());
            }
        }
    }
}
