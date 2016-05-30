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
        private StringBuilder destinations = null;
        private StringBuilder uuids = null;
        private string ringUrl = "";
        private string answerUrl = "";
        private string hangupUrl = "";
        private byte retries = 0;
        private string callerId = "";
        private byte status = 0;
        private int processedCount = 0;
        private int totalCount = 0;
        private long voiceRequestId = 0L;
        private byte toolId = 0;

        public long Id { get { return this.id; } set { this.id = value; } } 
        public string Xml { get { return this.xml; } set { this.xml = value; } } 
        public string Ip { get { return this.ip; } set { this.ip = value; } } 
        public StringBuilder Destinations { get { if (this.destinations == null) this.destinations = new StringBuilder(); return this.destinations; } set { this.destinations = value; } } 
        public StringBuilder UUIDs { get { if (this.uuids == null) this.uuids = new StringBuilder(); return this.uuids; } set { this.uuids = value; } } 
        public string RingUrl { get { return this.ringUrl; } set { this.ringUrl = value; } } 
        public string AnswerUrl { get { return this.answerUrl; } set { this.answerUrl = value; } } 
        public string HangupUrl { get { return this.hangupUrl; } set { this.hangupUrl = value; } } 
        public byte Retries { get { return this.retries; } set { this.retries = value; } } 
        public string CallerId { get { return this.callerId; } set { this.callerId = value; } } 
        public byte Status { get { return this.status; } set { this.status = value; } }
        public int ProcessedCount { get { return this.processedCount; } set { this.processedCount = value; } } 
        public long VoiceRequestId { get { return this.voiceRequestId; } set { this.voiceRequestId = value; } }
        public byte ToolId { get { return this.toolId; } set { this.toolId = value; } }
        public int TotalCount { get { return this.totalCount; } set { this.totalCount = value; } }
        public string DisplayString()
        {
            return " Id : " + this.Id.ToString() + ", RingUrl : " + this.RingUrl + ", AnswerUrl : " + this.AnswerUrl + ", HangupUrl : " + this.HangupUrl + ", CallerId : " + this.CallerId + ", Status : " + this.Status.ToString() + ", ProcessedCount : " + this.ProcessedCount.ToString() + ", VoiceRequestId : " + this.VoiceRequestId.ToString();
        }
        public void UpdateProcessedCount() {
            System.Data.SqlClient.SqlConnection sqlCon = null;
            System.Data.SqlClient.SqlCommand sqlCmd = null;
            try
            {
                //SharedClass.Logger.Info("BulkRequestId : " + this.id.ToString() + ", Processed : " + this.processedCount.ToString() + ", Remaining : " + (this.))
                sqlCon = new System.Data.SqlClient.SqlConnection(SharedClass.ConnectionString);
                sqlCmd = new System.Data.SqlClient.SqlCommand("Update BulkVoiceRequests with(rowlock) Set ProcessedCount = " + this.processedCount.ToString() + " Where Id = " + this.id.ToString(), sqlCon);
                sqlCon.Open();
                sqlCmd.ExecuteNonQuery();
                sqlCon.Close();
            }
            catch (Exception e) {
                SharedClass.Logger.Error("Error Updating BulkRequest : " + e.ToString());
            }
        }
        public void ReEnQueueToDataBase(bool isDeQueued = true, string reason = "") {
            System.Data.SqlClient.SqlConnection sqlCon = null;
            System.Data.SqlClient.SqlCommand sqlCmd = null;
            try
            {
                if (reason.Length > 500)
                    reason = reason.Substring(0, 500);
                sqlCon = new System.Data.SqlClient.SqlConnection(SharedClass.ConnectionString);
                sqlCmd = new System.Data.SqlClient.SqlCommand("Update BulkVoiceRequests with(rowlock) Set Status = @Status, Reason = @Reason Where Id = @BulkRequestId", sqlCon);
                // + ((isDeQueued == true) ? 9 : 0).ToString() + " Where Id = " + this.id.ToString(), sqlCon);
                sqlCmd.Parameters.Add("@Status", System.Data.SqlDbType.TinyInt).Value = isDeQueued == true ? 9 : 0;
                sqlCmd.Parameters.Add("@Reason", System.Data.SqlDbType.VarChar, 500).Value = reason;
                sqlCmd.Parameters.Add("@BulkRequestId", System.Data.SqlDbType.BigInt).Value = this.id;
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
