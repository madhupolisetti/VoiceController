using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public class BulkRequest
    {
        private long _id = 0;
        private string _xml = string.Empty;
        private string _ip = string.Empty;
        private StringBuilder _destinations = null;
        private StringBuilder _uuids = null;
        private string _ringUrl = "";
        private string _answerUrl = "";
        private string _hangupUrl = "";
        private byte _retries = 0;
        private string _callerId = "";
        private byte _status = 0;
        private int _processedCount = 0;
        private int _totalCount = 0;
        private long _voiceRequestId = 0L;
        private byte _toolId = 0;
        private long _campaignScheduleId = 0; 
        private Environment _environment = Environment.PRODUCTION;

        public long Id { get { return this._id; } set { this._id = value; } } 
        public string Xml { get { return this._xml; } set { this._xml = value; } } 
        public string Ip { get { return this._ip; } set { this._ip = value; } } 
        public StringBuilder Destinations { get { if (this._destinations == null) this._destinations = new StringBuilder(); return this._destinations; } set { this._destinations = value; } } 
        public StringBuilder UUIDs { get { if (this._uuids == null) this._uuids = new StringBuilder(); return this._uuids; } set { this._uuids = value; } } 
        public string RingUrl { get { return this._ringUrl; } set { this._ringUrl = value; } } 
        public string AnswerUrl { get { return this._answerUrl; } set { this._answerUrl = value; } } 
        public string HangupUrl { get { return this._hangupUrl; } set { this._hangupUrl = value; } } 
        public byte Retries { get { return this._retries; } set { this._retries = value; } } 
        public string CallerId { get { return this._callerId; } set { this._callerId = value; } } 
        public byte Status { get { return this._status; } set { this._status = value; } }
        public int ProcessedCount { get { return this._processedCount; } set { this._processedCount = value; } } 
        public long VoiceRequestId { get { return this._voiceRequestId; } set { this._voiceRequestId = value; } }
        public byte ToolId { get { return this._toolId; } set { this._toolId = value; } }
        public int TotalCount { get { return this._totalCount; } set { this._totalCount = value; } }
        public Environment Environment
        {
            get { return this._environment; }
            set { this._environment = value; }
        }
        public long CampaignScheduleId { get { return this._campaignScheduleId; } set { this._campaignScheduleId = value; } }

        public string DisplayString()
        {
            return " Id : " + this.Id.ToString() + ", RingUrl : " + this.RingUrl + ", AnswerUrl : " + this.AnswerUrl + ", HangupUrl : " + this.HangupUrl + ", CallerId : " + this.CallerId + ", Status : " + this.Status.ToString() + ", ProcessedCount : " + this.ProcessedCount.ToString() + ", VoiceRequestId : " + this.VoiceRequestId.ToString() + " CampaignScheduleId : "+this.CampaignScheduleId.ToString()+" Environment : " + this.Environment.ToString();
        }
        public void UpdateProcessedCount() {
            System.Data.SqlClient.SqlConnection sqlCon = null;
            System.Data.SqlClient.SqlCommand sqlCmd = null;
            try
            {
                //SharedClass.Logger.Info("BulkRequestId : " + this.id.ToString() + ", Processed : " + this.processedCount.ToString() + ", Remaining : " + (this.))
                sqlCon = new System.Data.SqlClient.SqlConnection(SharedClass.GetConnectionString(this._environment));
                sqlCmd = new System.Data.SqlClient.SqlCommand("Update BulkVoiceRequests with(rowlock) Set ProcessedCount = " + this._processedCount.ToString() + " Where Id = " + this._id.ToString(), sqlCon);
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
                sqlCon = new System.Data.SqlClient.SqlConnection(SharedClass.GetConnectionString(this._environment));
                sqlCmd = new System.Data.SqlClient.SqlCommand("Update BulkVoiceRequests with(rowlock) Set Status = @Status, Reason = @Reason Where Id = @BulkRequestId", sqlCon);
                // + ((isDeQueued == true) ? 9 : 0).ToString() + " Where Id = " + this.id.ToString(), sqlCon);
                sqlCmd.Parameters.Add("@Status", System.Data.SqlDbType.TinyInt).Value = isDeQueued == true ? 9 : 0;
                sqlCmd.Parameters.Add("@Reason", System.Data.SqlDbType.VarChar, 500).Value = reason;
                sqlCmd.Parameters.Add("@BulkRequestId", System.Data.SqlDbType.BigInt).Value = this._id;
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
