using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    [Serializable]
    public class Call
    {
        private long _queueTableSlno = 0;
        private long _mobileId = 0;
        private long _accountId = 0;
        private string _uuid = string.Empty;
        private string _callerId = string.Empty;
        private string _destination = string.Empty;
        private string _xml = string.Empty;
        private string _ringUrl = string.Empty;
        private string _answerUrl = string.Empty;
        private string _hangupUrl = string.Empty;
        private byte _pulse = 0;
        private float _pricePerPulse = 0.0f;
        private byte _priorityValue = 0;
        private bool _isGroupCall = false;
        private Environment _environment = Environment.PRODUCTION;

        public long QueueTableSlno
        {
            get
            {
                return this._queueTableSlno;
            }
            set
            {
                this._queueTableSlno = value;
            }
        }

        public long CallId
        {
            get
            {
                return this._mobileId;
            }
            set
            {
                this._mobileId = value;
            }
        }

        public long AccountId
        {
            get
            {
                return this._accountId;
            }
            set
            {
                this._accountId = value;
            }
        }

        public string UUID
        {
            get
            {
                return this._uuid;
            }
            set
            {
                this._uuid = value;
            }
        }

        public string CallerId
        {
            get
            {
                return this._callerId;
            }
            set
            {
                this._callerId = value;
            }
        }

        public string Destination
        {
            get
            {
                return this._destination;
            }
            set
            {
                this._destination = value;
            }
        }

        public string Xml
        {
            get
            {
                return this._xml;
            }
            set
            {
                this._xml = value;
            }
        }

        public string RingUrl
        {
            get
            {
                return this._ringUrl;
            }
            set
            {
                this._ringUrl = value;
            }
        }

        public string AnswerUrl
        {
            get
            {
                return this._answerUrl;
            }
            set
            {
                this._answerUrl = value;
            }
        }

        public string HangupUrl
        {
            get
            {
                return this._hangupUrl;
            }
            set
            {
                this._hangupUrl = value;
            }
        }

        public byte Pulse
        {
            get
            {
                return this._pulse;
            }
            set
            {
                this._pulse = value;
            }
        }

        public float PricePerPulse
        {
            get
            {
                return this._pricePerPulse;
            }
            set
            {
                this._pricePerPulse = value;
            }
        }

        public byte PriorityValue
        {
            get
            {
                return this._priorityValue;
            }
            set
            {
                this._priorityValue = value;
            }
        }
        public Environment Environment
        {
            get { return this._environment; }
            set { this._environment = value; }
        }

        public bool IsGroupCall
        {
            get { return this._isGroupCall; }
            set { this._isGroupCall = value; }
        }

        public string PrintMe()
        {
            return " QueueTableSlno : " + this.QueueTableSlno.ToString() + ", Environment : " + this._environment.ToString() + ", MobileId : " + this.CallId.ToString() + ", Destination : " + this.Destination + ", CallerId : " + this.CallerId + ", IsGroupCall : " +  this.IsGroupCall.ToString();
        }
    }
}
