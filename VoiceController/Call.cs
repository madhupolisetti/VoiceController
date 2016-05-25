﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    [Serializable]
    public class Call
    {
        private long queueTableSlno = 0L;
        private long mobileId = 0L;
        private long accountId = 0L;
        private string uuid = "";
        private string callerId = "";
        private string destination = "";
        private string xml = "";
        private string ringUrl = "";
        private string answerUrl = "";
        private string hangupUrl = "";
        private byte pulse = 0;
        private float pricePerPulse = 0.0f;
        private byte priorityValue = 0;

        public long QueueTableSlno
        {
            get
            {
                return this.queueTableSlno;
            }
            set
            {
                this.queueTableSlno = value;
            }
        }

        public long CallId
        {
            get
            {
                return this.mobileId;
            }
            set
            {
                this.mobileId = value;
            }
        }

        public long AccountId
        {
            get
            {
                return this.accountId;
            }
            set
            {
                this.accountId = value;
            }
        }

        public string UUID
        {
            get
            {
                return this.uuid;
            }
            set
            {
                this.uuid = value;
            }
        }

        public string CallerId
        {
            get
            {
                return this.callerId;
            }
            set
            {
                this.callerId = value;
            }
        }

        public string Destination
        {
            get
            {
                return this.destination;
            }
            set
            {
                this.destination = value;
            }
        }

        public string Xml
        {
            get
            {
                return this.xml;
            }
            set
            {
                this.xml = value;
            }
        }

        public string RingUrl
        {
            get
            {
                return this.ringUrl;
            }
            set
            {
                this.ringUrl = value;
            }
        }

        public string AnswerUrl
        {
            get
            {
                return this.answerUrl;
            }
            set
            {
                this.answerUrl = value;
            }
        }

        public string HangupUrl
        {
            get
            {
                return this.hangupUrl;
            }
            set
            {
                this.hangupUrl = value;
            }
        }

        public byte Pulse
        {
            get
            {
                return this.pulse;
            }
            set
            {
                this.pulse = value;
            }
        }

        public float PricePerPulse
        {
            get
            {
                return this.pricePerPulse;
            }
            set
            {
                this.pricePerPulse = value;
            }
        }

        public byte PriorityValue
        {
            get
            {
                return this.priorityValue;
            }
            set
            {
                this.priorityValue = value;
            }
        }

        public string PrintMe()
        {
            return " QueueTableSlno : " + this.QueueTableSlno.ToString() + ", MobileId : " + this.CallId.ToString() + ", Destination : " + this.Destination + ", CallerId : " + this.CallerId;
        }
    }
}
