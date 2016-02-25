using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public class Notifier
    {
        private string authKey = (string)null;
        private string authToken = (string)null;
        private string sendAlertsTo = (string)null;
        private string senderId = (string)null;
        private string apiUrl = (string)null;

        public string AuthKey
        {
            get
            {
                return this.authKey;
            }
            set
            {
                this.authKey = value;
            }
        }

        public string AuthToken
        {
            get
            {
                return this.authToken;
            }
            set
            {
                this.authToken = value;
            }
        }

        public string SendAlertsTo
        {
            get
            {
                return this.sendAlertsTo;
            }
            set
            {
                this.sendAlertsTo = value;
            }
        }

        public string SenderId
        {
            get
            {
                return this.senderId;
            }
            set
            {
                this.senderId = value;
            }
        }

        public string ApiUrl
        {
            get
            {
                return this.apiUrl;
            }
            set
            {
                this.apiUrl = value;
            }
        }

        public string SendSms(string text)
        {
            throw new NotImplementedException();
        }
    }
}
