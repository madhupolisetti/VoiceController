using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;

namespace VoiceController
{
    public class Listener
    {
        private string ip = "";
        private int port = 0;
        private List<string> prefixes = new List<string>();
        private HttpListener listener = (HttpListener)null;

        public string Ip
        {
            get
            {
                return this.ip;
            }
            set
            {
                this.ip = value;
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

        public void Initialize()
        {
            this.prefixes.Add("http://" + (object)this.ip + ":" + (string)(object)this.port + "/Gateways/");
            this.prefixes.Add("http://" + (object)this.ip + ":" + (string)(object)this.port + "/VoiceRequests/");
            new Thread(new ThreadStart(this.Start))
            {
                Name = "Listener"
            }.Start();
        }

        public void Destroy()
        {
            try
            {
                this.listener.Stop();
                SharedClass.Logger.Info((object)"WebListener Stop Executed");
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error((object)("Error Stopping WebListener : " + ex.Message));
            }
            try
            {
                this.listener.Close();
                SharedClass.Logger.Info((object)"Listener Close Executed");
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error((object)("Error Closing Listener " + ex.ToString()));
            }
            try
            {
                this.listener.Abort();
                SharedClass.Logger.Info((object)"Listener Abort Executed");
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error((object)("Error Aborting Listener " + ex.ToString()));
            }
            this.listener = (HttpListener)null;
        }

        private void Start()
        {
            this.listener = new HttpListener();
            foreach (string uriPrefix in this.prefixes)
            {
                SharedClass.Logger.Info((object)("Adding Url " + uriPrefix + " To Listener"));
                try
                {
                    this.listener.Prefixes.Add(uriPrefix);
                }
                catch (Exception ex)
                {
                    SharedClass.Logger.Error((object)("Error Adding prefix To Listener, Reason : " + ex.ToString()));
                }
            }
            this.listener.Start();
            SharedClass.Logger.Info((object)"Started Listening");
            //do
            //    ;
            //while (!SharedClass.HasStopSignal);
            //SharedClass.Logger.Info((object)"Listener Exited From While Loop");
        }
    }
}
