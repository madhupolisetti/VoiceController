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
        private List<string> prefixes = null;
        private HttpListener listener = (HttpListener)null;
        public void Initialize()
        {
            this.prefixes = new List<string>();
            this.prefixes.Add("http://" + this.ip + ":" + this.port.ToString() + "/Gateways/");
            this.prefixes.Add("http://" + this.ip + ":" + this.port.ToString() + "/VoiceRequests/");
            new Thread(new ThreadStart(this.Start))
            {
                Name = "Listener"
            }.Start();
        }

        public void Destroy()
        {
            if (this.listener == null) {
                return;
            }
            try
            {
                this.listener.Stop();
                SharedClass.Logger.Info("WebListener Stop Executed");
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error Stopping WebListener : " + ex.Message);
            }
            try
            {
                this.listener.Close();
                SharedClass.Logger.Info("Listener Close Executed");
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error Closing Listener " + ex.ToString());
            }
            try
            {
                this.listener.Abort();
                SharedClass.Logger.Info("Listener Abort Executed");
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error Aborting Listener " + ex.ToString());
            }
            this.listener = (HttpListener)null;
        }

        private void Start()
        {
            this.listener = new HttpListener();
            foreach (string uriPrefix in this.prefixes)
            {
                SharedClass.Logger.Info("Adding Url " + uriPrefix + " To Listener");
                try
                {
                    this.listener.Prefixes.Add(uriPrefix);
                }
                catch (Exception ex)
                {
                    SharedClass.Logger.Error("Error Adding prefix To Listener, Reason : " + ex.ToString());
                }
            }
            this.listener.Start();
            SharedClass.Logger.Info("Started Listening On " + this.ip + ":" + this.port.ToString());
            //do
            //    ;
            //while (!SharedClass.HasStopSignal);
            //SharedClass.Logger.Info((object)"Listener Exited From While Loop");
        }
        public string Ip { get { return this.ip; } set { this.ip = value; } }
        public int Port { get { return this.port; } set { this.port = value; } } 
    }
}
