using System;
using System.Collections.Generic;
using System.Net;
using System.Threading;
using Newtonsoft.Json.Linq;
using System.Reflection;
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
            HttpListenerContext context = null;
            JObject responseJSonObject = new JObject();
            JArray responseJSonArray = new JArray();
            JObject tempJObject = new JObject();
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
            while (!SharedClass.HasStopSignal)
            {
                try
                {
                    try
                    {
                        context = null;
                        context = listener.GetContext();
                    }
                    catch (HttpListenerException e)
                    {
                        continue;
                    }
                    if (context != null)
                    {
                        responseJSonObject.RemoveAll();
                        responseJSonArray.RemoveAll();
                        tempJObject.RemoveAll();
                        SharedClass.Logger.Info("New Request From " + context.Request.RemoteEndPoint.Address.ToString() + ":" + context.Request.RemoteEndPoint.Port.ToString() + " To : " + context.Request.RawUrl);
                        if (context.Request.RawUrl.Contains("/Gateways"))
                        {
                            lock (SharedClass.GatewayMap)
                            {
                                foreach (KeyValuePair<int, Gateway> kvp in SharedClass.GatewayMap)
                                {
                                    tempJObject = new JObject();
                                    foreach (PropertyInfo property in kvp.Value.GetType().GetProperties())
                                    {
                                        if (property.CanRead)
                                            tempJObject.Add(new JProperty(property.Name, property.GetValue(kvp.Value).ToString()));
                                    }
                                    responseJSonArray.Add(tempJObject);
                                }
                            }
                            responseJSonObject.Add(new JProperty("Success", true));
                            responseJSonObject.Add(new JProperty("Message", "Action Completed"));
                            responseJSonObject.Add(new JProperty("Gateways", responseJSonArray));
                        }
                    }
                }
                catch (Exception e)
                {
                    responseJSonObject = new JObject(new JProperty("Success", false), new JProperty("Message", e.ToString()));
                }
                finally
                {
                    try
                    {
                        if (context != null)
                        {
                            context.Response.OutputStream.Write(System.Text.Encoding.UTF8.GetBytes(responseJSonObject.ToString()), 0, System.Text.Encoding.UTF8.GetByteCount(responseJSonObject.ToString()));
                            context.Response.Close();
                        }
                    }
                    catch (Exception e)
                    {
                        SharedClass.Logger.Error("Error Writing Response : " + e.ToString());
                    }
                }
            }
            //do
            //    ;
            //while (!SharedClass.HasStopSignal);
            //SharedClass.Logger.Info((object)"Listener Exited From While Loop");
        }
        public string Ip { get { return this.ip; } set { this.ip = value; } }
        public int Port { get { return this.port; } set { this.port = value; } } 
    }
}
