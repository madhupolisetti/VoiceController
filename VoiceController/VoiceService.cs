using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace VoiceController
{
    public partial class VoiceService : ServiceBase
    {
        Thread mainThread = null;
        ApplicationController appController = null;
        public VoiceService()
        {
            InitializeComponent();
        }

        protected override void OnStart(string[] args)
        {
            appController = new ApplicationController();
            mainThread = new Thread(new ThreadStart(appController.Start));
            mainThread.Name = "ApplicationController";
            mainThread.Start();
        }

        protected override void OnStop()
        {
            SharedClass.HasStopSignal = true;
            Thread.CurrentThread.Name = "StopSignal";
            SharedClass.Logger.Info("========= Service Stop Signal Received ===========");
            // Add code here to perform any tear-down necessary to stop your service.
            appController.Stop();
            while (!SharedClass.IsServiceCleaned)
            {
                SharedClass.Logger.Info("Sleeping In OnStop. Service Not Yet Cleaned");
                Thread.Sleep(1000);
            }
            SharedClass.Logger.Info("========= Service Stopped ===========");
        }
    }
}
