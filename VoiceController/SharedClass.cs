using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using log4net;
using System.Threading;

namespace VoiceController
{
    public class SharedClass
    {

        public const string POST = "POST";

        private static string connectionString = null;
        private static bool hasStopSignal = false;
        private static bool isServiceCleaned = true;
        private static int gatewayHeartBeatSpan = 30;        
        private static ILog logger = null;
        private static ILog dumpLogger = null;
        private static ILog heartBeatLogger = null;
        private static bool isHangupProcessInMemory = false;
        private static RabbitMQClient rabbitMQClient = null;
        private static HangupProcessor hangupProcessor = null;
        private static bool isHangupConsumerRunning = false;
        private static bool isCallFlowsConsumerRunning = false;
        private static bool isHangupLazyProcessorRunning = false;
        private static Dictionary<int, Gateway> gatewayMap = new Dictionary<int, Gateway>();
        private static Mutex activeAccountsMutex = new Mutex();
        private static Dictionary<long, AccountProcessor> activeAccountProcessors = new Dictionary<long, AccountProcessor>();
        private static Notifier notifier = new Notifier();
        private static Listener listener = null;
        private static int bulkRequestBatchCount = 500;

        public static string ConnectionString { get { return SharedClass.connectionString; } set { SharedClass.connectionString = value; } }
        public static bool HasStopSignal { get { return SharedClass.hasStopSignal; } set { SharedClass.hasStopSignal = value; } } 
        public static bool IsServiceCleaned { get { return SharedClass.isServiceCleaned; } set { SharedClass.isServiceCleaned = value; } } 
        public static ILog Logger { get { return SharedClass.logger; } } 
        public static ILog DumpLogger { get { return SharedClass.dumpLogger; } } 
        public static ILog HeartBeatLogger { get { return SharedClass.heartBeatLogger; } } 
        public static int GatewayHeartBeatSpan { get { return SharedClass.gatewayHeartBeatSpan; } set { SharedClass.gatewayHeartBeatSpan = value; } } 
        public static bool IsHangupProcessInMemory { get { return SharedClass.isHangupProcessInMemory; } set { SharedClass.isHangupProcessInMemory = value; } } 
        public static bool IsHangupConsumerRunning { get { return SharedClass.isHangupConsumerRunning; } set { SharedClass.isHangupConsumerRunning = value; } } 
        public static bool IsCallFlowsConsumerRunning { get { return SharedClass.isCallFlowsConsumerRunning; } set { SharedClass.isCallFlowsConsumerRunning = value; } } 
        public static RabbitMQClient RabbitMQClient { get { return SharedClass.rabbitMQClient; } set { SharedClass.rabbitMQClient = value; } }
        public static HangupProcessor HangupProcessor { get { return SharedClass.hangupProcessor; } set { SharedClass.hangupProcessor = value; } } 
        public static bool IsHangupLazyProcessorRunning { get { return SharedClass.isHangupLazyProcessorRunning; } set { SharedClass.isHangupLazyProcessorRunning = value; } } 
        public static Dictionary<int, Gateway> GatewayMap { get { return SharedClass.gatewayMap; } } 
        public static Dictionary<long, AccountProcessor> ActiveAccountProcessors { get { return SharedClass.activeAccountProcessors; } }
        public static Notifier Notifier { get { return SharedClass.notifier; } }
        public static int BulkRequestBatchCount { get { return bulkRequestBatchCount; } set { bulkRequestBatchCount = value; } }
        public static Listener Listener { get { if (SharedClass.listener == null) SharedClass.listener = new Listener(); return SharedClass.listener; } set { SharedClass.listener = value; } } 
        public static void InitiaLizeLogger() {
            GlobalContext.Properties["LogName"] = DateTime.Now.ToString("yyyyMMdd");
            log4net.Config.XmlConfigurator.Configure();
            SharedClass.logger = LogManager.GetLogger("Log");
            SharedClass.dumpLogger = LogManager.GetLogger("DumpLogger");
            SharedClass.heartBeatLogger = LogManager.GetLogger("HeartBeatLogger");
        }

        public static bool AddAccountProcessor(long accountId, AccountProcessor Processor)
        {
            bool flag = false;
            SharedClass.logger.Info("Adding AccountID " + accountId.ToString() + " Into ActiveAccountProcessors");
            try
            {
                while (!SharedClass.activeAccountsMutex.WaitOne())
                    Thread.Sleep(10);
                if (!SharedClass.activeAccountProcessors.ContainsKey(accountId))
                    SharedClass.activeAccountProcessors.Add(accountId, Processor);
                flag = true;
            }
            catch (Exception ex)
            {
                SharedClass.logger.Error("Error Adding UserProcessor To Map : " + ex.Message);
            }
            finally
            {
                SharedClass.activeAccountsMutex.ReleaseMutex();
            }
            return flag;
        }

        public static bool ReleaseAccountProcessor(long accountId)
        {
            bool flag = false;
            SharedClass.logger.Info("Releasing AccountId " + accountId.ToString() + " From ActiveAccountProcessors Map");
            try
            {
                while (!SharedClass.activeAccountsMutex.WaitOne())
                    Thread.Sleep(10);
                if (SharedClass.activeAccountProcessors.ContainsKey(accountId))
                    SharedClass.activeAccountProcessors.Remove(accountId);
                flag = true;
            }
            catch (Exception ex)
            {
                SharedClass.logger.Error("Error Removing UserProcessor From Map : " + ex.Message);
            }
            finally
            {
                SharedClass.activeAccountsMutex.ReleaseMutex();
            }
            return flag;
        }

        public static bool IsAccountProcessorActive(long accountId)
        {
            bool flag = false;
            try
            {
                while (!SharedClass.activeAccountsMutex.WaitOne())
                    Thread.Sleep(10);
                if (SharedClass.activeAccountProcessors.ContainsKey(accountId))
                    flag = true;
            }
            catch (Exception ex)
            {
                SharedClass.logger.Error("Error While Chcecking ActiveAccountMap, Reason : " + ex.ToString());
            }
            finally
            {
                SharedClass.activeAccountsMutex.ReleaseMutex();
            }
            return flag;
        }

        public static long CurrentTimeStamp()
        {
            return Convert.ToInt64((DateTime.Now - new DateTime(1970, 1, 1)).TotalMilliseconds);
        }
    }
}
