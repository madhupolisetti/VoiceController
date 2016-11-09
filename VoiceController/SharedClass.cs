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
        #region MEMBER VARIABLES PRIVATE
        private static Dictionary<Environment, string> _connectionStrings = new Dictionary<Environment, string>();
        private static bool _pollStaging = false;
        private static bool _hasStopSignal = false;
        private static bool _isServiceCleaned = true;
        private static int _gatewayHeartBeatSpan = 30;
        private static ILog _logger = null;
        private static ILog _dumpLogger = null;
        private static ILog _heartBeatLogger = null;
        private static bool _isHangupProcessInMemory = false;
        private static RabbitMQClient _rabbitMQClient = null;
        private static HangupProcessor _hangupProcessor = null;
        private static bool _isHangupConsumerRunning = false;
        private static bool _isCallFlowsConsumerRunning = false;
        private static bool _isCallBacksConsumerRunning = false;
        private static bool _isHangupLazyProcessorRunning = false;
        private static Dictionary<int, Gateway> _gatewayMap = new Dictionary<int, Gateway>();
        private static Mutex _activeAccountsMutex = new Mutex();
        private static Dictionary<int, AccountProcessor> _activeAccountProcessors = new Dictionary<int, AccountProcessor>();
        private static Notifier _notifier = new Notifier();
        private static Listener _listener = null;
        private static int _bulkRequestBatchCount = 500;
        #endregion

        #region PUBLIC METHODS
        public static void InitiaLizeLogger()
        {
            GlobalContext.Properties["LogName"] = DateTime.Now.ToString("yyyyMMdd");
            log4net.Config.XmlConfigurator.Configure();
            _logger = LogManager.GetLogger("Log");
            _dumpLogger = LogManager.GetLogger("DumpLogger");
            _heartBeatLogger = LogManager.GetLogger("HeartBeatLogger");
        }
        public static void SetConnectionString(string connectionString, Environment environment)
        {
            if (_connectionStrings.ContainsKey(environment))
                _connectionStrings[environment] = connectionString;
            else
                _connectionStrings.Add(environment, connectionString);
        }
        public static string GetConnectionString(Environment environment)
        {
            if (!_connectionStrings.ContainsKey(environment))
                throw new KeyNotFoundException("Key " + environment.ToString() + " Not Found in the dictionary");
            else
                return _connectionStrings[environment];
        }

        public static bool AddAccountProcessor(int accountId, AccountProcessor processor)
        {
            bool flag = false;
            _logger.Info("Adding AccountID " + accountId.ToString() + " Into ActiveAccountProcessors");
            try
            {
                while (!_activeAccountsMutex.WaitOne())
                    Thread.Sleep(10);
                if (!_activeAccountProcessors.ContainsKey(accountId))
                    _activeAccountProcessors.Add(accountId, processor);
                flag = true;
            }
            catch (Exception ex)
            {
                _logger.Error("Error Adding UserProcessor To Map : " + ex.Message);
            }
            finally
            {
                _activeAccountsMutex.ReleaseMutex();
            }
            return flag;
        }

        public static bool ReleaseAccountProcessor(int accountId)
        {
            bool flag = false;
            _logger.Info("Releasing AccountId " + accountId.ToString() + " From ActiveAccountProcessors Map");
            try
            {
                while (!_activeAccountsMutex.WaitOne())
                    Thread.Sleep(10);
                if (_activeAccountProcessors.ContainsKey(accountId))
                    _activeAccountProcessors.Remove(accountId);
                flag = true;
            }
            catch (Exception ex)
            {
                _logger.Error("Error Removing UserProcessor From Map : " + ex.Message);
            }
            finally
            {
                _activeAccountsMutex.ReleaseMutex();
            }
            return flag;
        }

        public static bool IsAccountProcessorActive(int accountId)
        {
            bool isActive = false;
            try
            {
                while (!_activeAccountsMutex.WaitOne())
                    Thread.Sleep(10);
                if (_activeAccountProcessors.ContainsKey(accountId))
                    isActive = true;
            }
            catch (Exception ex)
            {
                _logger.Error("Error While Chcecking ActiveAccountMap, Reason : " + ex.ToString());
            }
            finally
            {
                _activeAccountsMutex.ReleaseMutex();
            }
            return isActive;
        }

        public static long CurrentTimeStamp()
        {
            return Convert.ToInt64((DateTime.Now - new DateTime(1970, 1, 1)).TotalMilliseconds);
        }
        #endregion        
        #region PROPERTIES
        //public static string ConnectionString
        //{
        //    get
        //    {
        //        return SharedClass._connectionString;
        //    }
        //    set
        //    {
        //        SharedClass._connectionString = value;
        //    }
        //}
        public static bool PollStaging
        {
            get { return _pollStaging; }
            set { _pollStaging = value; }
        }
        public static bool HasStopSignal
        {
            get
            {
                return SharedClass._hasStopSignal;
            }
            set
            {
                SharedClass._hasStopSignal = value;
            }
        }
        public static bool IsServiceCleaned
        {
            get
            {
                return SharedClass._isServiceCleaned;
            }
            set
            {
                SharedClass._isServiceCleaned = value;
            }
        }
        public static ILog Logger
        {
            get
            {
                return SharedClass._logger;
            }
        }
        public static ILog DumpLogger
        {
            get
            {
                return SharedClass._dumpLogger;
            }
        }
        public static ILog HeartBeatLogger
        {
            get
            {
                return SharedClass._heartBeatLogger;
            }
        }
        public static int GatewayHeartBeatSpan
        {
            get
            {
                return
                    SharedClass._gatewayHeartBeatSpan;
            }
            set
            {
                SharedClass._gatewayHeartBeatSpan = value;
            }
        }
        public static bool IsHangupProcessInMemory
        {
            get
            {
                return SharedClass._isHangupProcessInMemory;
            }
            set
            {
                SharedClass._isHangupProcessInMemory = value;
            }
        }
        public static bool IsHangupConsumerRunning
        {
            get
            {
                return SharedClass._isHangupConsumerRunning;
            }
            set
            {
                SharedClass._isHangupConsumerRunning = value;
            }
        }
        public static bool IsCallFlowsConsumerRunning
        {
            get
            {
                return SharedClass._isCallFlowsConsumerRunning;
            }
            set
            {
                SharedClass._isCallFlowsConsumerRunning = value;
            }
        }
        public static bool IsCallBacksConsumerRunning
        {
            get
            {
                return SharedClass._isCallBacksConsumerRunning;
            }
            set
            {
                SharedClass._isCallBacksConsumerRunning = value;
            }
        }

        public static RabbitMQClient RabbitMQClient
        {
            get
            {
                return SharedClass._rabbitMQClient;
            }
            set
            {
                SharedClass._rabbitMQClient = value;
            }
        }
        public static HangupProcessor HangupProcessor
        {
            get
            {
                return SharedClass._hangupProcessor;
            }
            set
            {
                SharedClass._hangupProcessor = value;
            }
        }
        public static bool IsHangupLazyProcessorRunning
        {
            get
            {
                return SharedClass._isHangupLazyProcessorRunning;
            }
            set
            {
                SharedClass._isHangupLazyProcessorRunning = value;
            }
        }
        public static Dictionary<int, Gateway> GatewayMap
        {
            get
            {
                return SharedClass._gatewayMap;
            }
        }
        public static Dictionary<int, AccountProcessor> ActiveAccountProcessors
        {
            get
            {
                return SharedClass._activeAccountProcessors;
            }
        }
        public static Notifier Notifier
        {
            get
            {
                return SharedClass._notifier;
            }
        }
        public static int BulkRequestBatchCount
        {
            get
            {
                return _bulkRequestBatchCount;
            }
            set
            {
                _bulkRequestBatchCount = value;
            }
        }
        public static Listener Listener
        {
            get
            {
                if (SharedClass._listener == null)
                    SharedClass._listener = new Listener();
                return SharedClass._listener;
            }
            set
            {
                SharedClass._listener = value;
            }
        }
        #endregion
    }
}
