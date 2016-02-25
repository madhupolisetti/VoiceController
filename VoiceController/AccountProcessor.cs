using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace VoiceController
{
    public class AccountProcessor
    {
        private long accountId = 0L;
        private Queue<BulkRequest> bulkRequestsQueue = new Queue<BulkRequest>();
        private Mutex queueMutex = new Mutex();
        private bool shouldIProcess = false;
        private short maxThreads = (short)1;
        private short activeThreads = (short)0;

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

        public short MaxThreads
        {
            get
            {
                return this.maxThreads;
            }
            set
            {
                this.maxThreads = value;
            }
        }

        public short ActiveThreads
        {
            get
            {
                return this.activeThreads;
            }
            set
            {
                this.activeThreads = value;
            }
        }

        public void Start()
        {
            SharedClass.Logger.Info("Started");
            while (this.shouldIProcess && !SharedClass.HasStopSignal)
            {
                if (this.QueueCount() > 0 && (int)this.ActiveThreads < (int)this.maxThreads)
                {
                    BulkRequest bulkRequest = this.DeQueue();
                    if (bulkRequest != null)
                    {
                        Thread thread = new Thread(new ParameterizedThreadStart(this.StartBulkProcess));
                        SharedClass.Logger.Info("Spawning New Thread For BulkRequest Id : " + (object)bulkRequest.Id);
                        thread.Start(bulkRequest);
                    }
                }
                Thread.Sleep(2000);
                if ((int)this.ActiveThreads == 0 && this.QueueCount() == 0)
                    this.Stop();
            }
        }

        public void Stop()
        {
            SharedClass.Logger.Info("Stopping AccountId : " + this.AccountId + " Processor");
            this.shouldIProcess = false;
            while (this.QueueCount() > 0)
            {
                //if (this.DeQueue() == null)
                //    ;
            }
            while ((int)this.ActiveThreads > 0)
            {
                SharedClass.Logger.Info("AccountId " + this.AccountId + " has still " + this.ActiveThreads + " active process threads running");
                Thread.Sleep(1000);
            }
            SharedClass.ReleaseAccountProcessor(this.AccountId);
        }

        public void StartBulkProcess(object input)
        {
            SharedClass.Logger.Info("Started Processing BulkRequest " + (input as BulkRequest).DisplayString());
            ++this.ActiveThreads;
            --this.ActiveThreads;
        }

        public bool EnQueue(BulkRequest bulkRequest)
        {
            SharedClass.Logger.Info("EnQueuing BulkRequest " + (object)bulkRequest.Id + " Into AccountId " + (string)(object)this.accountId + " Processor");
            bool flag = false;
            try
            {
                while (!this.queueMutex.WaitOne())
                    Thread.Sleep(200);
                this.bulkRequestsQueue.Enqueue(bulkRequest);
                flag = true;
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error("Error EnQueuing BulkRequest Id : " + bulkRequest.Id + " Into AccountId : " + this.accountId + " Processor. Reason : " + ex.ToString());
            }
            finally
            {
                this.queueMutex.ReleaseMutex();
            }
            return flag;
        }

        private int QueueCount()
        {
            int num = 0;
            try
            {
                while (!this.queueMutex.WaitOne())
                    Thread.Sleep(200);
                num = this.bulkRequestsQueue.Count;
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error((object)string.Concat(new object[4]
        {
          (object) "Error Querying Count In AccountId : ",
          (object) this.accountId,
          (object) " Processor. Reason : ",
          (object) ex.ToString()
        }));
            }
            finally
            {
                this.queueMutex.ReleaseMutex();
            }
            return num;
        }

        private BulkRequest DeQueue()
        {
            BulkRequest bulkRequest = null;
            try
            {
                while (!this.queueMutex.WaitOne())
                    Thread.Sleep(200);
                bulkRequest = this.bulkRequestsQueue.Dequeue();
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error((object)string.Concat(new object[4]
        {
          (object) "Error DeQueuing In AccountId : ",
          (object) this.accountId,
          (object) " Processor. Reason : ",
          (object) ex.ToString()
        }));
            }
            finally
            {
                this.queueMutex.ReleaseMutex();
            }
            return bulkRequest;
        }
    }
}
