using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace VoiceController
{
    [Serializable]
    public class CallsQueue
    {
        private Queue<Call> hpQ = (Queue<Call>)null;
        private Queue<Call> mpQ = (Queue<Call>)null;
        private Queue<Call> lpQ = (Queue<Call>)null;

        public CallsQueue()
        {
            this.hpQ = new Queue<Call>();
            this.mpQ = new Queue<Call>();
            this.lpQ = new Queue<Call>();
        }
        public int QueueCount(Priority.PriorityMode mode) {
            int count = 0;
            try
            {
                switch (mode)
                {
                    case Priority.PriorityMode.High:
                        lock (this.hpQ)
                        {
                            count = this.hpQ.Count;
                        }
                        break;
                    case Priority.PriorityMode.Medium:
                        lock (this.mpQ)
                        {
                            count = this.mpQ.Count;
                        }
                        break;
                    case Priority.PriorityMode.Low:
                        lock (this.lpQ)
                        {
                            count = this.lpQ.Count;
                        }
                        break;
                    default:
                        break;
                }
            }
            catch (Exception e) {
                SharedClass.Logger.Error("Error Querying Queue Count : " + e.ToString());
            }
            return count;
        }
        public void EnQueue(Call call, Priority.PriorityMode priority)
        {
            switch (priority)
            {
                case Priority.PriorityMode.High:
                    lock (this.hpQ)
                    {
                        this.hpQ.Enqueue(call);
                        break;
                    }
                case Priority.PriorityMode.Medium:
                    lock (this.mpQ)
                    {
                        this.mpQ.Enqueue(call);
                        break;
                    }
                default:
                    lock (this.lpQ)
                    {
                        this.lpQ.Enqueue(call);
                        break;
                    }
            }
        }

        public Call DeQueue()
        {
            //Call call = null;
            //try
            //{
            //    lock (this.hpQ)
            //    {
            //        if (this.hpQ.Count > 0)
            //        {
            //            call = this.hpQ.Dequeue();
            //        }
            //    }
            //    if (call == null) {
            //        lock (this.mpQ) {
            //            if (this.mpQ.Count > 0) {
            //                call = this.mpQ.Dequeue();
            //            }
            //        }
            //    }
            //    if (call == null) {
            //        lock (this.lpQ) {
            //            if (this.lpQ.Count > 0) {
            //                call = this.lpQ.Dequeue();
            //            }
            //        }
            //    }
            //}
            //catch (Exception e) { 

            //}            
            Call call = (Call)null;
            try
            {
                bool lockTaken1 = false;
                Queue<Call> queue = null;
                try
                {
                    Monitor.Enter((object)(queue = this.hpQ), ref lockTaken1);
                    if (this.hpQ.Count > 0)
                        call = this.hpQ.Dequeue();
                }
                finally
                {
                    if (lockTaken1)
                        Monitor.Exit((object)queue);
                }
                if (call == null)
                {
                    bool lockTaken2 = false;
                    try
                    {
                        Monitor.Enter((object)(queue = this.mpQ), ref lockTaken2);
                        if (this.mpQ.Count > 0)
                            call = this.mpQ.Dequeue();
                    }
                    finally
                    {
                        if (lockTaken2)
                            Monitor.Exit((object)queue);
                    }
                }
                if (call == null)
                {
                    bool lockTaken2 = false;
                    try
                    {
                        Monitor.Enter((object)(queue = this.lpQ), ref lockTaken2);
                        if (this.lpQ.Count > 0)
                            call = this.lpQ.Dequeue();
                    }
                    finally
                    {
                        if (lockTaken2)
                            Monitor.Exit((object)queue);
                    }
                }
            }
            catch (Exception ex)
            {
                SharedClass.Logger.Error(ex);
            }
            return call;
        }
    }
}
