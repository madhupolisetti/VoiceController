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
        private Queue<Call> upQ = null;
        private Queue<Call> hpQ = null;
        private Queue<Call> mpQ = null;
        private Queue<Call> lpQ = null;

        public CallsQueue()
        {
            this.upQ = new Queue<Call>();
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
                    case Priority.PriorityMode.Urgent:
                        lock (this.upQ) {
                            count = this.upQ.Count;
                        }
                        break;
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
                case Priority.PriorityMode.Urgent:
                    lock (this.upQ) {
                        this.upQ.Enqueue(call);
                    }
                    break;
                case Priority.PriorityMode.High:
                    lock (this.hpQ)
                    {
                        this.hpQ.Enqueue(call);                        
                    }
                    break;
                case Priority.PriorityMode.Medium:
                    lock (this.mpQ)
                    {
                        this.mpQ.Enqueue(call);                        
                    }
                    break;
                default:
                    lock (this.lpQ)
                    {
                        this.lpQ.Enqueue(call);                        
                    }
                    break;
            }
            SharedClass.Logger.Info("Enqueuing the call of Id : " + call.QueueTableSlno.ToString());
        }

        public Call DeQueue()
        {
            Call call = null;
            try
            {
                if (this.QueueCount(Priority.PriorityMode.Urgent) > 0) {
                    lock (this.upQ) {
                        call = this.upQ.Dequeue();
                    }
                }
                else if (this.QueueCount(Priority.PriorityMode.High) > 0) {
                    lock (this.hpQ) {
                        call = this.hpQ.Dequeue();
                    }
                }
                else if (this.QueueCount(Priority.PriorityMode.Medium) > 0) {
                    lock (this.mpQ) {
                        call = this.mpQ.Dequeue();
                    }
                }
                else if (this.QueueCount(Priority.PriorityMode.Low) > 0) {
                    lock(this.lpQ)
                    {
                        call = this.lpQ.Dequeue();
                    }
                    
                }
            }
            catch (Exception e) {
                SharedClass.Logger.Error("Error DeQueuing Call : " + e.ToString());
            }            
            return call;
        }
    }
}
