using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public static class Priority
    {
        private static byte hpFloor = 0;
        private static byte hpCeil = 0;
        private static byte mpFloor = 0;
        private static byte mpCeil = 0;
        private static byte lpFloor = 0;
        private static byte lpCeil = 0;

        public static byte HpFloor
        {
            get
            {
                return Priority.hpFloor;
            }
            set
            {
                Priority.hpFloor = value;
            }
        }

        public static byte HpCeil
        {
            get
            {
                return Priority.hpCeil;
            }
            set
            {
                Priority.hpCeil = value;
            }
        }

        public static byte MpFloor
        {
            get
            {
                return Priority.mpFloor;
            }
            set
            {
                Priority.mpFloor = value;
            }
        }

        public static byte MpCeil
        {
            get
            {
                return Priority.mpCeil;
            }
            set
            {
                Priority.mpCeil = value;
            }
        }

        public static byte LpFloor
        {
            get
            {
                return Priority.lpFloor;
            }
            set
            {
                Priority.lpFloor = value;
            }
        }

        public static byte LpCeil
        {
            get
            {
                return Priority.lpCeil;
            }
            set
            {
                Priority.lpCeil = value;
            }
        }

        public static Priority.PriorityMode GetPriority(byte value)
        {
            if (value == 0) {
                return PriorityMode.Urgent;
            }
            if (value <= Priority.hpCeil && value >= Priority.hpFloor)
            {
                return PriorityMode.High;
            }
            else if (value <= Priority.mpCeil && value >= Priority.mpFloor)
            {
                return PriorityMode.Urgent;
            }
            else {
                return PriorityMode.Low;
            }
        }

        public enum PriorityMode
        {
            Urgent,
            High,
            Medium,
            Low,
        }
    }
}
