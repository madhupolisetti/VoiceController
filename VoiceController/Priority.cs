using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public static class Priority
    {
        private static sbyte hpFloor = 0;
        private static sbyte hpCeil = 0;
        private static sbyte mpFloor = 0;
        private static sbyte mpCeil = 0;
        private static sbyte lpFloor = 0;
        private static sbyte lpCeil = 0;

        public static sbyte HpFloor
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

        public static sbyte HpCeil
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

        public static sbyte MpFloor
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

        public static sbyte MpCeil
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

        public static sbyte LpFloor
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

        public static sbyte LpCeil
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

        public static Priority.PriorityMode GetPriority(sbyte value)
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
