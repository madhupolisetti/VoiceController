using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public class Priority
    {
        private static sbyte hpFloor = (sbyte)0;
        private static sbyte hpCeil = (sbyte)0;
        private static sbyte mpFloor = (sbyte)0;
        private static sbyte mpCeil = (sbyte)0;
        private static sbyte lpFloor = (sbyte)0;
        private static sbyte lpCeil = (sbyte)0;

        public sbyte HpFloor
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

        public sbyte HpCeil
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

        public sbyte MpFloor
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

        public sbyte MpCeil
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

        public sbyte LpFloor
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

        public sbyte LpCeil
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
            if ((int)value <= (int)Priority.hpCeil)
                return Priority.PriorityMode.High;
            return (int)value <= (int)Priority.mpCeil ? Priority.PriorityMode.Medium : Priority.PriorityMode.Low;
        }

        public enum PriorityMode
        {
            High,
            Medium,
            Low,
        }
    }
}
