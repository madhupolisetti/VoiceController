using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VoiceController
{
    public static class CallsQueueSlno
    {
        private static Dictionary<int, Dictionary<Environment, Dictionary<Priority.PriorityMode, long>>> _gatewayEnvironmentPriorityModeSlnoDictionary = new Dictionary<int, Dictionary<Environment, Dictionary<Priority.PriorityMode, long>>>();
        public static void SetSlno(int gatewayId, Environment environment, Priority.PriorityMode priorityMode, long slno)
        {
            lock (_gatewayEnvironmentPriorityModeSlnoDictionary)
            {
                if (!_gatewayEnvironmentPriorityModeSlnoDictionary.ContainsKey(gatewayId))
                {
                    Dictionary<Priority.PriorityMode, long> priorityModeSlnoDictionary = new Dictionary<Priority.PriorityMode, long>();
                    priorityModeSlnoDictionary.Add(priorityMode, slno);
                    Dictionary<Environment, Dictionary<Priority.PriorityMode, long>> environmentPriorityModeSlnoDictionary = new Dictionary<Environment, Dictionary<Priority.PriorityMode, long>>();
                    environmentPriorityModeSlnoDictionary.Add(environment, priorityModeSlnoDictionary);
                    _gatewayEnvironmentPriorityModeSlnoDictionary.Add(gatewayId, environmentPriorityModeSlnoDictionary);
                }
                else if (!_gatewayEnvironmentPriorityModeSlnoDictionary[gatewayId].ContainsKey(environment))
                {
                    Dictionary<Priority.PriorityMode, long> priorityModeSlnoDictionary = new Dictionary<Priority.PriorityMode, long>();
                    priorityModeSlnoDictionary.Add(priorityMode, slno);
                    _gatewayEnvironmentPriorityModeSlnoDictionary[gatewayId].Add(environment, priorityModeSlnoDictionary);
                }
                else if (!_gatewayEnvironmentPriorityModeSlnoDictionary[gatewayId][environment].ContainsKey(priorityMode))
                {
                    _gatewayEnvironmentPriorityModeSlnoDictionary[gatewayId][environment].Add(priorityMode, slno);
                }
                else
                {
                    _gatewayEnvironmentPriorityModeSlnoDictionary[gatewayId][environment][priorityMode] = slno;
                }
            }
        }
        public static long GetSlno(int gatewayId, Environment environment, Priority.PriorityMode priorityMode)
        {
            long slno = 0;
            bool setFlag = false;
            lock (_gatewayEnvironmentPriorityModeSlnoDictionary)
            {
                if (_gatewayEnvironmentPriorityModeSlnoDictionary.ContainsKey(gatewayId) && _gatewayEnvironmentPriorityModeSlnoDictionary[gatewayId].ContainsKey(environment) && _gatewayEnvironmentPriorityModeSlnoDictionary[gatewayId][environment].ContainsKey(priorityMode))
                    slno = _gatewayEnvironmentPriorityModeSlnoDictionary[gatewayId][environment][priorityMode];
                else
                    setFlag = true;
            }
            if (setFlag)
                SetSlno(gatewayId, environment, priorityMode, slno);
            return slno;
        }


    }
}
