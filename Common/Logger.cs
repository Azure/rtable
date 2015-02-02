//-----------------------------------------------------------------------
// <copyright file="Logger.cs" company="Microsoft">
//    Copyright 2013 Microsoft Corporation
//
//    Licensed under the Apache License, Version 2.0 (the "License");
//    you may not use this file except in compliance with the License.
//    You may obtain a copy of the License at
//      http://www.apache.org/licenses/LICENSE-2.0
//
//    Unless required by applicable law or agreed to in writing, software
//    distributed under the License is distributed on an "AS IS" BASIS,
//    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//    See the License for the specific language governing permissions and
//    limitations under the License.
// </copyright>
//-----------------------------------------------------------------------

namespace Microsoft.WindowsAzure.Storage.RTable
{
    public static class Logger
    {
        private static RTableEventSource eventSource = new RTableEventSource();

        public static void LogError(string format, params object[] args)
        {
            if (Logger.eventSource.IsEnabled())
            {
                Logger.eventSource.Error(string.Format(format, args));
            }
        }

        public static void LogWarning(string format, params object[] args)
        {
            if (Logger.eventSource.IsEnabled())
            {
                Logger.eventSource.Warning(string.Format(format, args));
            }
        }

        public static void LogInformational(string format, params object[] args)
        {
            if (Logger.eventSource.IsEnabled())
            {
                Logger.eventSource.Informational(string.Format(format, args));
            }
        }

        public static void LogVerbose(string format, params object[] args)
        {
            if (Logger.eventSource.IsEnabled())
            {
                Logger.eventSource.Verbose(string.Format(format, args));
            }
        }
    }
}
