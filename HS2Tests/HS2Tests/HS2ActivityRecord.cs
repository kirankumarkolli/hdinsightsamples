using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HS2Tests
{
    public enum HS2ActivityState
    {
        NOINIT,
        SUCCESS,
        FAIL,
    }

    public enum HS2ActivityName
    {
        OpenSession,
        ExecuteQueryOnly,
        FetchResults,
    }

    public class HS2ActivityRecord
    {
        public DateTimeOffset StartTime { get; set; }
        public DateTimeOffset EndTime { get; set; }
        public HS2ActivityName Name { get; set; }
        public string SessionName { get; set; }
        public HS2ActivityState Status { get; set; }
        public int ResultSetCount { get; set; }
    }
}
