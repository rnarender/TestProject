using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace WKCompressor
{
    class CurrentInstanceData
    {
        public long LastInstanceID { get; set; }
        public String FailedIntanceIDs { get; set; }
        public bool IsFinished { get; set; }
    }
}
