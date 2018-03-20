using Orleans;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace Tester.Grains
{
    public interface ISampleGrain: IGrainWithGuidKey
    {

        Task<string> GetLatestMessage();
    }
}
