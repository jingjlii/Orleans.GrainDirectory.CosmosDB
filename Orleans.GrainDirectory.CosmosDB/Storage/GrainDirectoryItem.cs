using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orleans.GrainDirectory.CosmosDB.Storage
{
    public class GrainDirectoryItem
    {
        [JsonProperty(PropertyName = "id")]
        public string Id { get; set; }//clusterId-grainId

        public string ClusterId { get; set; }

        public string GrainId { get; set; }

        public string SiloAddress { get; set; }

        public string ActivationId { get; set; }

        public GrainAddress ToGrainAddress()
        {
            return new GrainAddress
            {
                GrainId = this.GrainId,
                SiloAddress = this.SiloAddress,
                ActivationId = this.ActivationId,
            };
        }

        public static GrainDirectoryItem FromGrainAddress(string clusterId, GrainAddress address)
        {
            return new GrainDirectoryItem
            {
                Id = $"{clusterId}-{address.GrainId}",
                ClusterId = clusterId,
                GrainId = address.GrainId,
                SiloAddress = address.SiloAddress,
                ActivationId = address.ActivationId
            };
        }
        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
