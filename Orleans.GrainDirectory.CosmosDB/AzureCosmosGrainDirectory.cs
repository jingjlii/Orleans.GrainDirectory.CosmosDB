using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.GrainDirectory.CosmosDB.Storage;
using Orleans.Runtime;

namespace Orleans.GrainDirectory.CosmosDB
{
    public class AzureCosmosGrainDirectory : IGrainDirectory, ILifecycleParticipant<ISiloLifecycle>
    {
        private readonly AzureCosmosGrainDirectoryOptions directoryOptions;
        private readonly string clusterId;
        private readonly ILogger<AzureCosmosGrainDirectory> logger;
        private readonly AzureCosmosDbManager cosmosDbManager;

        public AzureCosmosGrainDirectory(
            AzureCosmosGrainDirectoryOptions directoryOptions,
            IOptions<ClusterOptions> clusterOptions,
            ILoggerFactory loggerFactory)
        {
            this.cosmosDbManager = new AzureCosmosDbManager(
                directoryOptions.EndpointUri,
                directoryOptions.PrimaryKey,
                loggerFactory.CreateLogger<AzureCosmosDbManager>());
            this.directoryOptions = directoryOptions;
            this.logger = loggerFactory.CreateLogger<AzureCosmosGrainDirectory>();
            this.clusterId = clusterOptions.Value.ClusterId;
        }

        public async Task<GrainAddress> Lookup(string grainId)
        {
            var result = await cosmosDbManager.QueryItemAsync(GetItemId(grainId), clusterId);
            if (result == null)
                return null;

            return result.ToGrainAddress();
        }

        public async Task<GrainAddress> Register(GrainAddress address)
        {
            var item = GrainDirectoryItem.FromGrainAddress(clusterId, address);
            var result = await cosmosDbManager.AddItemsToContainerAsync(item);
            return result.ToGrainAddress();
        }

        public async Task Unregister(GrainAddress address)
        {
            var result = await this.cosmosDbManager.QueryItemAsync(GetItemId(address.GrainId), clusterId);

            // No item found
            if (result == null)
                return;

            // Check if the entry in storage match the one we were asked to delete
            if (result.ActivationId == address.ActivationId)
                await this.cosmosDbManager.DeleteItemAsync(GetItemId(address.GrainId), clusterId);
        }

        public async Task UnregisterSilos(List<string> siloAddresses)
        {
          foreach (var address in siloAddresses)
          {
              var queryString = $"select * from {directoryOptions.ContainerId} c where c.SiloAddress = '{address}'";
              await cosmosDbManager.DeleteItemsAsync(queryString, clusterId);
          }
        }
        // Called by lifecycle, should not be called explicitely, except for tests
        public async Task InitializeIfNeeded(CancellationToken ct = default)
        {
            await cosmosDbManager.InitContainerAsync(directoryOptions.DataBaseId, directoryOptions.ContainerId);
        }

        public void Participate(ISiloLifecycle lifecycle)
        {

            lifecycle.Subscribe(nameof(AzureCosmosGrainDirectory), ServiceLifecycleStage.RuntimeInitialize, InitializeIfNeeded);
        }

        private string GetItemId(string grainId) => $"{clusterId}-{grainId}";
    }
}
