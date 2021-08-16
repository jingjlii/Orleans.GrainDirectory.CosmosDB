using Microsoft.Azure.Cosmos;
using System;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using System.Net;
using System.Collections.Generic;

namespace Orleans.GrainDirectory.CosmosDB.Storage
{
    public class AzureCosmosDbManager
    {
        private CosmosClient cosmosClient;

        public Database database { get; private set; }

        public Container container { get; private set; }

        protected internal ILogger Logger { get; }

        public AzureCosmosDbManager(string endpointUri, string primaryKey, ILogger logger)
        {
            cosmosClient = new CosmosClient(endpointUri, primaryKey, new CosmosClientOptions()
            {
                ConnectionMode = ConnectionMode.Gateway
            });

            Logger = logger ?? throw new ArgumentNullException(nameof(logger));
        }

        public async Task InitContainerAsync(string databaseId, string containerId)
        {
            try
            {
                database = await cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
                container = await database.CreateContainerIfNotExistsAsync(containerId, "/ClusterId");
            }
            catch (Exception exc)
            {
                Logger.LogError($"Could not initialize connection to database {databaseId}", exc);
                throw;
            }          
        }

        public async Task<GrainDirectoryItem> AddItemsToContainerAsync(GrainDirectoryItem item)
        {
            try
            {
                // Read the item to see if it exists.  
                ItemResponse<GrainDirectoryItem> grainDirectoryResponse = await this.container.ReadItemAsync<GrainDirectoryItem>(item.Id, new PartitionKey(item.ClusterId));
                Logger.LogInformation("Item in database with id: {0} already exists\n", grainDirectoryResponse.Resource.Id);
                return grainDirectoryResponse.Resource;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                // Create an item in the container representing the grain. Note we provide the value of the partition key for this item, which is clusterId
                var grainDirectoryResponse = await this.container.CreateItemAsync(item, new PartitionKey(item.ClusterId));

                // Note that after creating the item, we can access the body of the item with the Resource property off the ItemResponse. We can also access the RequestCharge property to see the amount of RUs consumed on this request.
                Logger.LogInformation("Created item in database with id: {0} Operation consumed {1} RUs.\n", grainDirectoryResponse.Resource.Id, grainDirectoryResponse.RequestCharge);
                return grainDirectoryResponse.Resource;
            }
           
        }

        public async Task<GrainDirectoryItem> QueryItemAsync(string id, string clusterId)
        {
            try
            {
                var grainDiretoryResponse = await this.container.ReadItemAsync<GrainDirectoryItem>(id, new PartitionKey(clusterId));
                return grainDiretoryResponse.Resource;
            }
            catch (CosmosException ex) when (ex.StatusCode == HttpStatusCode.NotFound)
            {
                return null;
            }
        }

        public async Task<GrainDirectoryItem> DeleteItemAsync(string id, string clusterId)
        {
            var grainDirectoryResponse = await this.container.DeleteItemAsync<GrainDirectoryItem>(id, new PartitionKey(clusterId));
            return grainDirectoryResponse.Resource;
        }

        public async Task DeleteItemsAsync(string queryString, string partitionKey)
        {
            using (var iterator = container.GetItemQueryIterator<GrainDirectoryItem>(queryString, null, new QueryRequestOptions() { PartitionKey = new PartitionKey(partitionKey)}))
            {
                while (iterator.HasMoreResults)
                {
                    try
                    {
                        var item1 = await iterator.ReadNextAsync();
                        var result1 = item1.Resource;
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine(e.Message);
                    }
                  
                    foreach (var item in await iterator.ReadNextAsync())
                    {
                        var response = await container.DeleteItemAsync<GrainDirectoryItem>(item.Id, new PartitionKey(item.ClusterId));
                        Console.WriteLine(response.StatusCode);
                    }
                }
            }
        }
    }
}
