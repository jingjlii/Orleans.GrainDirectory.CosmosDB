using System;
using System.Collections.Generic;
using System.Text;
using Orleans.GrainDirectory.CosmosDB;
using Orleans.Runtime;

namespace Orleans.Configuration
{
    public class AzureCosmosGrainDirectoryOptions
    {
        public string ContainerId { get; set; } = DEFAULT_CONTAINER_ID;
        public const string DEFAULT_CONTAINER_ID = "GrainDirectoryContainer";

        public string DataBaseId { get; set; } = DEFAULT_DATABASE_ID;
        public const string DEFAULT_DATABASE_ID = "GrainDirectoryDatabase";

        public string PrimaryKey { get; set; }
        public string EndpointUri { get; set; }
    }

    public class AzureCosmosGrainDirectoryOptionsValidator : IConfigurationValidator
    {
        private readonly AzureCosmosGrainDirectoryOptions options;

        public AzureCosmosGrainDirectoryOptionsValidator(AzureCosmosGrainDirectoryOptions options)
        {
            this.options = options;
        }

        public void ValidateConfiguration()
        {
            if (string.IsNullOrEmpty(options.EndpointUri))
            {
                throw GetException($"{nameof(options.EndpointUri)} is required.");
            }

            if (string.IsNullOrEmpty(options.PrimaryKey))
            {
                throw GetException($"{nameof(options.PrimaryKey)} is required.");
            }

            Exception GetException(string message, Exception inner = null) =>
               new OrleansConfigurationException($"Configuration for {GetType().Name} AzureCosmosDBConfiguration is invalid. {message}", inner);
        }
    }
}
