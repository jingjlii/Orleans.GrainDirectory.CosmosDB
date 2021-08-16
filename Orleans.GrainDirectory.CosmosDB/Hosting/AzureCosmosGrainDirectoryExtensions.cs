using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Orleans.Configuration;
using Orleans.GrainDirectory;
using Orleans.GrainDirectory.CosmosDB;
using Orleans.Runtime;

namespace Orleans.Hosting
{
    public static class AzureCosmosGrainDirectoryExtensions
    {
        public static ISiloHostBuilder UseAzureCosmosGrainDirectoryAsDefault(
           this ISiloHostBuilder builder,
           Action<AzureCosmosGrainDirectoryOptions> configureOptions)
        {
            return builder.UseAzureCosmosGrainDirectoryAsDefault(ob => ob.Configure(configureOptions));
        }

        public static ISiloHostBuilder UseAzureCosmosGrainDirectoryAsDefault(
            this ISiloHostBuilder builder,
            Action<OptionsBuilder<AzureCosmosGrainDirectoryOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddAzureCosmosGrainDirectory(GrainDirectoryAttribute.DEFAULT_GRAIN_DIRECTORY, configureOptions));
        }

        public static ISiloHostBuilder AddAzureCosmosGrainDirectory(
            this ISiloHostBuilder builder,
            string name,
            Action<AzureCosmosGrainDirectoryOptions> configureOptions)
        {
            return builder.AddAzureCosmosGrainDirectory(name, ob => ob.Configure(configureOptions));
        }

        public static ISiloHostBuilder AddAzureCosmosGrainDirectory(
            this ISiloHostBuilder builder,
            string name,
            Action<OptionsBuilder<AzureCosmosGrainDirectoryOptions>> configureOptions)
        {
            return builder.ConfigureServices(services => services.AddAzureCosmosGrainDirectory(name, configureOptions));
        }

        private static IServiceCollection AddAzureCosmosGrainDirectory(
            this IServiceCollection services,
            string name,
            Action<OptionsBuilder<AzureCosmosGrainDirectoryOptions>> configureOptions)
        {
            configureOptions.Invoke(services.AddOptions<AzureCosmosGrainDirectoryOptions>(name));
            services
                .AddTransient<IConfigurationValidator>(sp => new AzureCosmosGrainDirectoryOptionsValidator(sp.GetRequiredService<IOptionsMonitor<AzureCosmosGrainDirectoryOptions>>().Get(name)))
                .ConfigureNamedOptionForLogging<AzureCosmosGrainDirectoryOptions>(name)
                .AddSingletonNamedService<IGrainDirectory>(name, (sp, name) => ActivatorUtilities.CreateInstance<AzureCosmosGrainDirectory>(sp, sp.GetOptionsByName<AzureCosmosGrainDirectoryOptions>(name)))
                .AddSingletonNamedService<ILifecycleParticipant<ISiloLifecycle>>(name, (s, n) => (ILifecycleParticipant<ISiloLifecycle>)s.GetRequiredServiceByName<IGrainDirectory>(n));

            return services;
        }
    }
}
