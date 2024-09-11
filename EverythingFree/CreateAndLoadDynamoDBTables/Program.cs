
using Amazon;
using Amazon.DynamoDBv2;
using Amazon.Runtime;
using DataLayer;
using DataLayer.Interfaces;
using Microsoft.Extensions.DependencyInjection;

namespace CreateAndLoadDynamoDBTables
{
    public class Program
    {
        public static void Main(string[] args)
        {
            var builder = WebApplication.CreateBuilder(args);

            // Add services to the container.

            builder.Services.AddControllers();
            // Learn more about configuring Swagger/OpenAPI at https://aka.ms/aspnetcore/swashbuckle
            builder.Services.AddEndpointsApiExplorer();
            builder.Services.AddSwaggerGen();

            builder.Services.AddSingleton<IAmazonDynamoDB>(sp =>
            {
                var creds = new BasicAWSCredentials("dummy","dummy");
                var clientConfig = new AmazonDynamoDBConfig { 

                    ServiceURL = "http://localhost:8000", 
                    RegionEndpoint = RegionEndpoint.USEast1
                };

                return new AmazonDynamoDBClient(clientConfig);
            });           

            builder.Services.AddScoped<ICreateTablesLoadData, CreateTablesLoadData>();

            var app = builder.Build();

            // Configure the HTTP request pipeline.
            if (app.Environment.IsDevelopment())
            {
                app.UseSwagger();
                app.UseSwaggerUI();
            }

            app.UseHttpsRedirection();

            app.UseAuthorization();


            app.MapControllers();

            app.Run();
        }
    }
}
