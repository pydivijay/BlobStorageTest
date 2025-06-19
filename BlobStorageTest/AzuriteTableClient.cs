using Azure;
using Azure.Data.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BlobStorageTest
{
    public class AzuriteTableClient
    {
        private readonly TableServiceClient _tableServiceClient;
        private const string ConnectionString =
            "DefaultEndpointsProtocol=http;" +
            "AccountName=devstoreaccount1;" +
            "AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;" +
            "TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;";

        public AzuriteTableClient()
        {
            _tableServiceClient = new TableServiceClient(ConnectionString);
        }

        public async Task<TableClient> CreateTableAsync(string tableName)
        {
            try
            {
                await _tableServiceClient.CreateTableAsync(tableName);
                Console.WriteLine($"Table '{tableName}' created successfully");
                return _tableServiceClient.GetTableClient(tableName);
            }
            catch (Exception ex) when (ex.Message.Contains("TableAlreadyExists"))
            {
                Console.WriteLine($"Table '{tableName}' already exists");
                return _tableServiceClient.GetTableClient(tableName);
            }
        }


        public async Task<bool> InsertEntityAsync<T>(string tableName, T entity) where T : class, ITableEntity
        {
            try
            {
                var tableClient = _tableServiceClient.GetTableClient(tableName);
                await tableClient.AddEntityAsync(entity);

                Console.WriteLine($"Entity inserted into table '{tableName}' with PartitionKey: {entity.PartitionKey}, RowKey: {entity.RowKey}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error inserting entity: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> UpsertEntityAsync<T>(string tableName, T entity) where T : class, ITableEntity
        {
            try
            {
                var tableClient = _tableServiceClient.GetTableClient(tableName);
                await tableClient.UpsertEntityAsync(entity);

                Console.WriteLine($"Entity upserted in table '{tableName}' with PartitionKey: {entity.PartitionKey}, RowKey: {entity.RowKey}");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error upserting entity: {ex.Message}");
                return false;
            }
        }

        public async Task<T?> GetEntityAsync<T>(string tableName, string partitionKey, string rowKey) where T : class, ITableEntity, new()
        {
            try
            {
                var tableClient = _tableServiceClient.GetTableClient(tableName);
                var response = await tableClient.GetEntityAsync<T>(partitionKey, rowKey);

                Console.WriteLine($"Entity retrieved from table '{tableName}'");
                return response.Value;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error getting entity: {ex.Message}");
                return null;
            }
        }

        public async Task<List<T>> QueryEntitiesAsync<T>(string tableName, string? filter = null) where T : class, ITableEntity, new()
        {
            try
            {
                var tableClient = _tableServiceClient.GetTableClient(tableName);
                var entities = new List<T>();

                await foreach (var entity in tableClient.QueryAsync<T>(filter))
                {
                    entities.Add(entity);
                }

                Console.WriteLine($"Retrieved {entities.Count} entities from table '{tableName}'");
                return entities;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error querying entities: {ex.Message}");
                return new List<T>();
            }
        }

        public async Task<bool> UpdateEntityAsync<T>(string tableName, T entity) where T : class, ITableEntity
        {
            try
            {
                var tableClient = _tableServiceClient.GetTableClient(tableName);
                await tableClient.UpdateEntityAsync(entity, entity.ETag, TableUpdateMode.Replace);

                Console.WriteLine($"Entity updated in table '{tableName}'");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error updating entity: {ex.Message}");
                return false;
            }
        }

        public async Task<bool> DeleteEntityAsync(string tableName, string partitionKey, string rowKey, ETag etag = default)
        {
            try
            {
                var tableClient = _tableServiceClient.GetTableClient(tableName);
                await tableClient.DeleteEntityAsync(partitionKey, rowKey, etag);

                Console.WriteLine($"Entity deleted from table '{tableName}'");
                return true;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error deleting entity: {ex.Message}");
                return false;
            }
        }
    }
}
