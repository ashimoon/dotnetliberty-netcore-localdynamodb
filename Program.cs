using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime;

namespace LocalDynamoDb
{
    public class Program
    {
        private readonly AmazonDynamoDBClient client;

        public static void Main(string[] args)
        {
            var program = new Program();
            program.Demo().Wait();
        }

        public Program()
        {
            client = BuildClient();
        }

        public async Task Demo()
        {
            var description = await BuildOrDescribeTable();
            Console.WriteLine($"Table status: {description.TableStatus}");
            await SaveItem();
            var loadedItem = await FetchItem();
            Console.WriteLine($"Item loaded. Description: {loadedItem["Description"].S}");
        }

        private AmazonDynamoDBClient BuildClient()
        {
            Console.WriteLine("Creating local DynamoDB client...");
            // Local DynamoDB server requires credentials to be specified,
            // but it doesn't matter what they are
            var credentials = new BasicAWSCredentials(
                accessKey: "fake-access-key",
                secretKey: "fake-secret-key");
            var config = new AmazonDynamoDBConfig
            {
                ServiceURL = "http://localhost:5151"
            };
            return new AmazonDynamoDBClient(credentials, config);
        }

        private async Task<Dictionary<string, AttributeValue>> FetchItem()
        {
            Console.WriteLine("About to fetch item '123' from the Widgets table...");
            var response = await client.GetItemAsync(
                tableName: "Widgets",
                key: new Dictionary<string, AttributeValue>
                {
                    {"WidgetId", new AttributeValue {S = "123"}}
                }
            );
            return response.Item;
        }

        private async Task SaveItem()
        {
            Console.WriteLine("About to save item '123' to the Widgets table...");
            await client.PutItemAsync(
                tableName: "Widgets",
                item: new Dictionary<string, AttributeValue>
                {
                    {"WidgetId", new AttributeValue {S = "123"}},
                    {"Description", new AttributeValue {S = "This is a widget."}}
                }
            );
        }

        private async Task<TableDescription> BuildOrDescribeTable()
        {
            var request = new CreateTableRequest(
                tableName: "Widgets",
                keySchema: new List<KeySchemaElement>
                {
                    new KeySchemaElement
                    {
                        AttributeName = "WidgetId",
                        KeyType = KeyType.HASH
                    }
                },
                attributeDefinitions: new List<AttributeDefinition>
                {
                    new AttributeDefinition()
                    {
                        AttributeName = "WidgetId",
                        AttributeType = ScalarAttributeType.S
                    }
                },
                // The provisioned throughput values are ignored locally
                provisionedThroughput: new ProvisionedThroughput
                {
                    ReadCapacityUnits = 10,
                    WriteCapacityUnits = 10
                }
            );
            Console.WriteLine("Sending request to build Widgets table...");
            try
            {
                var result = await client.CreateTableAsync(request);
                Console.WriteLine("Table created.");
                return result.TableDescription;
            }
            catch (ResourceInUseException)
            {
                // Table already created, just describe it
                Console.WriteLine("Table already exists. Fetching description...");
                var description = await client.DescribeTableAsync(new DescribeTableRequest {TableName = "Widgets"});
                return description.Table;
            }
        }
    }
}
