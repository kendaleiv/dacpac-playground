using Microsoft.Data.SqlClient;
using Microsoft.SqlServer.Dac;
using ThrowawayDb;
using Xunit;

namespace TestDatabase.Tests
{
    public class Tests
    {
        [Fact]
        public async Task DacServicesDeletesExistingIndexByDefault()
        {
            // Arrange
            using var database = ThrowawayDatabase.FromLocalInstance(@"(LocalDB)\MSSQLLocalDB");

            var dacPackage = DacPackage.Load("../../../../../src/TestDatabase/bin/Debug/TestDatabase.dacpac");
            var dacServices = new DacServices(database.ConnectionString);

            dacServices.Deploy(dacPackage, database.Name, upgradeExisting: true);

            using (var connection = await database.OpenConnectionAsync())
            {
                var createIndexCommand = new SqlCommand("CREATE NONCLUSTERED INDEX TestIndex1 ON [dbo].[TestTable] (Id)", connection);
                await createIndexCommand.ExecuteNonQueryAsync();
            }

            // Act
            dacServices.Deploy(dacPackage, database.Name, upgradeExisting: true);

            // Assert
            using (var connection = await database.OpenConnectionAsync())
            {
                var checkIndexCommand = new SqlCommand("SELECT 1 FROM sys.indexes WHERE name = 'TestIndex1'", connection);
                var result = await checkIndexCommand.ExecuteScalarAsync();

                Assert.Null(result);
            }
        }

        [Fact]
        public async Task DacServicesDoesNotDeleteExistingIndexWhenDropIndexesNotInSourceIsFalse()
        {
            // Arrange
            using var database = ThrowawayDatabase.FromLocalInstance(@"(LocalDB)\MSSQLLocalDB");

            var dacPackage = DacPackage.Load("../../../../../src/TestDatabase/bin/Debug/TestDatabase.dacpac");
            var dacServices = new DacServices(database.ConnectionString);

            dacServices.Deploy(dacPackage, database.Name, upgradeExisting: true);

            using (var connection = await database.OpenConnectionAsync())
            {
                var createIndexCommand = new SqlCommand("CREATE NONCLUSTERED INDEX TestIndex2 ON [dbo].[TestTable] (Id)", connection);
                await createIndexCommand.ExecuteNonQueryAsync();
            }

            // Act
            dacServices.Deploy(dacPackage, database.Name, upgradeExisting: true, new DacDeployOptions
            {
                DropIndexesNotInSource = false
            });

            // Assert
            using (var connection = await database.OpenConnectionAsync())
            {
                var checkIndexCommand = new SqlCommand("SELECT 1 FROM sys.indexes WHERE name = 'TestIndex2'", connection);
                var result = await checkIndexCommand.ExecuteScalarAsync();

                Assert.NotNull(result);
            }
        }
    }
}
