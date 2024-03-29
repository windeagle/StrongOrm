using System.Configuration;
using System.Web.Configuration;

namespace DianPing.BA.Framework.DAL
{
    public class DbConnectionStore
    {
        public static DbConnectionStore TheInstance = new DbConnectionStore();
        public static ConnectionStringSettingsCollection ConnectionStrings;

        static DbConnectionStore()
        {
            var connectionStringsSection =
                WebConfigurationManager.GetSection("connectionStrings") as ConnectionStringsSection;
            if (connectionStringsSection != null)
            {
                ConnectionStrings =
                    connectionStringsSection.ConnectionStrings;
            }
        }

        private DbConnectionStore()
        {
        }

        public string GetConnection(string connStrAlians)
        {
            if (ConnectionStrings != null)
            {
                ConnectionStringSettings connStringSettings = ConnectionStrings[connStrAlians];
                //DbProviderFactory providerFactory = DbProviderFactories.GetFactory(connStringSettings.ProviderName);
                //DbConnection cn = providerFactory.CreateConnection();
                return connStringSettings.ConnectionString;
            }
            return string.Empty;
        }
    }
}