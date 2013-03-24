using System;
using System.Data;
using MySql.Data.MySqlClient;

namespace DianPing.BA.Framework.DAL
{
    public class MySql : AdoHelper
    {
        public override string FieldPrefix
        {
            get { return "`"; }
        }

        public override string FieldSuffix
        {
            get { return "`"; }
        }

        public override string ParamPrefix
        {
            get { return "?"; }
        }

        public override IDbConnection GetConnection(string connectionString)
        {
            return new MySqlConnection(connectionString);
        }

        protected override IDbDataAdapter GetDataAdapter()
        {
            return new MySqlDataAdapter();
        }

        protected override void DeriveParameters(IDbCommand cmd)
        {
            if (!(cmd is MySqlCommand))
                throw new ArgumentException("The command provided is not a MySqlCommand instance.", "cmd");
            MySqlCommandBuilder.DeriveParameters((MySqlCommand) cmd);
        }

        public override bool DrHasRows(IDataReader dataReader)
        {
            if (dataReader == null || !(dataReader is MySqlDataReader))
                throw new ArgumentException(
                    "The dataReader provided is not a MySqlDataReader instance. please check your codes!", "dataReader");
            return (dataReader as MySqlDataReader).HasRows;
        }

        public override IDataParameter GetParameter()
        {
            return new MySqlParameter();
        }

        private static string GetParameterName(string parameterName)
        {
            string str = parameterName;
            if (!parameterName.StartsWith("?"))
                str = "?" + parameterName;
            return str;
        }

        public override IDataParameter GetParameter(string parameterName)
        {
            return new MySqlParameter(GetParameterName(parameterName.Trim()), null);
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType)
        {
            return new MySqlParameter(GetParameterName(parameterName.Trim()), GetMySqlType(dbType));
        }

        private static MySqlDbType GetMySqlType(DbType argType)
        {
            switch (argType)
            {
                case DbType.String:
                    return MySqlDbType.VarChar;
                case DbType.Int32:
                    return MySqlDbType.Int32;
                case DbType.Date:
                    return MySqlDbType.Date;
                case DbType.DateTime:
                    return MySqlDbType.DateTime;
                case DbType.Decimal:
                    return MySqlDbType.Decimal;
                default:
                    return MySqlDbType.VarString;
            }
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType, object paramValue)
        {
            var param = new MySqlParameter(GetParameterName(parameterName.Trim()), GetMySqlType(dbType))
                            {Value = paramValue};
            return param;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection)
        {
            var param = new MySqlParameter(GetParameterName(parameterName.Trim()), GetMySqlType(dbType))
                            {Direction = paramDirection};
            return param;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection, int size)
        {
            var param = new MySqlParameter(GetParameterName(parameterName.Trim()), GetMySqlType(dbType), size)
                            {Direction = paramDirection};
            return param;
        }


        protected override IDataParameter[] DiscoverSpParameterSet(IDbConnection connection, string spName,
                                                                   bool includeReturnValueParameter)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (String.IsNullOrEmpty(spName))
                throw new ArgumentNullException("spName");
            IDbCommand command = connection.CreateCommand();
            command.CommandText = spName;
            command.CommandType = CommandType.StoredProcedure;
            connection.Open();
            DeriveParameters(command);
            connection.Close();
            //if (!includeReturnValueParameter)
            //    command.Parameters.RemoveAt(0);
            var dataParameterArray = new IDataParameter[command.Parameters.Count];
            command.Parameters.CopyTo(dataParameterArray, 0);
            foreach (IDataParameter dataParameter in dataParameterArray)
                dataParameter.Value = DBNull.Value;
            return dataParameterArray;
        }
    }
}