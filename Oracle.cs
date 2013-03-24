using System;
using System.Data;
using System.Data.OracleClient;

namespace DianPing.BA.Framework.DAL
{
    public class Oracle : AdoHelper
    {
        public override IDbConnection GetConnection(string connectionString)
        {
            return new OracleConnection(connectionString);
        }

        protected override IDbDataAdapter GetDataAdapter()
        {
            return new OracleDataAdapter();
        }

        protected override void DeriveParameters(IDbCommand cmd)
        {
            if (!(cmd is OracleCommand))
                throw new ArgumentException("The command provided is not a OleDbCommand instance.", "cmd");
            OracleCommandBuilder.DeriveParameters((OracleCommand) cmd);
        }

        public override IDataParameter GetParameter()
        {
            return new OracleParameter();
        }

        private string GetParameterName(string parameterName)
        {
            return parameterName;
        }

        public override IDataParameter GetParameter(string parameterName)
        {
            var oracleParameter = new OracleParameter();
            oracleParameter.ParameterName = GetParameterName(parameterName.Trim());
            return oracleParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType)
        {
            var oracleParameter = new OracleParameter();
            oracleParameter.ParameterName = GetParameterName(parameterName);
            oracleParameter.DbType = dbType;
            return oracleParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType, object paramValue)
        {
            var oracleParameter = GetParameter(parameterName, dbType) as OracleParameter;
            oracleParameter.Value = paramValue;
            return oracleParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection)
        {
            var oracleParameter = new OracleParameter();
            oracleParameter.ParameterName = GetParameterName(parameterName);
            oracleParameter.DbType = dbType;
            if (dbType == DbType.Object)
                oracleParameter.OracleType = OracleType.Cursor;
            oracleParameter.Direction = paramDirection;
            return oracleParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection, int size)
        {
            var oracleParameter = GetParameter(parameterName, dbType, paramDirection) as OracleParameter;
            oracleParameter.Size = size;
            return oracleParameter;
        }

        public override bool DrHasRows(IDataReader dataReader)
        {
            if (dataReader == null || !(dataReader is OracleDataReader))
                throw new ArgumentException(
                    "The dataReader provided is not a OracleDataReader instance. please check your codes!", "dataReader");
            return (dataReader as OracleDataReader).HasRows;
        }
    }
}