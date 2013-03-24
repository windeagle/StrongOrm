using System;
using System.Data;
using System.Data.OleDb;

namespace DianPing.BA.Framework.DAL
{
    public class OleDb : AdoHelper
    {
        public override string ParamPrefix
        {
            get { return "@"; }
        }

        public override IDbConnection GetConnection(string connectionString)
        {
            return new OleDbConnection(connectionString);
        }

        protected override IDbDataAdapter GetDataAdapter()
        {
            return new OleDbDataAdapter();
        }

        protected override void DeriveParameters(IDbCommand cmd)
        {
            if (!(cmd is OleDbCommand))
                throw new ArgumentException("The command provided is not a OleDbCommand instance.", "cmd");
            OleDbCommandBuilder.DeriveParameters((OleDbCommand) cmd);
        }

        public override IDataParameter GetParameter()
        {
            return new OleDbParameter();
        }

        private string GetParameterName(string parameterName)
        {
            string str = parameterName;
            if (!parameterName.StartsWith("@"))
                str = "@" + parameterName;
            return str;
        }

        public override IDataParameter GetParameter(string parameterName)
        {
            var oleDbParameter = new OleDbParameter();
            oleDbParameter.ParameterName = GetParameterName(parameterName);
            return oleDbParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType)
        {
            var oleDbParameter = new OleDbParameter();
            oleDbParameter.ParameterName = GetParameterName(parameterName);
            oleDbParameter.DbType = dbType;
            return oleDbParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType, object paramValue)
        {
            var oleDbParameter = GetParameter(parameterName, dbType) as OleDbParameter;
            oleDbParameter.Value = paramValue;
            return oleDbParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection)
        {
            var oleDbParameter = new OleDbParameter();
            oleDbParameter.ParameterName = GetParameterName(parameterName);
            oleDbParameter.DbType = dbType;
            oleDbParameter.Direction = paramDirection;
            return oleDbParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection, int size)
        {
            var oleDbParameter = new OleDbParameter();
            oleDbParameter.ParameterName = GetParameterName(parameterName);
            oleDbParameter.DbType = dbType;
            oleDbParameter.Direction = paramDirection;
            oleDbParameter.Size = size;
            return oleDbParameter;
        }

        public override bool DrHasRows(IDataReader dataReader)
        {
            if (dataReader == null || !(dataReader is OleDbDataReader))
                throw new ArgumentException("The dataReader provided is not a OleDbDataReader instance.", "dataReader");
            return (dataReader as OleDbDataReader).HasRows;
        }
    }
}