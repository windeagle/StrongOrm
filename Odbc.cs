using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Odbc;
using System.Text;
using System.Text.RegularExpressions;

namespace DianPing.BA.Framework.DAL
{
    public class Odbc : AdoHelper
    {
        private static readonly Regex RegExpr = new Regex("\\{.*call|CALL\\s\\w+.*}", RegexOptions.Compiled);

        public override IDbConnection GetConnection(string connectionString)
        {
            return new OdbcConnection(connectionString);
        }

        protected override IDbDataAdapter GetDataAdapter()
        {
            return new OdbcDataAdapter();
        }

        protected override void DeriveParameters(IDbCommand cmd)
        {
            if (!(cmd is OdbcCommand))
                throw new ArgumentException("The command provided is not a OdbcCommand instance.", "cmd");
            OdbcCommandBuilder.DeriveParameters((OdbcCommand) cmd);
        }

        public override IDataParameter GetParameter()
        {
            return new OdbcParameter();
        }

        private string GetParameterName(string parameterName)
        {
            return parameterName;
        }

        public override IDataParameter GetParameter(string parameterName)
        {
            IDataParameter dataParameter = new OdbcParameter();
            dataParameter.ParameterName = GetParameterName(parameterName);
            return dataParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType)
        {
            IDataParameter dataParameter = new OdbcParameter();
            dataParameter.ParameterName = GetParameterName(parameterName);
            dataParameter.DbType = dbType;
            return dataParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType, object paramValue)
        {
            IDataParameter parameter = GetParameter(parameterName, dbType);
            parameter.Value = paramValue;
            return parameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection)
        {
            IDataParameter dataParameter = new OdbcParameter();
            dataParameter.ParameterName = GetParameterName(parameterName);
            dataParameter.DbType = dbType;
            dataParameter.Direction = paramDirection;
            return dataParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection, int size)
        {
            IDataParameter dataParameter = new OdbcParameter();
            dataParameter.ParameterName = GetParameterName(parameterName);
            dataParameter.DbType = dbType;
            dataParameter.Direction = paramDirection;
            return dataParameter;
        }

        public override bool DrHasRows(IDataReader dataReader)
        {
            if (dataReader == null || !(dataReader is OdbcDataReader))
                throw new ArgumentException("The dataReader provided is not a OdbcDataReader instance.", "dataReader");
            return (dataReader as OdbcDataReader).HasRows;
        }

        [Obsolete("不推荐使用，只有ODBC使用")]
        protected override void PrepareCommand(IDbCommand command, IDbConnection connection, IDbTransaction transaction,
                                               CommandType commandType, string commandText,
                                               IEnumerable<IDataParameter> commandParameters,
                                               out bool mustCloseConnection)
        {
            base.PrepareCommand(command, connection, transaction, commandType, commandText, commandParameters,
                                out mustCloseConnection);
            if (command.CommandType != CommandType.StoredProcedure || RegExpr.Match(command.CommandText).Success ||
                command.CommandText.Trim().IndexOf(" ") != -1)
                return;
            var stringBuilder = new StringBuilder();
            if (command.Parameters.Count != 0)
            {
                bool flag = true;
                for (int index = 0; index < command.Parameters.Count; ++index)
                {
                    if ((command.Parameters[index] as OdbcParameter).Direction != ParameterDirection.ReturnValue)
                    {
                        if (flag)
                        {
                            flag = false;
                            stringBuilder.Append("(?");
                        }
                        else
                            stringBuilder.Append(",?");
                    }
                }
                stringBuilder.Append(")");
            }
            command.CommandText = "{ call " + command.CommandText + (stringBuilder) + " }";
        }
    }
}