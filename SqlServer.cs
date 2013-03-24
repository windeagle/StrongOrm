using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Xml;

namespace DianPing.BA.Framework.DAL
{
    public class SqlServer : AdoHelper
    {
        public override string FieldPrefix
        {
            get { return "["; }
        }

        public override string FieldSuffix
        {
            get { return "]"; }
        }

        public override string ParamPrefix
        {
            get { return "@"; }
        }

        public override IDbConnection GetConnection(string connectionString)
        {
            return new SqlConnection(connectionString);
        }

        protected override IDbDataAdapter GetDataAdapter()
        {
            return new SqlDataAdapter();
        }

        protected override void DeriveParameters(IDbCommand cmd)
        {
            if (!(cmd is SqlCommand))
                throw new ArgumentException("The command provided is not a SqlCommand instance.", "cmd");
            SqlCommandBuilder.DeriveParameters((SqlCommand) cmd);
        }

        private static string GetParameterName(string parameterName)
        {
            string str = parameterName;
            if (!parameterName.StartsWith("@"))
                str = "@" + parameterName;
            return str;
        }

        public override IDataParameter GetParameter()
        {
            return new SqlParameter();
        }

        public override IDataParameter GetParameter(string parameterName)
        {
            return new SqlParameter {ParameterName = GetParameterName(parameterName)};
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType)
        {
            var sqlParameter = GetParameter(parameterName);
            sqlParameter.DbType = dbType;
            return sqlParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType, object paramValue)
        {
            var sqlParameter = GetParameter(parameterName, dbType);
            sqlParameter.Value = paramValue;
            return sqlParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection)
        {
            var sqlParameter = GetParameter(parameterName, dbType);
            sqlParameter.Direction = paramDirection;
            return sqlParameter;
        }

        public override IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection, int size)
        {
            var parameter = GetParameter(parameterName, dbType, paramDirection);
            var sqlParameter = parameter as SqlParameter;
            if (sqlParameter != null)
            {
                sqlParameter.Size = size;
            }
            return parameter;
        }

        public override bool DrHasRows(IDataReader dataReader)
        {
            if (dataReader == null || !(dataReader is SqlDataReader))
                throw new ArgumentException("The dataReader provided is not a SqlDataReader instance.", "dataReader");
            return (dataReader as SqlDataReader).HasRows;
        }

        protected override void ClearCommand(IDbCommand command)
        {
            bool flag = true;
            foreach (IDataParameter dataParameter in command.Parameters)
            {
                if (dataParameter.Direction != ParameterDirection.Input)
                    flag = false;
            }
            if (!flag)
                return;
            command.Parameters.Clear();
        }

        #region ExecuteXmlReader

        public virtual XmlReader ExecuteXmlReader<T, TU>(T conn, string commandText, TU connParams,
                                                         CommandType commandType, out List<IDataParameter> parameters,
                                                         string conAlians = null, string cmdAlians = null)
            where T : class
            where TU : class
        {
            if (conn == null)
                throw new ArgumentNullException("conn");
            if (typeof (T) == typeof (String))
            {
                string connStr = conn as String;
                if (String.IsNullOrEmpty(connStr))
                    throw new ArgumentNullException("conn");
                IDbConnection connection = null;
                IDbTransaction transaction = null;
                try
                {
                    connection = GetConnection(connStr);
                    transaction = GetTransaction(connection);
                    if (transaction != null)
                        return ExecuteXmlReader(transaction, commandText, connParams, commandType, out parameters,
                                                conAlians, cmdAlians);
                    return ExecuteXmlReader(connection, commandText, connParams, commandType, out parameters, conAlians,
                                            cmdAlians);
                }
                finally
                {
                    IDisposable disposable = connection;
                    if (disposable != null && transaction == null)
                        disposable.Dispose();
                }
            }
            IDataParameter[] spParameterSet;
            bool mustCloseConnection;
            var command = GetCommand(conn, commandText, connParams, commandType, conAlians, cmdAlians,
                                     out spParameterSet, out mustCloseConnection);
            //PerformanceUtil.InsertPerformanceAnchor("ExecuteXmlReader前");
            XmlReader xmlReader = ((SqlCommand) command).ExecuteXmlReader();
            //PerformanceUtil.InsertPerformanceAnchor("ExecuteXmlReader后");
            ClearCommand(command);
            parameters = new List<IDataParameter>(spParameterSet);
            if (mustCloseConnection)
                command.Connection.Close();
            return xmlReader;
        }

        public virtual XmlReader ExecuteXmlReader<T, TU>(T conn, string commandText, TU connParams,
                                                         out List<IDataParameter> parameters, string conAlians = null,
                                                         string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteXmlReader(conn, commandText, connParams, CommandType.StoredProcedure, out parameters,
                                    conAlians, cmdAlians);
        }

        public virtual XmlReader ExecuteXmlReader<T, TU>(T conn, string commandText, CommandType commandType,
                                                         out List<IDataParameter> parameters, string conAlians = null,
                                                         string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteXmlReader<T, TU>(conn, commandText, null, commandType, out parameters, conAlians, cmdAlians);
        }

        public virtual XmlReader ExecuteXmlReader<T, TU>(T conn, string commandText, out List<IDataParameter> parameters,
                                                         string conAlians = null, string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteXmlReader<T, TU>(conn, commandText, CommandType.StoredProcedure, out parameters, conAlians,
                                           cmdAlians);
        }

        #endregion
    }
}