using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Reflection;
using System.Runtime.Remoting.Messaging;
using System.Web.Configuration;
using StrongCutIn.Util;

namespace DianPing.BA.Framework.DAL
{
    public abstract class AdoHelper
    {
        //protected static Hashtable paramCache = Hashtable.Synchronized(new Hashtable());
        protected static IDictionary<string, AdoHelper> ADOCache = new Dictionary<string, AdoHelper>();
        protected static readonly object LockObjADOCache = new object();
        private int _commandTimeout = 30;

        public virtual string FieldPrefix
        {
            get { return string.Empty; }
        }

        public virtual string FieldSuffix
        {
            get { return string.Empty; }
        }

        public virtual string ParamPrefix
        {
            get { return string.Empty; }
        }

        public virtual int CommandTimeout
        {
            get { return _commandTimeout; }
            set { _commandTimeout = value; }
        }

        public abstract IDbConnection GetConnection(string connectionString);

        protected abstract IDbDataAdapter GetDataAdapter();

        protected abstract void DeriveParameters(IDbCommand cmd);

        public abstract bool DrHasRows(IDataReader dataReader);

        private static AdoHelper CreateHelper(string providerAssembly, string providerType)
        {
            object instance = Assembly.Load(providerAssembly).CreateInstance(providerType);
            if (instance is AdoHelper)
                return instance as AdoHelper;
            throw new Exception("The provider specified does not extends the AdoHelper abstract class.");
        }

        /// <summary>
        ///   根据 DbProvideType 获取常用的 AdoHelper
        /// </summary>
        /// <param name="type"> </param>
        /// <returns> </returns>
        public static AdoHelper CreateHelper(DbProvideType type)
        {
            if (type == DbProvideType.Oracle)
                return CreateHelper("Oracle");
            if (type == DbProvideType.SqlServer)
                return CreateHelper("SqlServer");
            if (type == DbProvideType.OleDb)
                return CreateHelper("OleDb");
            if (type == DbProvideType.Odbc)
                return CreateHelper("Odbc");
            if (type == DbProvideType.MySql)
                return CreateHelper("MySql");
            throw new NotSupportedException("Not supported Provider" + (type));
        }

        public static AdoHelper CreateHelper(string providerAlias)
        {
            if (ADOCache.ContainsKey(providerAlias))
                return ADOCache[providerAlias];
            if (providerAlias == "OleDb")
                ADOCache.Add(providerAlias, new OleDb());
            if (providerAlias == "Oracle")
                ADOCache.Add(providerAlias, new Oracle());
            if (providerAlias == "SqlServer")
                ADOCache.Add(providerAlias, new SqlServer());
            if (providerAlias == "Odbc")
                ADOCache.Add(providerAlias, new Odbc());
            if (providerAlias == "MySql")
                ADOCache.Add(providerAlias, new MySql());
            Dictionary<string, ProviderAlias> config =
                WebConfigurationManager.GetSection("daabProviders") as Dictionary<string, ProviderAlias>;
            if (config != null)
            {
                var ado = CreateHelper(config[providerAlias.ToLower()].AssemblyName,
                                       config[providerAlias.ToLower()].TypeName);
                lock (LockObjADOCache)
                {
                    if (!ADOCache.ContainsKey(providerAlias))
                        ADOCache.Add(providerAlias, ado);
                }
            }
            else
                throw new ArgumentException("Invalid Provider Name");
            return ADOCache[providerAlias];
        }

        #region private utility methods

        /// <summary>
        ///   找到 Sql Command 的配置信息
        /// </summary>
        /// <param name="conAlians"> </param>
        /// <param name="cmdAlians"> </param>
        /// <returns> </returns>
        public static CommandInfo GetCommandInfo(string conAlians, string cmdAlians)
        {
            try
            {
                if (string.IsNullOrEmpty(conAlians)) conAlians = "Default";
                MappingInfo map = MappingStore.TheInstance.GetMappingInfo(conAlians);
                return map != null ? map.GetCommandInfo(cmdAlians) : null;
            }
            catch (Exception e)
            {
                throw new Exception(
                    "找不到" + conAlians + "连接字符串对应的配置节或者在" + conAlians + "连接字符串对应的配置节中找不到" + cmdAlians + "命令对应的配置节", e);
            }
        }

        /// <summary>
        ///   用实体对象给 Sql 参数列表赋值
        /// </summary>
        /// <typeparam name="T"> </typeparam>
        /// <param name="commandParameters"> </param>
        /// <param name="obj"> </param>
        /// <param name="conAlians"> </param>
        /// <param name="cmdAlians"> </param>
        public static void AssignParameterValues<T>(IDataParameter[] commandParameters, T obj, string conAlians,
                                                    string cmdAlians) where T : class
        {
            if (commandParameters == null || obj == null || String.IsNullOrEmpty(cmdAlians))
            {
                // Do nothing if we get no data
                return;
            }

            CommandInfo cmdInfo = GetCommandInfo(conAlians, cmdAlians);

            int i = 0;
            // Set the parameters values
            foreach (IDataParameter commandParameter in commandParameters)
            {
                // Check the parameter name
                if (commandParameter.ParameterName == null ||
                    commandParameter.ParameterName.Length <= 1)
                    throw new Exception(string.Format(
                        "Please provide a valid parameter name on the parameter #{0}, the ParameterName property has the following value: '{1}'.",
                        i, commandParameter.ParameterName));

                string tmpStr = cmdInfo != null &&
                                cmdInfo.ParamMappings.ContainsKey(commandParameter.ParameterName.ToLower())
                                    ? cmdInfo.ParamMappings[commandParameter.ParameterName.ToLower()]
                                    : commandParameter.ParameterName;
                if (tmpStr.Equals(commandParameter.ParameterName, StringComparison.OrdinalIgnoreCase))
                {
                    tmpStr = cmdInfo != null &&
                             cmdInfo.ParamMappings.ContainsKey(commandParameter.ParameterName.Substring(1).ToLower())
                                 ? cmdInfo.ParamMappings[commandParameter.ParameterName.Substring(1).ToLower()]
                                 : commandParameter.ParameterName;
                }
                foreach (PropertyInfo propertyInfo in DbUtil.GetProperties(obj.GetType()))
                {
                    if ((tmpStr.Equals(propertyInfo.Name, StringComparison.OrdinalIgnoreCase)) ||
                        (tmpStr.Substring(1).Equals(propertyInfo.Name,
                                                    StringComparison.OrdinalIgnoreCase)))
                    {
                        object fieldValue = TypeUtility.GetMemberGetDelegate(propertyInfo)(obj);
                        if (fieldValue != null)
                        {
                            if (propertyInfo.PropertyType.IsEnum)
                            {
                                var vals = Enum.GetValues(propertyInfo.PropertyType);

                                foreach (var val in vals)
                                {
                                    if (fieldValue.Equals(Enum.ToObject(propertyInfo.PropertyType, val)))
                                    {
                                        commandParameter.Value = val;
                                        break;
                                    }
                                }
                            }
                            else
                                commandParameter.Value = fieldValue;
                        }
                        i++;
                        break;
                    }
                }
            }
        }

        /// <summary>
        ///   用 DataRow 给 Sql 参数列表赋值
        /// </summary>
        /// <param name="commandParameters"> Array of IDataParameters to be assigned values </param>
        /// <param name="dataRow"> The dataRow used to hold the stored procedure's parameter values </param>
        public static void AssignParameterValues(IDataParameter[] commandParameters, DataRow dataRow)
        {
            if (commandParameters == null || dataRow == null)
            {
                // Do nothing if we get no data
                return;
            }

            DataColumnCollection columns = dataRow.Table.Columns;

            int i = 0;
            // Set the parameters values
            foreach (IDataParameter commandParameter in commandParameters)
            {
                // Check the parameter name
                if (commandParameter.ParameterName == null ||
                    commandParameter.ParameterName.Length <= 1)
                    throw new Exception(String.Format(
                        "Please provide a valid parameter name on the parameter #{0}, the ParameterName property has the following value: '{1}'.",
                        i, commandParameter.ParameterName));

                if (columns.Contains(commandParameter.ParameterName))
                    commandParameter.Value = dataRow[commandParameter.ParameterName];
                else if (columns.Contains(commandParameter.ParameterName.Substring(1)))
                    commandParameter.Value = dataRow[commandParameter.ParameterName.Substring(1)];

                i++;
            }
        }

        /// <summary>
        ///   This method assigns an array of values to an array of IDataParameters
        /// </summary>
        /// <param name="commandParameters"> Array of IDataParameters to be assigned values </param>
        /// <param name="parameterValues"> Array of objects holding the values to be assigned </param>
        public static void AssignParameterValues(IDataParameter[] commandParameters, object[] parameterValues)
        {
            if (commandParameters == null || parameterValues == null)
            {
                // Do nothing if we get no data
                return;
            }

            // We must have the same number of values as we pave parameters to put them in
            if (commandParameters.Length != parameterValues.Length)
            {
                throw new ArgumentException("Parameter count does not match Parameter Value count.");
            }

            // Iterate through the IDataParameters, assigning the values from the corresponding position in the 
            // value array
            for (int i = 0, j = commandParameters.Length; i < j; i++)
            {
                // If the current array value derives from IDataParameter, then assign its Value property
                if (parameterValues[i] is IDataParameter)
                {
                    var paramInstance = (IDataParameter) parameterValues[i];
                    commandParameters[i].Value = paramInstance.Value ?? DBNull.Value;
                }
                else if (parameterValues[i] == null)
                {
                    commandParameters[i].Value = DBNull.Value;
                }
                else
                {
                    commandParameters[i].Value = parameterValues[i];
                }
            }
        }

        ///<summary>
        ///  内部方法，把指定的IDataparameter参数数组附给IDbCommand对象 This method is used to attach array of IDataParameters to a IDbCommand. This method will assign a value of DbNull to any parameter with a direction of InputOutput and a value of null. This behavior will prevent default values from being used, but this will be the less common case than an intended pure output parameter (derived as InputOutput) where the user provided no input value.
        ///</summary>
        ///<param name="command"> The command to which the parameters will be added </param>
        ///<param name="commandParameters"> An array of IDataParameterParameters to be added to command </param>
        private static void AttachParameters(IDbCommand command, IEnumerable<IDataParameter> commandParameters)
        {
            if (command == null) throw new ArgumentNullException("command");
            if (commandParameters != null)
            {
                foreach (IDataParameter p in commandParameters)
                {
                    if (p != null)
                    {
                        // Check for derived output value with no value assigned
                        if ((p.Direction == ParameterDirection.InputOutput ||
                             p.Direction == ParameterDirection.Input) &&
                            (p.Value == null))
                        {
                            p.Value = DBNull.Value;
                        }
                        command.Parameters.Add(p);
                    }
                }
            }
        }

        /// <summary>
        ///   打开（如果有必要）数据库连接，并把连接对象，事务对象，Command类型和参数列表赋给指定的Command对象
        /// </summary>
        /// <param name="command"> The IDbCommand to be prepared </param>
        /// <param name="connection"> A valid IDbConnection, on which to execute this command </param>
        /// <param name="transaction"> A valid IDbTransaction, or 'null' </param>
        /// <param name="commandType"> The CommandType (stored procedure, text, etc.) </param>
        /// <param name="commandText"> The stored procedure name or SQL command </param>
        /// <param name="commandParameters"> An array of IDataParameters to be associated with the command or 'null' if no parameters are required </param>
        /// <param name="mustCloseConnection"> <c>true</c> if the connection was opened by the method, otherwose is false. </param>
        [Obsolete("不推荐使用，只有ODBC使用")]
        protected virtual void PrepareCommand(IDbCommand command, IDbConnection connection, IDbTransaction transaction,
                                              CommandType commandType, string commandText,
                                              IEnumerable<IDataParameter> commandParameters,
                                              out bool mustCloseConnection)
        {
            if (command == null) throw new ArgumentNullException("command");
            if (String.IsNullOrEmpty(commandText)) throw new ArgumentNullException("commandText");

            mustCloseConnection = false;
            // Set the command text (stored procedure name or SQL statement)
            command.CommandText = commandText;

            // 如果提供了一个有效的Transaction对象,则把该Transaction对象付给Command对象
            if (transaction != null)
            {
                if (transaction.Connection == null || transaction.Connection.State == ConnectionState.Closed)
                    throw new ArgumentException(
                        "The transaction was rollbacked or commited, please provide an open transaction.", "transaction");
                command.Transaction = transaction;
            }
            else
            {
                // If the provided connection is not open, we will open it
                if (connection.State != ConnectionState.Open)
                {
                    mustCloseConnection = true; //内部打开的数据库连接，则在方法退出前必须关闭连接
                    connection.Open();
                }

                // Associate the connection with the command
                command.Connection = connection;
            }

            // Set the command type
            command.CommandType = commandType;
            command.CommandTimeout = CommandTimeout;

            // Attach the command parameters if they are provided
            if (commandParameters != null)
            {
                AttachParameters(command, commandParameters);
            }
        }

        protected void PrepareCommand(IDbCommand command, IDbConnection connection,
                                      CommandType commandType, string commandText,
                                      IEnumerable<IDataParameter> commandParameters, out bool mustCloseConnection)
        {
            if (command == null) throw new ArgumentNullException("command");
            if (String.IsNullOrEmpty(commandText)) throw new ArgumentNullException("commandText");

            mustCloseConnection = false;
            // Set the command text (stored procedure name or SQL statement)
            command.CommandText = commandText;

            // 如果提供了一个有效的Transaction对象,则把该Transaction对象付给Command对象
            if (connection != null)
            {
                // If the provided connection is not open, we will open it
                if (connection.State != ConnectionState.Open)
                {
                    mustCloseConnection = true; //内部打开的数据库连接，则在方法退出前必须关闭连接
                    connection.Open();
                }

                // Associate the connection with the command
                command.Connection = connection;
            }

            // Set the command type
            command.CommandType = commandType;
            command.CommandTimeout = CommandTimeout;

            // Attach the command parameters if they are provided
            if (commandParameters != null)
            {
                AttachParameters(command, commandParameters);
            }
        }

        protected static IDbTransaction GetTransaction(IDbConnection connection)
        {
            var tranScope = CallContext.GetData("TransactionScope");
            if (tranScope != null)
            {
                var transactionScope = tranScope as TransactionScope;
                if (transactionScope != null)
                {
                    var dbTransactions = transactionScope.Transactions as IList<IDbTransaction>;
                    if (dbTransactions != null)
                    {
                        foreach (var tran in dbTransactions)
                        {
                            if (tran.Connection != null &&
                                tran.Connection.ConnectionString.Equals(connection.ConnectionString,
                                                                        StringComparison.OrdinalIgnoreCase))
                            {
                                return tran;
                            }
                        }
                        if (connection.State != ConnectionState.Open)
                        {
                            connection.Open();
                        }
                        var transaction = connection.BeginTransaction();
                        dbTransactions.Add(transaction);
                        return transaction;
                    }
                }
            }
            return null;
        }

        protected IDbCommand GetCommand<T, TU>(T conn, string commandText, TU connParams, CommandType commandType,
                                               string conAlians, string cmdAlians,
                                               out IDataParameter[] spParameterSet, out bool mustCloseConnection)
            where T : class
            where TU : class
        {
            IDbConnection connection;
            IDbTransaction trans = null;
            if (conn is IDbConnection)
            {
                connection = conn as IDbConnection;
            }
            else if (conn is IDbTransaction)
            {
                trans = conn as IDbTransaction;
                connection = trans.Connection;
            }
            else
            {
                throw new ArgumentException(
                    "Invalid argument.", "conn");
            }
            var command = connection.CreateCommand();

            //注意：如果 CommandType 不是 StoredProcedure 且 connParams 不是 IDataParameter[]，参数将不被赋值
            //如果 CommandType 不是 StoredProcedure，connParams 就必须是 IDataParameter[]
            spParameterSet = new IDataParameter[0];
            if (commandType == CommandType.StoredProcedure &&
                (connParams == null || !(connParams is IDataParameter[])))
            {
                spParameterSet = GetSpParameterSet(connection, commandText);
            }
            if (connParams != null)
            {
                if (connParams is DataRow)
                    AssignParameterValues(spParameterSet, connParams as DataRow);
                else if (connParams is IDataParameter[])
                    spParameterSet = connParams as IDataParameter[];
                else if (connParams is object[])
                    AssignParameterValues(spParameterSet, connParams as object[]);
                else
                    AssignParameterValues(spParameterSet, connParams, conAlians, cmdAlians);
            }
            if (trans != null)
            {
                PrepareCommand(command, null, commandType, commandText, spParameterSet,
                               out mustCloseConnection);
                command.Transaction = trans;
            }
            else
            {
                PrepareCommand(command, connection, commandType, commandText, spParameterSet,
                               out mustCloseConnection);
            }
            return command;
        }

        /// <summary>
        ///   This method clears (if necessary) the connection, transaction, command type and parameters from the provided command
        /// </summary>
        /// <remarks>
        ///   Not implemented here because the behavior of this method differs on each data provider.
        /// </remarks>
        /// <param name="command"> The IDbCommand to be cleared </param>
        protected virtual void ClearCommand(IDbCommand command)
        {
        }

        #endregion private utility methods

        #region ExecuteDataset

        public virtual DataSet ExecuteDataset<T, TU>(T conn, string commandText, TU connParams, CommandType commandType,
                                                     out List<IDataParameter> parameters, string conAlians = null,
                                                     string cmdAlians = null)
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
                        ExecuteDataset(transaction, commandText, connParams, commandType, out parameters, conAlians,
                                       cmdAlians);
                    return ExecuteDataset(connection, commandText, connParams, commandType, out parameters, conAlians,
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
            IDbDataAdapter dbDataAdapter = null;
            try
            {
                dbDataAdapter = GetDataAdapter();
                dbDataAdapter.SelectCommand = command;
                var dataSet = new DataSet();
                dbDataAdapter.Fill(dataSet);
                ClearCommand(command);
                parameters = new List<IDataParameter>(spParameterSet);
                if (mustCloseConnection)
                    command.Connection.Close();
                return dataSet;
            }
            finally
            {
                if (dbDataAdapter != null)
                {
                    var disposable = dbDataAdapter as IDisposable;
                    if (disposable != null)
                        disposable.Dispose();
                }
            }
        }

        public virtual DataSet ExecuteDataset<T, TU>(T conn, string commandText, TU connParams,
                                                     out List<IDataParameter> parameters, string conAlians = null,
                                                     string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteDataset(conn, commandText, connParams, CommandType.StoredProcedure, out parameters, conAlians,
                                  cmdAlians);
        }

        public virtual DataSet ExecuteDataset<T, TU>(T conn, string commandText, CommandType commandType,
                                                     out List<IDataParameter> parameters, string conAlians = null,
                                                     string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteDataset<T, TU>(conn, commandText, null, commandType, out parameters, conAlians, cmdAlians);
        }

        public virtual DataSet ExecuteDataset<T, TU>(T conn, string commandText, out List<IDataParameter> parameters,
                                                     string conAlians = null, string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteDataset<T, TU>(conn, commandText, CommandType.StoredProcedure, out parameters, conAlians,
                                         cmdAlians);
        }

        #endregion

        #region ExecuteNonQuery

        public virtual int ExecuteNonQuery<T, TU>(T conn, string commandText, TU connParams, CommandType commandType,
                                                  out List<IDataParameter> parameters, string conAlians = null,
                                                  string cmdAlians = null)
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
                        return ExecuteNonQuery(transaction, commandText, connParams, commandType, out parameters,
                                               conAlians, cmdAlians);
                    return ExecuteNonQuery(connection, commandText, connParams, commandType, out parameters, conAlians,
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
            //PerformanceUtil.InsertPerformanceAnchor("ExecuteNonQuery前");
            int num = command.ExecuteNonQuery();
            //PerformanceUtil.InsertPerformanceAnchor("ExecuteNonQuery后");
            ClearCommand(command);
            parameters = new List<IDataParameter>(spParameterSet);
            if (mustCloseConnection)
                command.Connection.Close();
            return num;
        }

        public virtual int ExecuteNonQuery<T, TU>(T conn, string commandText, TU connParams,
                                                  out List<IDataParameter> parameters, string conAlians = null,
                                                  string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteNonQuery(conn, commandText, connParams, CommandType.StoredProcedure, out parameters, conAlians,
                                   cmdAlians);
        }

        public virtual int ExecuteNonQuery<T, TU>(T conn, string commandText, CommandType commandType,
                                                  out List<IDataParameter> parameters, string conAlians = null,
                                                  string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteNonQuery<T, TU>(conn, commandText, null, commandType, out parameters, conAlians, cmdAlians);
        }

        public virtual int ExecuteNonQuery<T, TU>(T conn, string commandText, out List<IDataParameter> parameters,
                                                  string conAlians = null, string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteNonQuery<T, TU>(conn, commandText, CommandType.StoredProcedure, out parameters, conAlians,
                                          cmdAlians);
        }

        #endregion

        #region ExecuteReader

        public virtual IDataReader ExecuteReader<T, TU>(T conn, string commandText, TU connParams,
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

                IDbConnection connection = GetConnection(connStr);
                IDbTransaction transaction = GetTransaction(connection);
                if (transaction != null)
                    return ExecuteReader(transaction, commandText, connParams, commandType, out parameters, conAlians,
                                         cmdAlians);
                return ExecuteReader(connection, commandText, connParams, commandType, out parameters, conAlians,
                                     cmdAlians);
            }
            IDataParameter[] spParameterSet;
            bool mustCloseConnection;
            var command = GetCommand(conn, commandText, connParams, commandType, conAlians, cmdAlians,
                                     out spParameterSet, out mustCloseConnection);
            try
            {
                //PerformanceUtil.InsertPerformanceAnchor("ExecuteReader前");
                IDataReader dataReader = mustCloseConnection
                                             ? command.ExecuteReader(CommandBehavior.CloseConnection)
                                             : command.ExecuteReader();
                //PerformanceUtil.InsertPerformanceAnchor("ExecuteReader后");
                ClearCommand(command);
                parameters = new List<IDataParameter>(spParameterSet);
                return dataReader;
            }
            catch
            {
                if (mustCloseConnection)
                    command.Connection.Close();
                throw;
            }
        }

        public virtual IDataReader ExecuteReader<T, TU>(T conn, string commandText, TU connParams,
                                                        out List<IDataParameter> parameters, string conAlians = null,
                                                        string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteReader(conn, commandText, connParams, CommandType.StoredProcedure, out parameters, conAlians,
                                 cmdAlians);
        }

        public virtual IDataReader ExecuteReader<T, TU>(T conn, string commandText, CommandType commandType,
                                                        out List<IDataParameter> parameters, string conAlians = null,
                                                        string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteReader<T, TU>(conn, commandText, null, commandType, out parameters, conAlians, cmdAlians);
        }

        public virtual IDataReader ExecuteReader<T, TU>(T conn, string commandText, out List<IDataParameter> parameters,
                                                        string conAlians = null, string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteReader<T, TU>(conn, commandText, CommandType.StoredProcedure, out parameters, conAlians,
                                        cmdAlians);
        }

        #endregion

        #region ExecuteScalar

        public virtual object ExecuteScalar<T, TU>(T conn, string commandText, TU connParams, CommandType commandType,
                                                   out List<IDataParameter> parameters, string conAlians = null,
                                                   string cmdAlians = null)
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
                        return ExecuteScalar(transaction, commandText, connParams, commandType, out parameters,
                                             conAlians, cmdAlians);
                    return ExecuteScalar(connection, commandText, connParams, commandType, out parameters, conAlians,
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
            //PerformanceUtil.InsertPerformanceAnchor("ExecuteScalar前");
            object obj = command.ExecuteScalar();
            //PerformanceUtil.InsertPerformanceAnchor("ExecuteScalar后");
            ClearCommand(command);
            parameters = new List<IDataParameter>(spParameterSet);
            if (mustCloseConnection)
                command.Connection.Close();
            return obj;
        }

        public virtual object ExecuteScalar<T, TU>(T conn, string commandText, TU connParams,
                                                   out List<IDataParameter> parameters, string conAlians = null,
                                                   string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteScalar(conn, commandText, connParams, CommandType.StoredProcedure, out parameters, conAlians,
                                 cmdAlians);
        }

        public virtual object ExecuteScalar<T, TU>(T conn, string commandText, CommandType commandType,
                                                   out List<IDataParameter> parameters, string conAlians = null,
                                                   string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteScalar<T, TU>(conn, commandText, null, commandType, out parameters, conAlians, cmdAlians);
        }

        public virtual object ExecuteScalar<T, TU>(T conn, string commandText, out List<IDataParameter> parameters,
                                                   string conAlians = null, string cmdAlians = null)
            where T : class
            where TU : class
        {
            return ExecuteScalar<T, TU>(conn, commandText, CommandType.StoredProcedure, out parameters, conAlians,
                                        cmdAlians);
        }

        #endregion

        #region FillDataSet

        public virtual void FillDataSet<T, TU>(T conn, string commandText, TU connParams, CommandType commandType,
                                               DataSet dataSet, string[] tableNames, out List<IDataParameter> parameters,
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
                        FillDataSet(transaction, commandText, connParams, commandType, dataSet, tableNames,
                                    out parameters, conAlians, cmdAlians);
                    FillDataSet(connection, commandText, connParams, commandType, dataSet, tableNames, out parameters,
                                conAlians, cmdAlians);
                }
                finally
                {
                    IDisposable disposable = connection;
                    if (disposable != null && transaction == null)
                        disposable.Dispose();
                }
            }
            else
            {
                IDataParameter[] spParameterSet;
                bool mustCloseConnection;
                var command = GetCommand(conn, commandText, connParams, commandType, conAlians, cmdAlians,
                                         out spParameterSet, out mustCloseConnection);
                IDbDataAdapter dbDataAdapter = null;
                try
                {
                    dbDataAdapter = GetDataAdapter();
                    dbDataAdapter.SelectCommand = command;
                    if (tableNames != null && tableNames.Length > 0)
                    {
                        const string str = "Table";
                        for (int index = 0; index < tableNames.Length; ++index)
                        {
                            if (String.IsNullOrEmpty(tableNames[index]))
                                throw new ArgumentException(
                                    "The tableNames parameter must contain a list of tables, a value was provided as null or empty string.",
                                    "tableNames");
                            dbDataAdapter.TableMappings.Add(str + (index == 0 ? "" : index.ToString()),
                                                            tableNames[index]);
                        }
                    }
                    dbDataAdapter.Fill(dataSet);
                    ClearCommand(command);
                    parameters = new List<IDataParameter>(spParameterSet);
                    if (mustCloseConnection)
                        command.Connection.Close();
                }
                finally
                {
                    if (dbDataAdapter != null)
                    {
                        var disposable = dbDataAdapter as IDisposable;
                        if (disposable != null)
                            disposable.Dispose();
                    }
                }
            }
        }

        public virtual void FillDataSet<T, TU>(T conn, string commandText, TU connParams, DataSet dataSet,
                                               string[] tableNames, out List<IDataParameter> parameters,
                                               string conAlians = null, string cmdAlians = null)
            where T : class
            where TU : class
        {
            FillDataSet(conn, commandText, connParams, CommandType.StoredProcedure, dataSet, tableNames, out parameters,
                        conAlians, cmdAlians);
        }

        public virtual void FillDataSet<T, TU>(T conn, string commandText, CommandType commandType, DataSet dataSet,
                                               string[] tableNames, out List<IDataParameter> parameters,
                                               string conAlians = null, string cmdAlians = null)
            where T : class
            where TU : class
        {
            FillDataSet<T, TU>(conn, commandText, null, commandType, dataSet, tableNames, out parameters, conAlians,
                               cmdAlians);
        }

        public virtual void FillDataSet<T, TU>(T conn, string commandText, DataSet dataSet, string[] tableNames,
                                               out List<IDataParameter> parameters, string conAlians = null,
                                               string cmdAlians = null)
            where T : class
            where TU : class
        {
            FillDataSet<T, TU>(conn, commandText, CommandType.StoredProcedure, dataSet, tableNames, out parameters,
                               conAlians, cmdAlians);
        }

        public virtual void UpdateDataset(IDbCommand insertCommand, IDbCommand deleteCommand, IDbCommand updateCommand,
                                          DataSet dataSet, string tableName)
        {
            if (insertCommand == null)
                throw new ArgumentNullException("insertCommand");
            if (deleteCommand == null)
                throw new ArgumentNullException("deleteCommand");
            if (updateCommand == null)
                throw new ArgumentNullException("updateCommand");
            if (String.IsNullOrEmpty(tableName))
                throw new ArgumentNullException("tableName");
            IDbDataAdapter dbDataAdapter = null;
            try
            {
                bool mustCloseConnection;
                dbDataAdapter = GetDataAdapter();
                var commandParameters1 = new IDataParameter[updateCommand.Parameters.Count];
                updateCommand.Parameters.CopyTo(commandParameters1, 0);
                updateCommand.Parameters.Clear();
                PrepareCommand(updateCommand, updateCommand.Connection, updateCommand.CommandType,
                               updateCommand.CommandText, commandParameters1, out mustCloseConnection);
                dbDataAdapter.UpdateCommand = updateCommand;
                var commandParameters2 = new IDataParameter[insertCommand.Parameters.Count];
                insertCommand.Parameters.CopyTo(commandParameters2, 0);
                insertCommand.Parameters.Clear();
                PrepareCommand(insertCommand, insertCommand.Connection, insertCommand.CommandType,
                               insertCommand.CommandText, commandParameters2, out mustCloseConnection);
                dbDataAdapter.InsertCommand = insertCommand;
                var commandParameters3 = new IDataParameter[deleteCommand.Parameters.Count];
                deleteCommand.Parameters.CopyTo(commandParameters3, 0);
                deleteCommand.Parameters.Clear();
                PrepareCommand(deleteCommand, deleteCommand.Connection, deleteCommand.CommandType,
                               deleteCommand.CommandText, commandParameters3, out mustCloseConnection);
                dbDataAdapter.DeleteCommand = deleteCommand;
                if (dbDataAdapter is DbDataAdapter)
                {
                    ((DbDataAdapter) dbDataAdapter).Update(dataSet, tableName);
                }
                else
                {
                    dbDataAdapter.TableMappings.Add(tableName, "Table");
                    dbDataAdapter.Update(dataSet);
                }
                dataSet.AcceptChanges();
            }
            finally
            {
                var disposable = dbDataAdapter as IDisposable;
                if (disposable != null)
                    disposable.Dispose();
            }
        }

        public virtual IDbCommand CreateCommand(IDbConnection connection, string spName, params string[] sourceColumns)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (String.IsNullOrEmpty(spName))
                throw new ArgumentNullException("spName");
            IDbCommand command = connection.CreateCommand();
            command.CommandText = spName;
            command.CommandType = CommandType.StoredProcedure;
            if (sourceColumns != null && sourceColumns.Length > 0)
            {
                IDataParameter[] spParameterSet = GetSpParameterSet(connection, spName);
                for (int index = 0; index < sourceColumns.Length; ++index)
                    spParameterSet[index].SourceColumn = sourceColumns[index];
                AttachParameters(command, spParameterSet);
            }
            return command;
        }

        #endregion

        #region 自动获取参数

        /// <summary>
        ///   获取数据库命令参数
        /// </summary>
        /// <param name="connectionString"> 连接字符串 </param>
        /// <param name="commandText"> SQL命令语句 </param>
        /// <param name="includeReturnValueParameter"> 是否包含返回参数 </param>
        /// <param name="commandType"> 命令类型 </param>
        /// <returns> </returns>
        public IDataParameter[] GetSpParameterSet(string connectionString, string commandText,
                                                  bool includeReturnValueParameter = false,
                                                  CommandType commandType = CommandType.StoredProcedure)

        {
            if (String.IsNullOrEmpty(connectionString))
                throw new ArgumentNullException("connectionString");
            using (IDbConnection connection = GetConnection(connectionString))
                return GetSpParameterSet(connection, commandText, includeReturnValueParameter, commandType);
        }

        /// <summary>
        ///   获取数据库命令参数
        /// </summary>
        /// <param name="connection"> 数据库连接 </param>
        /// <param name="commandText"> SQL命令语句 </param>
        /// <param name="includeReturnValueParameter"> 是否包含返回参数 </param>
        /// <param name="commandType"> 命令类型 </param>
        /// <returns> </returns>
        public IDataParameter[] GetSpParameterSet(IDbConnection connection, string commandText,
                                                  bool includeReturnValueParameter = false,
                                                  CommandType commandType = CommandType.StoredProcedure)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (String.IsNullOrEmpty(commandText))
                throw new ArgumentNullException("commandText");
            string str = commandText +
                         (includeReturnValueParameter ? ":include ReturnValue Parameter" : "");
            IDataParameter[] dataParameterArray =
                ADOHelperParameterCache.GetCachedParameterSet(connection.ConnectionString, str);
            if (dataParameterArray == null && commandType == CommandType.StoredProcedure)
            {
                if (!(connection is ICloneable))
                    throw new ArgumentException(
                        "can't discover parameters if the connection doesn't implement the ICloneable interface",
                        "connection");
                IDataParameter[] originalParameters =
                    DiscoverSpParameterSet((IDbConnection) ((ICloneable) connection).Clone(), commandText,
                                           includeReturnValueParameter);
                ADOHelperParameterCache.CacheParameterSet(connection.ConnectionString, str, originalParameters);
                dataParameterArray = ADOHelperParameterCache.CloneParameters(originalParameters);
            }
            return dataParameterArray;
        }

        /// <summary>
        ///   手动缓存SQL命令的参数
        /// </summary>
        /// <param name="connection"> 数据库连接 </param>
        /// <param name="commandText"> SQL命令语句 </param>
        /// <param name="originalParameters"> 参数列表 </param>
        /// <param name="includeReturnValueParameter"> 是否包含返回参数 </param>
        public void CacheParameterSet(IDbConnection connection, string commandText, IDataParameter[] originalParameters,
                                      bool includeReturnValueParameter = false)
        {
            if (connection == null)
                throw new ArgumentNullException("connection");
            if (String.IsNullOrEmpty(commandText))
                throw new ArgumentNullException("commandText");
            string str = commandText +
                         (includeReturnValueParameter ? ":include ReturnValue Parameter" : "");

            ADOHelperParameterCache.CacheParameterSet(connection.ConnectionString, str,
                                                      ADOHelperParameterCache.CloneParameters(originalParameters, true));
        }

        /// <summary>
        ///   自动检测存储过程参数
        /// </summary>
        /// <param name="connection"> 数据库连接 </param>
        /// <param name="spName"> 存储过程名 </param>
        /// <param name="includeReturnValueParameter"> 是否包含返回参数 </param>
        /// <returns> </returns>
        protected virtual IDataParameter[] DiscoverSpParameterSet(IDbConnection connection, string spName,
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
            if (!includeReturnValueParameter)
                command.Parameters.RemoveAt(0);
            var dataParameterArray = new IDataParameter[command.Parameters.Count];
            command.Parameters.CopyTo(dataParameterArray, 0);
            foreach (IDataParameter dataParameter in dataParameterArray)
                dataParameter.Value = DBNull.Value;
            return dataParameterArray;
        }

        #endregion

        #region 参数相关

        public virtual IDataParameter GetInStringPara(string parameterName, string paraValue)
        {
            return GetParameter(parameterName, DbType.String, paraValue);
        }

        public virtual IDataParameter GetInIntegerPara(string parameterName, int paraValue)
        {
            return GetParameter(parameterName, DbType.Int32, paraValue);
        }

        public virtual IDataParameter GetOutParameter(string parameterName, DbType dbType)
        {
            return GetParameter(parameterName, dbType, ParameterDirection.Output);
        }

        public virtual IDataParameter GetOutParameter(string parameterName, DbType dbType, int size)
        {
            return GetParameter(parameterName, dbType, ParameterDirection.Output, size);
        }

        public virtual IDataParameter GetReturnParameter(string paramName)
        {
            return GetParameter(paramName, DbType.Int32, ParameterDirection.ReturnValue);
        }

        public virtual IDataParameter GetReturnParameter()
        {
            return GetParameter("ReturnValue", DbType.Int32, ParameterDirection.ReturnValue);
        }

        #region 标准加参数方法

        public abstract IDataParameter GetParameter();

        public abstract IDataParameter GetParameter(string parameterName);

        /// <summary>
        ///   根据参数名称和参数值，取得IDataParameter实例
        /// </summary>
        /// <param name="name"> 参数名称 </param>
        /// <param name="value"> 参数值 </param>
        /// <returns> IDataparameter参数实例 </returns>
        public virtual IDataParameter GetParameter(string name, object value)
        {
            IDataParameter parameter = GetParameter();
            parameter.ParameterName = name;
            parameter.Value = value;
            return parameter;
        }

        public abstract IDataParameter GetParameter(string parameterName, DbType dbType);

        public abstract IDataParameter GetParameter(string parameterName, DbType dbType, object paramValue);

        public abstract IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection);

        public abstract IDataParameter GetParameter(string parameterName, DbType dbType,
                                                    ParameterDirection paramDirection, int size);

        #endregion

        #endregion
    }
}