using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;

namespace DianPing.BA.Framework.DAL
{
    public static class DbUtil
    {
        public static AdoHelper DefaultADO = AdoHelper.CreateHelper(DbProvideType.SqlServer);

        private static readonly IDictionary<string, IEnumerable<PropertyInfo>> PropertyList =
            new Dictionary<string, IEnumerable<PropertyInfo>>();

        private static readonly object LockObjPropertyList = new object();

        static DbUtil()
        {
        }

        /// <summary>
        ///   NOTE: 不建议直接使用因为不是从缓存获取
        /// </summary>
        /// <returns> </returns>
        public static List<IDataParameter> CreateOracleParam()
        {
            return new Param(DbProvideType.Oracle);
        }

        /// <summary>
        ///   NOTE: 不建议直接使用因为不是从缓存获取
        /// </summary>
        /// <returns> </returns>
        public static List<IDataParameter> CreateSqlServerParam()
        {
            return new Param(DbProvideType.SqlServer);
        }

        public static List<IDataParameter> CreateParam(this AdoHelper helper)
        {
            return new Param
                       {
                           Ado = helper
                       };
        }

        public static IDataParameter AddReturn<T>(this List<IDataParameter> paramList)
        {
            AdoHelper adoHelper = null;
            if (paramList is Param)
                adoHelper = (paramList as Param).Ado;
            if (adoHelper == null)
                adoHelper = DefaultADO;

            Type paramType = typeof (T);
            if (paramType.IsGenericType && paramType.GetGenericTypeDefinition() == typeof(int?).GetGenericTypeDefinition())
                paramType = paramType.GetGenericArguments()[0];

            IDataParameter parameter;
            if (paramType == typeof(byte) || paramType == typeof(UInt16) || paramType == typeof(int) || paramType.IsEnum || paramType == typeof(SByte) || paramType == typeof(short))
                parameter = adoHelper.GetParameter("ReturnValue", DbType.Int32, ParameterDirection.ReturnValue);
            //else if (paramType == typeof(byte) || paramType == typeof(UInt16) || paramType == typeof(UInt32) || paramType == typeof(UInt64))
            //    parameter = adoHelper.GetParameter("ReturnValue", DbType.UInt64, ParameterDirection.ReturnValue);
            else if (paramType == typeof(UInt32) || paramType == typeof(long))
                parameter = adoHelper.GetParameter("ReturnValue", DbType.Int64, ParameterDirection.ReturnValue);
            else if (paramType == typeof(string))
            {
                if (adoHelper is Oracle)
                    parameter = adoHelper.GetParameter("ReturnValue", DbType.AnsiString, ParameterDirection.ReturnValue);
                else
                    parameter = adoHelper.GetParameter("ReturnValue", DbType.String, ParameterDirection.ReturnValue);
            }
            else if (paramType == typeof(DateTime))
                parameter = adoHelper.GetParameter("ReturnValue", DbType.DateTime, ParameterDirection.ReturnValue);
            else if (paramType == typeof(Decimal))
                parameter = adoHelper.GetParameter("ReturnValue", DbType.Decimal, ParameterDirection.ReturnValue);
            else if (paramType == typeof(float))
                parameter = adoHelper.GetParameter("ReturnValue", DbType.Single, ParameterDirection.ReturnValue);
            else if (paramType == typeof(Guid))
                parameter = adoHelper.GetParameter("ReturnValue", DbType.Guid, ParameterDirection.ReturnValue);
            else if (paramType == typeof(double))
                parameter = adoHelper.GetParameter("ReturnValue", DbType.Double, ParameterDirection.ReturnValue);
            else if (paramType == typeof(bool))
                parameter = adoHelper.GetParameter("ReturnValue", DbType.Boolean, ParameterDirection.ReturnValue);
            else
                throw new Exception("不支持Binary、Currency、Object、VarNumeric、Xml类型");
            paramList.Add(parameter);
            return parameter;
        }

        public static T GetValue<T>(this IDataRecord rec, string fldname)
        {
            int ordinal = rec.GetOrdinal(fldname);
            if (rec.IsDBNull(ordinal))
                return default (T);
            return (T) rec[ordinal];
        }

        public static T GetValue<T>(this IDataRecord rec, string fldname, T defaultValue)
        {
            int ordinal = rec.GetOrdinal(fldname);
            if (rec.IsDBNull(ordinal))
                return defaultValue;
            return (T) rec[ordinal];
        }

        public static T GetValue<T>(this IDataRecord rec, int index)
        {
            if (rec.IsDBNull(index))
                return default (T);
            return (T) rec[index];
        }

        public static T GetValue<T>(this IDataRecord rec, int index, T defaultValue)
        {
            if (rec.IsDBNull(index))
                return defaultValue;
            return (T) rec[index];
        }

        public static int GetInt(this IDataRecord rec, string fldname)
        {
            return Field.GetInt32(rec, fldname);
        }

        public static long GetLong(this IDataRecord rec, string fldname)
        {
            return Field.GetInt64(rec, fldname);
        }

        public static string GetStr(this IDataRecord rec, string fldname)
        {
            return Field.GetString(rec, fldname);
        }

        public static DateTime GetDate(this IDataRecord rec, string fldname)
        {
            return rec.GetValue<DateTime>(fldname);
        }

        public static Decimal GetDec(this IDataRecord rec, string fldname)
        {
            return rec.GetValue<decimal>(fldname);
        }

        public static IEnumerable<PropertyInfo> GetProperties<T>()
        {
            return GetProperties(typeof (T));
        }

        public static IEnumerable<PropertyInfo> GetProperties(Type type)
        {
            string typeName = type.ToString();
            if (PropertyList.ContainsKey(typeName))
                return PropertyList[typeName];
            var list = type.GetProperties().ToList();
            lock (LockObjPropertyList)
            {
                if (!PropertyList.ContainsKey(typeName))
                    PropertyList.Add(typeName, list);
            }
            return list;
        }

        #region 获取参数值

        public static T GetValue<T>(this IDataParameter param)
        {
            if (param != null && param.Value != null && param.Value != DBNull.Value)
                return (T) param.Value;
            return default (T);
        }

        public static T GetValue<T>(this IDataParameter param, T defalutValue)
        {
            if (param != null && param.Value != null && param.Value != DBNull.Value)
                return (T) param.Value;
            return defalutValue;
        }

        public static T GetParamValue<T>(this List<IDataParameter> paramList, string name)
        {
            name = name ?? string.Empty;
            if (paramList == null || paramList.Count == 0)
                throw new ArgumentException("参数不能为空");
            return paramList.Find((p => p.ParameterName.ToLower() == name.ToLower())).GetValue<T>();
        }

        public static T GetParamValue<T>(this List<IDataParameter> paramList, int index)
        {
            if (paramList == null || paramList.Count == 0)
                throw new ArgumentException("参数不能为空");
            return paramList[index].GetValue<T>();
        }

        public static T GetParamValue<T>(this List<IDataParameter> paramList, int index, T defaultValue)
        {
            if (paramList == null || paramList.Count == 0)
                throw new ArgumentException("参数不能为空");
            return paramList[index].GetValue(defaultValue);
        }

        public static T GetParamValue<T>(this List<IDataParameter> paramList, string name, T defaultValue)
        {
            name = name ?? string.Empty;
            if (paramList == null || paramList.Count == 0)
                throw new ArgumentException("参数不能为空");
            return paramList.Find((p => p.ParameterName.ToLower() == name.ToLower())).GetValue(defaultValue);
        }

        public static int GetReturnValue(this List<IDataParameter> paramList, int defaultValue)
        {
            return paramList.Find((p => p.Direction == ParameterDirection.ReturnValue)).GetValue(defaultValue);
        }

        #endregion

        #region 添加参数

        #region 添加输入参数

        public static IDataParameter GetParam<T>(string paramName, T value)
        {
            return DefaultADO.GetParam(paramName, value);
        }

        /// <summary>
        ///   TODO: 可能有问题
        /// </summary>
        /// <typeparam name="T"> </typeparam>
        /// <param name="helper"> </param>
        /// <param name="paramName"> </param>
        /// <param name="value"> </param>
        /// <returns> </returns>
        public static IDataParameter GetParam<T>(this AdoHelper helper, string paramName, T value)
        {
            AdoHelper adoHelper = helper;

            Type paramType = typeof(T);
            if (paramType.IsGenericType && paramType.GetGenericTypeDefinition() == typeof(int?).GetGenericTypeDefinition())
                paramType = paramType.GetGenericArguments()[0];

            if (paramType == typeof(byte) || paramType == typeof(UInt16) || paramType == typeof(int) || paramType.IsEnum || paramType == typeof(SByte) || paramType == typeof(short))
                return adoHelper.GetParameter(paramName, DbType.Int32, value);
            //if (paramType == typeof(byte) || paramType == typeof(UInt16) || paramType == typeof(UInt32) || paramType == typeof(UInt64))
            //    return adoHelper.GetParameter(paramName, DbType.UInt64, value);
            if (paramType == typeof(UInt32) || paramType == typeof(long))
                return adoHelper.GetParameter(paramName, DbType.Int64, value);
            if (typeof (T) == typeof (string))
            {
                if (adoHelper is Oracle)
                    return adoHelper.GetParameter(paramName, DbType.AnsiString, value);
                return adoHelper.GetParameter(paramName, DbType.String, value);
            }
            if (typeof (T) == typeof (DateTime))
                return adoHelper.GetParameter(paramName, DbType.DateTime, value);
            if (typeof (T) == typeof (Decimal))
                return adoHelper.GetParameter(paramName, DbType.Decimal, value);
            if (paramType == typeof(float))
                return adoHelper.GetParameter(paramName, DbType.Single, value);
            if (paramType == typeof(Guid))
                return adoHelper.GetParameter(paramName, DbType.Guid, value);
            if (typeof (T) == typeof (double))
                return adoHelper.GetParameter(paramName, DbType.Double, value);
            if (paramType == typeof(bool))
                return adoHelper.GetParameter(paramName, DbType.Boolean, value);
            throw new Exception("不支持Binary、Currency、Object、VarNumeric、Xml类型");
        }

        public static IDataParameter GetParam(this AdoHelper helper, string paramName, Type paramType)
        {
            AdoHelper adoHelper = helper;

            if (paramType.IsGenericType && paramType.GetGenericTypeDefinition() == typeof(int?).GetGenericTypeDefinition())
                paramType = paramType.GetGenericArguments()[0];

            if (paramType == typeof(byte) || paramType == typeof(UInt16) || paramType == typeof(int) || paramType.IsEnum || paramType == typeof(SByte) || paramType == typeof(short))
                return adoHelper.GetParameter(paramName, DbType.Int32);
            //if (paramType == typeof(byte) || paramType == typeof(UInt16) || paramType == typeof(UInt32) || paramType == typeof(UInt64))
            //    return adoHelper.GetParameter(paramName, DbType.UInt64);
            if (paramType == typeof(UInt32) || paramType == typeof(long))
                return adoHelper.GetParameter(paramName, DbType.Int64);
            if (paramType == typeof(string))
            {
                if (adoHelper is Oracle)
                    return adoHelper.GetParameter(paramName, DbType.AnsiString);
                return adoHelper.GetParameter(paramName, DbType.String);
            }
            if (paramType == typeof(DateTime))
                return adoHelper.GetParameter(paramName, DbType.DateTime);
            if (paramType == typeof(Decimal))
                return adoHelper.GetParameter(paramName, DbType.Decimal);
            if (paramType == typeof(float))
                return adoHelper.GetParameter(paramName, DbType.Single);
            if (paramType == typeof(Guid))
                return adoHelper.GetParameter(paramName, DbType.Guid);
            if (paramType == typeof(double))
                return adoHelper.GetParameter(paramName, DbType.Double);
            if (paramType == typeof(bool))
                return adoHelper.GetParameter(paramName, DbType.Boolean);
            throw new Exception("不支持Binary、Currency、Object、VarNumeric、Xml类型");
        }

        public static List<IDataParameter> GetParams<T>(this List<IDataParameter> paramList, T value)
        {
            AdoHelper helper = paramList is Param ? (paramList as Param).Ado : DefaultADO;
            foreach (PropertyInfo propertyInfo in GetProperties(value.GetType()))
            {
                IDataParameter dataParameter = helper.GetParam(propertyInfo.Name, propertyInfo.PropertyType);
                paramList.Add(dataParameter);
            }
            return paramList;
        }

        public static List<IDataParameter> AddIn<T>(this List<IDataParameter> paramList, string paramName, T value)
        {
            AdoHelper helper = null;
            if (paramList is Param)
                helper = (paramList as Param).Ado;
            IDataParameter dataParameter = helper == null
                                               ? GetParam(paramName, value)
                                               : helper.GetParam(paramName, value);
            paramList.Add(dataParameter);
            return paramList;
        }

        public static List<IDataParameter> AddInt(this List<IDataParameter> paramList, string paramName, int value)
        {
            return paramList.AddIn(paramName, value);
        }

        public static List<IDataParameter> AddString(this List<IDataParameter> paramList, string paramName, string value)
        {
            return paramList.AddIn(paramName, value);
        }

        public static List<IDataParameter> AddDate(this List<IDataParameter> paramList, string paramName, DateTime value)
        {
            return paramList.AddIn(paramName, value);
        }

        public static List<IDataParameter> AddAnsiString(this List<IDataParameter> paramList, string paramName,
                                                         string value)
        {
            AdoHelper adoHelper = null;
            if (paramList is Param)
                adoHelper = (paramList as Param).Ado;
            if (adoHelper == null)
                throw new ApplicationException();
            IDataParameter parameter = adoHelper.GetParameter(paramName, DbType.AnsiString, value);
            paramList.Add(parameter);
            return paramList;
        }

        #endregion

        #region 添加输出参数

        /// <summary> 
        /// </summary>
        /// <typeparam name="T"> </typeparam>
        /// <param name="helper"> </param>
        /// <param name="paramName"> </param>
        /// <param name="size"> </param>
        /// <returns> </returns>
        public static IDataParameter GetOutParam<T>(this AdoHelper helper, string paramName, int size)
        {
            AdoHelper adoHelper = helper;

            Type paramType = typeof(T);
            if (paramType.IsGenericType && paramType.GetGenericTypeDefinition() == typeof(int?).GetGenericTypeDefinition())
                paramType = paramType.GetGenericArguments()[0];

            if (paramType == typeof(byte) || paramType == typeof(UInt16) || paramType == typeof(int) || paramType.IsEnum || paramType == typeof(SByte) || paramType == typeof(short))
                return adoHelper.GetOutParameter(paramName, DbType.Int32);
            //if (paramType == typeof(byte) || paramType == typeof(UInt16) || paramType == typeof(UInt32) || paramType == typeof(UInt64))
            //    return adoHelper.GetOutParameter(paramName, DbType.UInt64);
            if (paramType == typeof(UInt32) || paramType == typeof(long))
                return adoHelper.GetOutParameter(paramName, DbType.Int64);
            if (paramType == typeof(string))
            {
                if (adoHelper is Oracle)
                    return size == 0 ? adoHelper.GetOutParameter(paramName, DbType.AnsiString) : adoHelper.GetOutParameter(paramName, DbType.AnsiString, size);
                return size == 0 ? adoHelper.GetOutParameter(paramName, DbType.String) : adoHelper.GetOutParameter(paramName, DbType.String, size);
            }
            if (paramType == typeof(DateTime))
                return adoHelper.GetOutParameter(paramName, DbType.DateTime);
            if (paramType == typeof(Decimal))
                return adoHelper.GetOutParameter(paramName, DbType.Decimal);
            if (paramType == typeof(float))
                return adoHelper.GetOutParameter(paramName, DbType.Single);
            if (paramType == typeof(Guid))
                return adoHelper.GetOutParameter(paramName, DbType.Guid);
            if (paramType == typeof(double))
                return adoHelper.GetOutParameter(paramName, DbType.Double);
            if (paramType == typeof(bool))
                return adoHelper.GetOutParameter(paramName, DbType.Boolean);
            throw new Exception("不支持Binary、Currency、Object、VarNumeric、Xml类型");
        }

        public static IDataParameter GetOutParam<T>(string paramName, int size)
        {
            return DefaultADO.GetOutParam<T>(paramName, size);
        }

        public static List<IDataParameter> AddOut<T>(this List<IDataParameter> paramList, string paramName, int size)
        {
            AdoHelper helper = null;
            if (paramList is Param)
                helper = (paramList as Param).Ado;
            IDataParameter dataParameter = helper == null
                                               ? GetOutParam<T>(paramName, size)
                                               : helper.GetOutParam<T>(paramName, size);
            paramList.Add(dataParameter);
            return paramList;
        }

        public static List<IDataParameter> AddOut<T>(this List<IDataParameter> paramList, string paramName)
        {
            AdoHelper helper = null;
            if (paramList is Param)
                helper = (paramList as Param).Ado;
            IDataParameter dataParameter = helper == null
                                               ? GetOutParam<T>(paramName, 0)
                                               : helper.GetOutParam<T>(paramName, 0);
            paramList.Add(dataParameter);
            return paramList;
        }

        #endregion

        #endregion
    }
}