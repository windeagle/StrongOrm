using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using Com.Dianping.Cat;
using StrongCutIn.Util;

namespace DianPing.BA.Framework.DAL
{
    public static class Query4Entity
    {
        private static readonly Dictionary<string, List<string>> SpFieldList = new Dictionary<string, List<string>>();
        private static readonly object LockObjSpField = new object();

        static Query4Entity()
        {
        }

        public static T QuerySpForEntity<T, TU, TE>(this AdoHelper helper, TU conn, string spName,
                                                    TE param, Action<object> action, out List<IDataParameter> parameters,
                                                    string conAlians = null, string cmdAlians = null)
            where T : class
            where TU : class
            where TE : class
        {
            IDataReader dataReader = null;
            if (String.IsNullOrEmpty(cmdAlians))
                cmdAlians = spName;
            try
            {
                dataReader = helper.ExecuteReader(conn, spName, param, CommandType.StoredProcedure, out parameters,
                                                  conAlians, cmdAlians);
                List<string> fieldNames;
                if (!SpFieldList.ContainsKey(spName))
                {
                    fieldNames =
                        (dataReader.GetSchemaTable().AsEnumerable().Select((row => (row[0] as string).ToLower()))).
                            ToList();
                    lock (LockObjSpField)
                    {
                        if (!SpFieldList.ContainsKey(spName))
                            SpFieldList.Add(spName, fieldNames);
                    }
                }
                else
                    fieldNames = SpFieldList[spName];
                var order = Activator.CreateInstance<T>();
                if (dataReader.Read())
                    SetValues(dataReader, ref order, fieldNames, conAlians, cmdAlians);
                else
                    order = default (T);
                if (action != null)
                    action(param);
                return order;
            }
            finally
            {
                if (dataReader != null)
                    dataReader.Close();
            }
        }

        public static T QuerySqlForEntity<T, TU, TE>(this AdoHelper helper, TU conn, string sqlText,
                                                     TE param, Action<object> action,
                                                     out List<IDataParameter> parameters, string conAlians = null,
                                                     string cmdAlians = null)
            where T : class
            where TU : class
            where TE : class
        {
            IDataReader dataReader = null;
            try
            {
                dataReader = helper.ExecuteReader(conn, sqlText, param, CommandType.Text, out parameters, conAlians,
                                                  cmdAlians);
                List<string> fieldNames;
                if (!SpFieldList.ContainsKey(sqlText))
                {
                    fieldNames =
                        (dataReader.GetSchemaTable().AsEnumerable().Select((row => (row[0] as string).ToLower()))).
                            ToList();
                    lock (LockObjSpField)
                    {
                        if (!SpFieldList.ContainsKey(sqlText))
                            SpFieldList.Add(sqlText, fieldNames);
                    }
                }
                else
                    fieldNames = SpFieldList[sqlText];
                var order = Activator.CreateInstance<T>();
                if (dataReader.Read())
                    SetValues(dataReader, ref order, fieldNames, conAlians, cmdAlians);
                else
                    order = default(T);
                if (action != null)
                    action(param);
                return order;
            }
            finally
            {
                if (dataReader != null)
                    dataReader.Close();
            }
        }

        public static List<T> QuerySpForList<T, TU, TE>(this AdoHelper helper, TU conn, string spName,
                                                        TE param, Action<object> action,
                                                        out List<IDataParameter> parameters, string conAlians = null,
                                                        string cmdAlians = null)
            where T : class
            where TU : class
            where TE : class
        {
            IDataReader dataReader = null;
            if (String.IsNullOrEmpty(cmdAlians))
                cmdAlians = spName;
            try
            {
                dataReader = helper.ExecuteReader(conn, spName, param, CommandType.StoredProcedure, out parameters,
                                                  conAlians, cmdAlians);
                List<string> fieldNames;
                if (!SpFieldList.ContainsKey(spName))
                {
                    fieldNames =
                        (dataReader.GetSchemaTable().AsEnumerable().Select((row => (row[0] as string).ToLower()))).
                            ToList();
                    lock (LockObjSpField)
                    {
                        if (!SpFieldList.ContainsKey(spName))
                            SpFieldList.Add(spName, fieldNames);
                    }
                }
                else
                    fieldNames = SpFieldList[spName];
                var list2 = new List<T>();
                while (dataReader.Read())
                {
                    var instance = Activator.CreateInstance<T>();
                    SetValues(dataReader, ref instance, fieldNames, conAlians, cmdAlians);
                    list2.Add(instance);
                }
                if (action != null)
                    action(param);
                Cat.GetProducer().LogEvent("SQL.ReturnList", "ItemCount", "0", "ItemCount=" + list2.Count);
                return list2;
            }
            finally
            {
                if (dataReader != null)
                    dataReader.Close();
            }
        }

        public static List<T> QuerySqlForList<T, TU, TE>(this AdoHelper helper, TU conn, string sqlText,
                                                         TE param, Action<object> action,
                                                         out List<IDataParameter> parameters, string conAlians = null,
                                                         string cmdAlians = null)
            where T : class
            where TU : class
            where TE : class
        {
            IDataReader dataReader = null;
            try
            {
                dataReader = helper.ExecuteReader(conn, sqlText, param, CommandType.Text, out parameters, conAlians,
                                                  cmdAlians);
                List<string> fieldNames;
                if (!SpFieldList.ContainsKey(sqlText))
                {
                    fieldNames =
                        (dataReader.GetSchemaTable().AsEnumerable().Select((row => (row[0] as string).ToLower()))).
                            ToList();
                    lock (LockObjSpField)
                    {
                        if (!SpFieldList.ContainsKey(sqlText))
                            SpFieldList.Add(sqlText, fieldNames);
                    }
                }
                else
                    fieldNames = SpFieldList[sqlText];
                var list2 = new List<T>();
                while (dataReader.Read())
                {
                    var instance = Activator.CreateInstance<T>();
                    SetValues(dataReader, ref instance, fieldNames, conAlians, cmdAlians);
                    list2.Add(instance);
                }
                if (action != null)
                    action(param);
                Cat.GetProducer().LogEvent("SQL.ReturnList", "ItemCount", "0", "ItemCount=" + list2.Count);
                return list2;
            }
            finally
            {
                if (dataReader != null)
                    dataReader.Close();
            }
        }

        private static string GetTypeFullName(Type t)
        {
            if (t.IsGenericType)
                if (t.FullName != null) return t.FullName.Substring(0, t.FullName.IndexOf('['));
            return t.FullName;
        }

        private static void SetValues<T>(IDataRecord rec, ref T order, List<string> fieldNames, string conAlians = null,
                                         string cmdAlians = null)
        {
            ReturnObject returnObject = null;
            if (cmdAlians != null)
            {
                CommandInfo cmdInfo = AdoHelper.GetCommandInfo(conAlians, cmdAlians);

                try
                {
                    returnObject = (cmdInfo != null)
                                       ? cmdInfo.GetReturnObject(GetTypeFullName(typeof (T)))
                                       : null;
                }
                catch (Exception e)
                {
                    throw new Exception("在命令" + cmdAlians + "中找不到" + GetTypeFullName(typeof (T)) + "对应的返回对象配置节", e);
                }
            }

            if (returnObject != null && returnObject.ParamMappings.Count == 1 &&
                returnObject.ParamMappings.ContainsKey("self"))
            {
                var selfField = returnObject.ParamMappings["self"];
                if (((IEnumerable<string>) fieldNames).Contains(selfField.ToLower()))
                {
                    order = (T) GetRecValue(rec, selfField);
                }
                return;
            }

            var properties = DbUtil.GetProperties<T>();
            foreach (PropertyInfo propertyInfo in properties)
            {
                string tmStr = returnObject != null &&
                               returnObject.ParamMappings.ContainsKey(propertyInfo.Name.ToLower())
                                   ? returnObject.ParamMappings[propertyInfo.Name.ToLower()]
                                   : propertyInfo.Name;
                if (((IEnumerable<string>) fieldNames).Contains(tmStr.ToLower()))
                {
                    object obj = propertyInfo.PropertyType != typeof (int)
                                     ? (propertyInfo.PropertyType != typeof (long)
                                            ? GetRecValue(rec, tmStr)
                                            : rec.GetLong(tmStr))
                                     : rec.GetInt(tmStr);
                    try
                    {
                        if (propertyInfo.PropertyType.IsEnum)
                        {
                            if (obj is bool)
                                obj = (((bool) obj) ? 1 : 0);
                            if (obj == null)
                                obj = 0;
                            obj = Enum.ToObject(propertyInfo.PropertyType, obj);
                        }

                        if (obj != null)
                            TypeUtility.GetMemberSetDelegate(propertyInfo)(order, obj);
                    }
                    catch (Exception e)
                    {
                        throw new Exception(
                            string.Format("属性设置失败，属性名：{0}, 属性映射结果集列名：{1}, 属性类型：{2}, 所设值类型：{3}", propertyInfo.Name, tmStr,
                                          propertyInfo.PropertyType, (obj != null
                                                                          ? obj.GetType().ToString()
                                                                          : "null")), e);
                    }
                }
            }
        }

        private static object GetRecValue(IDataRecord rec, string name)
        {
            int ordinal = rec.GetOrdinal(name);
            if (ordinal < 0)
                return null;
            return rec.GetValue(ordinal) == DBNull.Value ? null : rec.GetValue(ordinal);
        }
    }
}