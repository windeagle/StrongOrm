using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using StrongCutIn.Util;

namespace DianPing.BA.Framework.DAL.DACBase
{
    internal static class DACBaseObject
    {
        private static readonly DACBase<object> DACBase = new DACBase<object>();

        public static int Insert(object record)
        {
            return DACBase.InsertCascade(record);
        }

        public static void Update(object record)
        {
            DACBase.UpdateCascade(record);
        }
    }

    /// <summary>
    ///   通用的数据库操作类
    /// </summary>
    /// <typeparam name="T"> 对应的实体类型 </typeparam>
    public class DACBase<T> where T : class
    {
        public DACBase()
        {
            DataBaseInstance = DataBase.Instance;
        }

        /// <summary>
        ///   当前实例所使用的数据库访问信息（未设定时为默认数据库访问信息）
        /// </summary>
        public DataBase DataBaseInstance { get; set; }

        /// <summary>
        ///   向数据库插入实体，根据实体类型和命令配置信息自动生成SQL语句 如果实体有既非值类型又非接口的属性，则当作子数据记录处理，当实体插入数据库成功后，级联向数据库对应的表插入这些子数据记录
        /// </summary>
        /// <param name="record"> 实体 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Insert + 实体类型名 </param>
        /// <returns> 返回插入记录的整型identity，失败则返回0 </returns>
        [Obsolete("不建议使用，可能有问题")]
        public int InsertCascade(T record, string cmdAlians = null)
        {
            var ret = Insert(record, record.GetType(), cmdAlians);

            if (ret != 0)
            {
                var list = DbUtil.GetProperties(record.GetType());
                if (list != null)
                {
                    foreach (PropertyInfo propertyInfo in list)
                    {
                        if (!propertyInfo.PropertyType.IsEnum && propertyInfo.PropertyType != typeof (string) &&
                            propertyInfo.PropertyType.IsClass)
                        {
                            object fieldValue = TypeUtility.GetMemberGetDelegate(propertyInfo)(record);
                            if (fieldValue != null)
                            {
                                try
                                {
                                    DACBaseObject.Insert(fieldValue);
                                }
                                catch (Exception e)
                                {
                                    //TODO; 记了些日志
                                    throw new Exception("级联添加出错", e);
                                }
                            }
                        }
                    }
                }
            }

            return ret;
        }

        private int Insert(object record, Type type, string cmdAlians = null)
        {
            if (record.GetType() != type)
                throw new Exception("级联添加类型不匹配");

            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Insert" + type.Name;
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(type, ado);
            comtext += "select @@identity";

            List<IDataParameter> outP;
            Param p = ado.CreateParam() as Param;
            p.GetParams(record);
            var excludeParams = p.Where(param => !comtext.ToLower().Contains(param.ParameterName.ToLower())).ToList();
            var needP = p.Except(excludeParams);

            AdoHelper.AssignParameterValues(needP.ToArray(), record, connAlians, cmdAlians);
            var obj = ado.ExecuteScalar(conStr, comtext, needP.ToArray(), CommandType.Text, out outP,
                                        connAlians, cmdAlians);
            int ret;
            return !int.TryParse(obj.ToString(), out ret) ? 0 : ret;
        }

        /// <summary>
        ///   更新实体到数据库，根据实体类型和命令配置信息自动生成SQL语句 如果实体有既非值类型又非接口的属性，则当作子数据记录处理，当实体更新数据库成功后，级联向数据库对应的表更新这些子数据记录
        /// </summary>
        /// <param name="record"> 实体 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Update + 实体类型名 </param>
        /// <returns> </returns>
        [Obsolete("不建议使用，可能有问题")]
        public int UpdateCascade(T record, string cmdAlians = null)
        {
            var ret = Update(record, record.GetType(), cmdAlians);

            var list = DbUtil.GetProperties(record.GetType());
            if (list != null)
            {
                foreach (PropertyInfo propertyInfo in list)
                {
                    if (!propertyInfo.PropertyType.IsEnum && propertyInfo.PropertyType != typeof (string) &&
                        propertyInfo.PropertyType.IsClass)
                    {
                        object fieldValue = TypeUtility.GetMemberGetDelegate(propertyInfo)(record);
                        if (fieldValue != null)
                        {
                            try
                            {
                                DACBaseObject.Update(fieldValue);
                            }
                            catch (Exception e)
                            {
                                //TODO; 记了些日志
                                throw new Exception("级联更新出错", e);
                            }
                        }
                    }
                }
            }

            return ret;
        }

        private int Update(object record, Type type, string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Update" + type.Name;
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.GetParams(record);

            var excludeParams = p.Where(param => !comtext.ToLower().Contains(param.ParameterName.ToLower())).ToList();
            var needP = p.Except(excludeParams);

            AdoHelper.AssignParameterValues(needP.ToArray(), record, connAlians, cmdAlians);
            return ado.ExecuteNonQuery(conStr, comtext, needP.ToArray(), CommandType.Text, out outP,
                                       connAlians, cmdAlians);
        }

        /// <summary>
        ///   向数据库插入实体，根据实体类型和命令配置信息自动生成SQL语句
        /// </summary>
        /// <param name="record"> 实体 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Insert + 实体类型名 </param>
        /// <returns> 返回插入记录的整型identity，失败则返回0 </returns>
        public int Insert(T record, string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Insert" + typeof (T).Name;
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(typeof (T), ado);
            comtext += "select @@identity";

            List<IDataParameter> outP;
            Param p = ado.CreateParam() as Param;
            p.GetParams(record);
            var excludeParams = p.Where(param => !comtext.ToLower().Contains(param.ParameterName.ToLower())).ToList();
            var needP = p.Except(excludeParams);

            AdoHelper.AssignParameterValues(needP.ToArray(), record, connAlians, cmdAlians);
            var obj = ado.ExecuteScalar(conStr, comtext, needP.ToArray(), CommandType.Text, out outP,
                                        connAlians, cmdAlians);
            int ret;
            return !int.TryParse(obj.ToString(), out ret) ? 0 : ret;
        }

        /// <summary>
        ///   删除某列的值等于某个值的记录
        /// </summary>
        /// <typeparam name="TK"> 列类型 </typeparam>
        /// <param name="columnParamName"> 列名 </param>
        /// <param name="paramValue"> 列的条件值 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Delete + 实体类型名 + By + 列名 </param>
        /// <returns> </returns>
        [Obsolete("不建议使用，请使用Delete方法")]
        public void DeleteWithoutReturn<TK>(string columnParamName, TK paramValue, string cmdAlians = null)
        {
            Delete(columnParamName, paramValue, cmdAlians);
        }

        /// <summary>
        ///   删除某列的值等于某个值的记录，返回影响的记录行数
        /// </summary>
        /// <typeparam name="TK"> 列类型 </typeparam>
        /// <param name="columnParamName"> 列名 </param>
        /// <param name="paramValue"> 列的条件值 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Delete + 实体类型名 + By + 列名 </param>
        /// <returns> </returns>
        public int Delete<TK>(string columnParamName, TK paramValue, string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Delete" + typeof (T).Name + "By" + columnParamName;
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.AddIn(columnParamName, paramValue);
            return ado.ExecuteNonQuery(conStr, comtext, p.ToArray(), CommandType.Text, out outP,
                                       connAlians, cmdAlians);
        }

        /// <summary>
        ///   更新实体到数据库
        /// </summary>
        /// <param name="record"> 实体 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Update + 实体类型名 </param>
        /// <returns> </returns>
        [Obsolete("不建议使用，请使用Update方法")]
        public void UpdateWithoutReturn(T record, string cmdAlians = null)
        {
            Update(record, cmdAlians);
        }

        /// <summary>
        ///   更新实体到数据库, 返回影响的行数
        /// </summary>
        /// <param name="record"> 实体 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Update + 实体类型名 </param>
        /// <returns> </returns>
        public int Update(T record, string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Update" + typeof (T).Name;
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.GetParams(record);

            var excludeParams = p.Where(param => !comtext.ToLower().Contains(param.ParameterName.ToLower())).ToList();
            var needP = p.Except(excludeParams);

            AdoHelper.AssignParameterValues(needP.ToArray(), record, connAlians, cmdAlians);
            return ado.ExecuteNonQuery(conStr, comtext, needP.ToArray(), CommandType.Text, out outP,
                                       connAlians, cmdAlians);
        }

        /// <summary>
        ///   操作满足一组条件的所有记录，无返回
        /// </summary>
        /// <param name="paramList"> 列的条件字典，key为列名，value为列的条件值 </param>
        /// <param name="cmdAlians"> 命令别名 </param>
        /// <returns> </returns>
        public void Execute(IDictionary<string, object> paramList, string cmdAlians)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.AddRange(paramList.Select(param => ado.GetParam(param.Key, param.Value.GetType())));
            AdoHelper.AssignParameterValues(p.ToArray(), paramList.Values.ToArray());
            ado.ExecuteNonQuery(conStr, comtext, p.ToArray(), CommandType.Text, out outP,
                                connAlians, cmdAlians);
        }

        /// <summary>
        ///   操作满足一组条件的所有记录，返回影响的行数
        /// </summary>
        /// <param name="paramList"> 列的条件字典，key为列名，value为列的条件值 </param>
        /// <param name="cmdAlians"> 命令别名 </param>
        /// <returns> </returns>
        public int ExecuteWithReturn(IDictionary<string, object> paramList, string cmdAlians)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.AddRange(paramList.Select(param => ado.GetParam(param.Key, param.Value.GetType())));
            AdoHelper.AssignParameterValues(p.ToArray(), paramList.Values.ToArray());
            return ado.ExecuteNonQuery(conStr, comtext, p.ToArray(), CommandType.Text, out outP,
                                       connAlians, cmdAlians);
        }

        /// <summary>
        ///   无参数条件查询所有记录
        /// </summary>
        /// <param name="cmdAlians"> 命令别名，未指定则为Select + 实体类型名 </param>
        /// <returns> </returns>
        public IEnumerable<T> SelectAll(string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Select" + typeof (T).Name;
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            return ado.QuerySqlForList<T, string, T>(conStr, comtext, null, null, out outP,
                                                     connAlians, cmdAlians);
        }

        /// <summary>
        ///   查询某列的值等于某个值的单条记录
        /// </summary>
        /// <typeparam name="TK"> 列类型 </typeparam>
        /// <param name="columnParamName"> 列名 </param>
        /// <param name="paramValue"> 列的条件值 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Get + 实体类型名 + By + 列名 </param>
        /// <returns> </returns>
        public T GetByColumn<TK>(string columnParamName, TK paramValue, string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Get" + typeof (T).Name + "By" + columnParamName;
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.AddIn(columnParamName, paramValue);
            return ado.QuerySqlForEntity<T, string, IDataParameter[]>(conStr, comtext, p.ToArray(), null, out outP,
                                                                      connAlians, cmdAlians);
        }

        /// <summary>
        ///   查询满足一组条件的单条记录
        /// </summary>
        /// <param name="paramList"> 列的条件字典，key为列名，value为列的条件值 </param>
        /// <param name="cmdAlians"> 命令别名 </param>
        /// <returns> </returns>
        public T GetByColumn(IDictionary<string, object> paramList, string cmdAlians)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.AddRange(paramList.Select(param => ado.GetParam(param.Key, param.Value.GetType())));
            AdoHelper.AssignParameterValues(p.ToArray(), paramList.Values.ToArray());
            return ado.QuerySqlForEntity<T, string, IDataParameter[]>(conStr, comtext, p.ToArray(), null, out outP,
                                                                      connAlians, cmdAlians);
        }

        /// <summary>
        ///   查询满足一组条件的所有记录
        /// </summary>
        /// <param name="paramList"> 列的条件字典，key为列名，value为列的条件值 </param>
        /// <param name="cmdAlians"> 命令别名 </param>
        /// <returns> </returns>
        public IEnumerable<T> GetListByColumn(IDictionary<string, object> paramList, string cmdAlians)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.AddRange(paramList.Select(param => ado.GetParam(param.Key, param.Value.GetType())));
            AdoHelper.AssignParameterValues(p.ToArray(), paramList.Values.ToArray());
            return ado.QuerySqlForList<T, string, IDataParameter[]>(conStr, comtext, p.ToArray(), null, out outP,
                                                                    connAlians, cmdAlians);
        }

        /// <summary>
        ///   查询某列的值等于某个值的所有记录
        /// </summary>
        /// <typeparam name="TK"> 列类型 </typeparam>
        /// <param name="columnParamName"> 列名 </param>
        /// <param name="paramValue"> 列的条件值 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Get + 实体类型名 + By + 列名 </param>
        /// <returns> </returns>
        public IEnumerable<T> GetListByColumn<TK>(string columnParamName, TK paramValue, string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Get" + typeof (T).Name + "By" + columnParamName;
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.AddIn(columnParamName, paramValue);
            return ado.QuerySqlForList<T, string, IDataParameter[]>(conStr, comtext, p.ToArray(), null, out outP,
                                                                    connAlians, cmdAlians);
        }

        /// <summary>
        ///   查询某列的值不等于某个值的所有记录
        /// </summary>
        /// <typeparam name="TK"> 列类型 </typeparam>
        /// <param name="columnParamName"> 列名 </param>
        /// <param name="paramValue"> 列的条件值 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为GetOther + 实体类型名 + By + 列名 </param>
        /// <returns> </returns>
        public IEnumerable<T> GetOtherByColumn<TK>(string columnParamName, TK paramValue, string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "GetOther" + typeof (T).Name + "By" + columnParamName;
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            var comtext = com.RealCommandText(null);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.AddIn(columnParamName, paramValue);
            return ado.QuerySqlForList<T, string, IDataParameter[]>(conStr, comtext, p.ToArray(), null, out outP,
                                                                    connAlians, cmdAlians);
        }

        /// <summary>
        ///   查询某列的值在列表内的所有记录
        /// </summary>
        /// <param name="columnParamName"> 列名 </param>
        /// <param name="paramValue"> 列的值列表 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Get + 实体类型名 + By + 列名 + InList </param>
        /// <returns> </returns>
        [Obsolete("不推荐使用，只支持in语句的列表类型为带引号的")]
        public IEnumerable<T> GetByColumnInList(string columnParamName, string paramValue,
                                                string cmdAlians = null)
        {
            return GetByColumnInList(columnParamName, paramValue.Split(new[] {','}), cmdAlians);
        }

        /// <summary>
        ///   查询某列的值不在列表内的所有记录
        /// </summary>
        /// <param name="columnParamName"> 列名 </param>
        /// <param name="paramValue"> 列的值列表 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Get + 实体类型名 + By + 列名 + NotInList </param>
        /// <returns> </returns>
        [Obsolete("不推荐使用，只支持in语句的列表类型为带引号的")]
        public IEnumerable<T> GetByColumnNotInList(string columnParamName, string paramValue,
                                                   string cmdAlians = null)
        {
            return GetByColumnNotInList(columnParamName, paramValue.Split(new[] {','}), cmdAlians);
        }

        /// <summary>
        ///   查询某列的值在列表内的所有记录
        /// </summary>
        /// <typeparam name="TK"> 列类型 </typeparam>
        /// <param name="columnParamName"> 列名 </param>
        /// <param name="paramValue"> 列的值列表 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Get + 实体类型名 + By + 列名 + InList </param>
        /// <returns> </returns>
        public IEnumerable<T> GetByColumnInList<TK>(string columnParamName, IEnumerable<TK> paramValue,
                                                    string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Get" + typeof (T).Name + "By" + columnParamName + "InList";
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            string listStr = SplitSourceInDc(paramValue, true, columnParamName);
            var contextObj = new
                                 {
                                     List = listStr
                                 };
            string comtext = com.CommandText;
            com.FormatCommandText(ref comtext, new Dictionary<string, object> {{"Obj", contextObj}}, false);
            List<IDataParameter> outP;

            return ado.QuerySqlForList<T, string, IDataParameter[]>(conStr, comtext, null, null, out outP,
                                                                    connAlians, cmdAlians);
        }

        /// <summary>
        ///   查询某列的值不在列表内的所有记录
        /// </summary>
        /// <typeparam name="TK"> 列类型 </typeparam>
        /// <param name="columnParamName"> 列名 </param>
        /// <param name="paramValue"> 列的值列表 </param>
        /// <param name="cmdAlians"> 命令别名，未指定则为Get + 实体类型名 + By + 列名 + NotInList </param>
        /// <returns> </returns>
        public IEnumerable<T> GetByColumnNotInList<TK>(string columnParamName, IEnumerable<TK> paramValue,
                                                       string cmdAlians = null)
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(cmdAlians))
                cmdAlians = "Get" + typeof (T).Name + "By" + columnParamName + "NotInList";
            var com = AdoHelper.GetCommandInfo(connAlians, cmdAlians);
            string listStr = SplitSourceInDc(paramValue, false, columnParamName);
            var contextObj = new
                                 {
                                     List = listStr
                                 };
            string comtext = com.CommandText;
            com.FormatCommandText(ref comtext, new Dictionary<string, object> {{"Obj", contextObj}}, false);
            List<IDataParameter> outP;

            return ado.QuerySqlForList<T, string, IDataParameter[]>(conStr, comtext, null, null, out outP,
                                                                    connAlians, cmdAlians);
        }

        /// <summary>
        ///   生成in语句
        /// </summary>
        /// <typeparam name="TK"> in语句的列表里的项类型 </typeparam>
        /// <param name="list"> in语句的列表 </param>
        /// <param name="inOrNot"> 是in查询还是not in查询 </param>
        /// <param name="field"> in语句的字段 </param>
        /// <param name="manageSizeEachOnce"> 每个in语句的列表最多几个项，最多1000个 </param>
        /// <returns> 当list里的项多于manageSizeEachOnce指定的值时，自动分成多个in语句 </returns>
        public static string SplitSourceInDc<TK>(IEnumerable<TK> list, bool inOrNot, string field,
                                                 int manageSizeEachOnce = 0)
        {
            int size = 1000;
            // 如果指定了每次处理的数量,没有写默认为1000
            if (manageSizeEachOnce != 0)
                size = manageSizeEachOnce;
            if (list == null) return "1=0";

            var theList = list.ToList();
            //if (typeof (TK).IsClass)
            //{ 
            var sqlInjectValue =
                theList.FirstOrDefault(v => v != null && !CommandInfo.ProcessSqlStr(v.ToString()));
            if (sqlInjectValue != null && !sqlInjectValue.Equals(default(TK)))
                throw new Exception("发现有SQL注入攻击代码" + field + ":" + sqlInjectValue);
            //}

            var count = theList.Count;
            if (count > 0)
            {
                var firstItem = theList.First();
                var addQuotation = (firstItem is DateTime ||
                                    firstItem is string ||
                                    firstItem is Guid);
                Type tmpType = firstItem.GetType();
                bool isEnum = tmpType.IsEnum;

                // 总共分成totalPage数个in (not in)语句
                int totalPage = count/size + (count%size > 0 ? 1 : 0);
                StringBuilder sbOuter = new StringBuilder();
                sbOuter.Append("(");

                for (int i = 0; i < totalPage; i++)
                {
                    // 起始list的记录
                    int startIndexOfList = i*size;
                    // 结束list的记录
                    int pagecount = size;// (i + 1) * size > count ? count - i * size : size;
                    IEnumerable<TK> sublist = theList.Skip(startIndexOfList).Take(pagecount);
                    StringBuilder sb = new StringBuilder();
                    foreach (var i1 in sublist)
                    {
                        if(isEnum)
                            sb.Append((addQuotation ? "'" : "") + (int)(Enum.Parse(tmpType, i1.ToString())) + (addQuotation ? "'" : "") + ",");
                        else
                        sb.Append((addQuotation ? "'" : "") + i1 + (addQuotation ? "'" : "") + ",");
                    }
                    if (i != 0)
                        sbOuter.Append(inOrNot ? " or " : " and ");
                    sbOuter.Append(field + " " + (inOrNot ? "in" : "not in") + " (" + sb.ToString().TrimEnd(new[] {','}) +
                                   ")");
                }

                sbOuter.Append(")");
                return sbOuter.ToString();
            }
            return "1=0";
        }
    }
}