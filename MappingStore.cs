using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Web;
using System.Xml;
using DianPing.BA.Framework.DAL.DACBase;
using NVelocity;
using NVelocity.App;
using StrongCutIn.Util;

namespace DianPing.BA.Framework.DAL
{
    /// <summary>
    ///   数据访问层配置根
    /// </summary>
    /// <remarks>
    ///   可以有多个数据访问层配置文件，但是都必须在App_Data\MappingFiles目录下，该目录下不得有其他文件
    /// </remarks>
    public class MappingStore
    {
        public static MappingStore TheInstance = new MappingStore();
        private readonly Dictionary<string, MappingInfo> _mappings = new Dictionary<string, MappingInfo>();

        /// <summary>
        ///   mapping one file
        /// </summary>
        /// <param name="mappingFile"> </param>
        public MappingStore(string mappingFile)
        {
            var path = string.Empty;
            if (HttpContext.Current != null)
            {
                path = HttpContext.Current.Server.MapPath("/bin");
                path = Path.Combine(path, "App_Data");
            }
            if (string.IsNullOrEmpty(path))
            {
                path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "App_Data");
                if (!File.Exists(Path.Combine(path,
                                              @"MappingFiles\" + mappingFile)))
                {
                    path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"bin\App_Data");
                }
            }

            string mappingPath = Path.Combine(path,
                                              @"MappingFiles\" + mappingFile);

            var doc = new XmlDocument {PreserveWhitespace = false};
            doc.Load(mappingPath);

            XmlNodeList mappings = doc.SelectNodes("//object.mappings/object.mapping");
            if (mappings != null)
                foreach (XmlNode mapping in mappings)
                {
                    string mappingData = mapping.OuterXml;
                    var mappingInfo = new MappingInfo(mappingData);

                    _mappings.Add(mappingInfo.For.ToLower(), mappingInfo);
                }
        }

        /// <summary>
        ///   mapping all files
        /// </summary>
        private MappingStore()
        {
            try
            {
                var path = string.Empty;
                if (HttpContext.Current != null)
                {
                    path = HttpContext.Current.Server.MapPath("/bin");
                    path = Path.Combine(path, "App_Data");
                }
                if (string.IsNullOrEmpty(path))
                {
                    path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "App_Data");
                    if (!Directory.Exists(Path.Combine(path,
                                                  @"MappingFiles")))
                    {
                        path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"bin\App_Data");
                    }
                    else if (!new DirectoryInfo(Path.Combine(path, @"MappingFiles")).GetFiles().Any())
                    {
                        path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"bin\App_Data");
                    }
                }

                string mappingPath = Path.Combine(path, @"MappingFiles");
                var dirInfo = new DirectoryInfo(mappingPath);
                FileInfo[] files = dirInfo.GetFiles();

                foreach (FileInfo file in files)
                {
                    var doc = new XmlDocument {PreserveWhitespace = false};
                    doc.Load(file.FullName);

                    XmlNodeList mappings = doc.SelectNodes("//object.mappings/object.mapping");
                    if (mappings != null)
                        foreach (XmlNode mapping in mappings)
                        {
                            string mappingData = mapping.OuterXml;
                            var mappingInfo = new MappingInfo(mappingData);

                            _mappings.Add(mappingInfo.For.ToLower(), mappingInfo);
                        }
                }
            }
            catch (Exception e)
            {
                throw new Exception("MappingStore初始化出错", e);
            }
        }

        /// <summary>
        ///   根据数据库连接别名返回数据库连接配置信息
        /// </summary>
        /// <param name="connName"> </param>
        /// <returns> </returns>
        public MappingInfo GetMappingInfo(string connName)
        {
            return _mappings[connName.ToLower()];
        }
    }

    /// <summary>
    ///   数据库连接配置信息类
    /// </summary>
    /// <remarks>
    ///   数据库命令在同一个数据库连接范围内唯一
    /// </remarks>
    public class MappingInfo
    {
        private readonly Dictionary<string, CommandInfo> _commands = new Dictionary<string, CommandInfo>();
        private readonly string _mappingFor = string.Empty;

        /// <summary>
        ///   根据XML格式的配置文件片段初始化数据库连接配置信息
        /// </summary>
        /// <param name="data"> </param>
        public MappingInfo(string data)
        {
            var doc = new XmlDocument();
            doc.LoadXml(data);

            if (doc.DocumentElement != null) _mappingFor = doc.DocumentElement.Attributes["for"].Value;

            XmlNodeList commands = doc.SelectNodes("//object.mapping/command");
            if (commands != null)
                foreach (XmlNode command in commands)
                {
                    string commandData = command.OuterXml;
                    var commandInfo = new CommandInfo(commandData);

                    _commands.Add(commandInfo.CommandAlians.ToLower(), commandInfo);
                }
            XmlNodeList simpleMappings = doc.SelectNodes("//object.mapping/simple.mapping");
            if (simpleMappings != null)
                foreach (XmlNode simpleMapping in simpleMappings)
                {
                    string simpleMappingData = simpleMapping.OuterXml;
                    var simpleMappingInfo = new SimpleMapping(simpleMappingData);

                    var insertCommandInfo = new CommandInfo(simpleMappingInfo, AutoType.AutoInsert);
                    if (!_commands.Any(c => c.Key.Equals(insertCommandInfo.CommandAlians, StringComparison.OrdinalIgnoreCase)))
                        _commands.Add(insertCommandInfo.CommandAlians.ToLower(), insertCommandInfo);

                    var selectCommandInfo = new CommandInfo(simpleMappingInfo, AutoType.AutoSelect);
                    if (!_commands.Any(c => c.Key.Equals(selectCommandInfo.CommandAlians, StringComparison.OrdinalIgnoreCase)))
                        _commands.Add(selectCommandInfo.CommandAlians.ToLower(), selectCommandInfo);

                    var updateCommandInfo = new CommandInfo(simpleMappingInfo, AutoType.AutoUpdate);
                    if (!_commands.Any(c => c.Key.Equals(updateCommandInfo.CommandAlians, StringComparison.OrdinalIgnoreCase)))
                        _commands.Add(updateCommandInfo.CommandAlians.ToLower(), updateCommandInfo);

                    var deleteCommandInfo = new CommandInfo(simpleMappingInfo, AutoType.AutoDelete);
                    if (!_commands.Any(c => c.Key.Equals(deleteCommandInfo.CommandAlians, StringComparison.OrdinalIgnoreCase)))
                        _commands.Add(deleteCommandInfo.CommandAlians.ToLower(), deleteCommandInfo);
                }

        }

        /// <summary>
        ///   配置节对应的数据库连接的别名
        /// </summary>
        public string For
        {
            get { return _mappingFor; }
        }

        /// <summary>
        ///   根据数据库命令别名返回数据库命令配置信息
        /// </summary>
        /// <param name="commandName"> </param>
        /// <returns> </returns>
        public CommandInfo GetCommandInfo(string commandName)
        {
            return _commands[commandName.ToLower()];
        }
    }

    /// <summary>
    /// 简单映射，旨在一个配置搞定一个实体类与数据库的表的关系
    /// </summary>
    public class SimpleMapping
    {
        /// <summary>
        ///   数据库命令的表名
        /// </summary>
        public string TableName { set; get; }
        public IEnumerable<string> ExcludeProperty { set; get; }
        public IEnumerable<string> IncludeProperty { set; get; }
        public string PrimaryKey { set; get; }
        public string EntityType { set; get; }
        public IDictionary<string, string> ParamMappings { set; get; }
 
        /// <summary>
        ///   根据XML格式的配置文件片段初始化数据库命令配置信息
        /// </summary>
        /// <param name="data"> </param>
        public SimpleMapping(string data)
        {
            var doc = new XmlDocument();
            doc.LoadXml(data);

            var selectSingleNode = doc.SelectSingleNode("//simple.mapping/text");
            TableName = selectSingleNode != null ? selectSingleNode.InnerText.Trim() : string.Empty;

            if (selectSingleNode != null && selectSingleNode.Attributes != null)
            {
                if (selectSingleNode.Attributes["from"] != null && selectSingleNode.Attributes["namespace"] != null)
                    EntityType = selectSingleNode.Attributes["namespace"].Value.Trim() + "." +
                                      selectSingleNode.Attributes["from"].Value.Trim();
                if (selectSingleNode.Attributes["excludeProperty"] != null)
                {
                    ExcludeProperty = selectSingleNode.Attributes["excludeProperty"].Value.Trim().
                        Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries).
                        Distinct();
                }
                if (selectSingleNode.Attributes["includeProperty"] != null)
                {
                    IncludeProperty = selectSingleNode.Attributes["includeProperty"].Value.Trim().
                        Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries).
                        Distinct();
                }
                if (selectSingleNode.Attributes["primaryKey"] != null)
                {
                    PrimaryKey = selectSingleNode.Attributes["primaryKey"].Value.Trim();
                }
            }

            XmlNodeList parameters = doc.SelectNodes("//simple.mapping/parameters/add");
            if (parameters != null)
            {
                ParamMappings = new Dictionary<string, string>();
                foreach (XmlNode param in parameters)
                {
                    string dbMember = param.Attributes["dbMember"].Value.ToLower();
                    string objMember = param.Attributes["objMember"].Value.ToLower();

                    ParamMappings.Add(dbMember, objMember);
                }
            }
        }
    }

    public enum AutoType
    {
        NotAuto,
        AutoInsert,
        AutoSelect,
        AutoUpdate,
        AutoDelete
    }

    /// <summary>
    ///   数据库命令配置信息类
    /// </summary>
    public class CommandInfo
    {
        private static readonly VelocityEngine VelocityEngine;

        static CommandInfo()
        {
            VelocityEngine = new VelocityEngine();
            VelocityEngine.Init();
        }

        public CommandInfo(SimpleMapping simpleMapping, AutoType autoType)
        {
            _autoType = autoType;
            _commandText = simpleMapping.TableName;
            _autoEntityType = simpleMapping.EntityType;
            _excludeProperty = simpleMapping.ExcludeProperty;
            _includeProperty = simpleMapping.IncludeProperty;
            _paramMappings = simpleMapping.ParamMappings ?? new Dictionary<string, string>();
            var tmpInt = simpleMapping.EntityType.LastIndexOf('.');
            var mappingFor = simpleMapping.EntityType.Substring(tmpInt + 1);
            if (autoType == AutoType.AutoSelect)
            {
                _commandAlians = "Select" + mappingFor;
                if (!string.IsNullOrEmpty(_autoEntityType) && !_returnObjectMappings.ContainsKey(_autoEntityType.ToLower()))
                {
                    //参数映射直接作为默认的返回映射
                    _returnObjectMappings.Add(_autoEntityType.ToLower(),
                                                  new ReturnObject(_autoEntityType,
                                                                   _paramMappings.Select(
                                                                       pair =>
                                                                       new KeyValuePair<string, string>(pair.Value, pair.Key))));
                }
            }
            else if (autoType == AutoType.AutoDelete)
            {
                _commandAlians = "Delete" + mappingFor;
            }
            else if (autoType == AutoType.AutoInsert || autoType == AutoType.AutoUpdate)
            {
                _commandAlians = (autoType == AutoType.AutoInsert ? "Insert" : "Update") + mappingFor;
                if (!string.IsNullOrWhiteSpace(simpleMapping.PrimaryKey))
                {
                    var primaryKeys = simpleMapping.PrimaryKey.Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries);
                    if (primaryKeys.Any())
                    {
                        if (_excludeProperty != null)
                        {
                        }
                        else
                        {
                            _excludeProperty = new string[] {};
                        }
                        foreach (var primaryKey in primaryKeys)
                        {
                            if (!_excludeProperty.Any(
                                e => e.Equals(primaryKey, StringComparison.OrdinalIgnoreCase)))
                            {
                                _excludeProperty = _excludeProperty.Concat(new[]
                                                                               {
                                                                                   primaryKey
                                                                               });
                            }
                        }
                    }
                }
            }
        }

        /// <summary>
        ///   根据XML格式的配置文件片段初始化数据库命令配置信息
        /// </summary>
        /// <param name="data"> </param>
        public CommandInfo(string data)
        {
            var doc = new XmlDocument();
            doc.LoadXml(data);

            if (doc.DocumentElement != null)
            {
                _commandAlians = doc.DocumentElement.Attributes["alians"].Value;
            }
            var selectSingleNode = doc.SelectSingleNode("//command/text");
            _commandText = selectSingleNode != null ? selectSingleNode.InnerText.Trim() : _commandAlians;

            if (selectSingleNode != null && selectSingleNode.Attributes != null)
            {
                if (selectSingleNode.Attributes["type"] != null)
                {
                    _autoType =
                        (AutoType) Enum.Parse(typeof (AutoType), selectSingleNode.Attributes["type"].Value, true);

                    if (selectSingleNode.Attributes["from"] != null && selectSingleNode.Attributes["namespace"] != null)
                        _autoEntityType = selectSingleNode.Attributes["namespace"].Value.Trim() + "." +
                                          selectSingleNode.Attributes["from"].Value.Trim();
                    if (selectSingleNode.Attributes["excludeProperty"] != null)
                    {
                        _excludeProperty = selectSingleNode.Attributes["excludeProperty"].Value.Trim().
                            Split(new[] {','}, StringSplitOptions.RemoveEmptyEntries).
                            Distinct();
                    }
                    if (selectSingleNode.Attributes["includeProperty"] != null)
                    {
                        _includeProperty = selectSingleNode.Attributes["includeProperty"].Value.Trim().
                            Split(new[] { ',' }, StringSplitOptions.RemoveEmptyEntries).
                            Distinct();
                    }
                }
                else
                {
                    if (selectSingleNode.Attributes["sqlfilename"] != null)
                    {
                        _sqlFileName = selectSingleNode.Attributes["sqlfilename"].Value;

                        var path = string.Empty;
                        if (HttpContext.Current != null)
                        {
                            path = HttpContext.Current.Server.MapPath("/bin");
                            path = Path.Combine(path, "App_Data");
                        }
                        if (string.IsNullOrEmpty(path))
                        {
                            path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "App_Data");
                            if (!File.Exists(Path.Combine(path,
                                                          @"SqlFiles\" + _sqlFileName)))
                            {
                                path = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, @"bin\App_Data");
                            }
                        }

                        string sqlFileName = Path.Combine(path,
                                                          @"SqlFiles\" + _sqlFileName);
                        _commandText = File.ReadAllText(sqlFileName, Encoding.UTF8);
                    }
                }
            }

            XmlNodeList parameters = doc.SelectNodes("//command/parameters/add");
            if (parameters != null)
                foreach (XmlNode param in parameters)
                {
                    string dbMember = param.Attributes["dbMember"].Value.ToLower();
                    string objMember = param.Attributes["objMember"].Value.ToLower();

                    ParamMappings.Add(dbMember, objMember);
                }

            XmlNodeList returnobjects = doc.SelectNodes("//command/returnobjects/returnobject");
            if (returnobjects != null && returnobjects.Count > 0)
            {
                foreach (XmlNode returnobject in returnobjects)
                {
                    string returnobjectData = returnobject.OuterXml;
                    var returnObjectInfo = new ReturnObject(returnobjectData);

                    _returnObjectMappings.Add(
                        string.Format("{0}.{1}", returnObjectInfo.Namespace, returnObjectInfo.For).ToLower(),
                        returnObjectInfo);
                }
            }
            if (!string.IsNullOrEmpty(_autoEntityType) && !_returnObjectMappings.ContainsKey(_autoEntityType.ToLower()))
            {
                //参数映射直接作为默认的返回映射
                _returnObjectMappings.Add(_autoEntityType.ToLower(),
                                          new ReturnObject(_autoEntityType,
                                                           _paramMappings.Select(
                                                               pair =>
                                                               new KeyValuePair<string, string>(pair.Value, pair.Key))));
            }
        }

        #region 属性

        private readonly string _autoEntityType = string.Empty;
        private readonly AutoType _autoType;
        private readonly string _commandAlians = string.Empty;
        private readonly string _commandText = string.Empty;
        private readonly IEnumerable<string> _excludeProperty;
        private readonly IEnumerable<string> _includeProperty;

        //private Template _commandTypeTemplate;
        private readonly IDictionary<string, string> _paramMappings = new Dictionary<string, string>();
        private readonly Dictionary<string, ReturnObject> _returnObjectMappings = new Dictionary<string, ReturnObject>();
        private readonly string _sqlFileName = string.Empty;

        /// <summary>
        ///   数据库命令的别名
        /// </summary>
        public string CommandAlians
        {
            get { return _commandAlians; }
        }

        /// <summary>
        ///   数据库命令的SQL语句、存储过程名或者表名
        /// </summary>
        public string CommandText
        {
            get { return _commandText; }
        }

        /// <summary>
        ///   数据库命令的表名
        /// </summary>
        public string TableName
        {
            get { return _autoType != AutoType.NotAuto ? _commandText : null; }
        }

        //public string SqlFileName
        //{
        //    get { return _sqlFileName; }
        //}

        //public AutoType AutoType
        //{
        //    get { return _autoType; }
        //}

        /// <summary>
        ///   数据库命令的参数映射关系（以数据库命令的参数名为Key,以实体的属性名为Value的键值对列表）
        /// </summary>
        public IDictionary<string, string> ParamMappings
        {
            get { return _paramMappings; }
        }

        //public string AutoInsertType
        //{
        //    get { return _autoInsertType; }
        //}

        #endregion

        /// <summary>
        ///   根据实体类型、数据库类型以及命令配置信息（参数映射关系、排除的实体属性）生成实体插入数据库的SQL语句
        /// </summary>
        /// <param name="type"> 实体类型 </param>
        /// <param name="ado"> 数据库访问底层封装类 </param>
        /// <param name="includeProperty"> 运行时传入的包含属性 </param>
        /// <param name="excludeProperty"> 运行时传入的排除属性 </param>
        /// <returns> 实体插入数据库的SQL语句 </returns>
        public string RealCommandText(Type type, AdoHelper ado, IEnumerable<string> includeProperty = null, IEnumerable<string> excludeProperty = null)
        {
            if (EqualsType(type) && _autoType == AutoType.AutoInsert)
            {
                var pairs = GetParamMapping(type, includeProperty, excludeProperty);

                //VelocityContext vltContext = new VelocityContext();
                //vltContext.Put("FieldPrefix", ado.FieldPrefix);
                //vltContext.Put("FieldSuffix", ado.FieldSuffix);
                //vltContext.Put("ParamPrefix", ado.ParamPrefix);
                //vltContext.Put("CommandText", CommandText);
                //vltContext.Put("Pairs", pairs);
                //StringWriter vltWriter = new StringWriter();
                //VelocityEngine.Evaluate(vltContext, vltWriter, null, SqlTemp.InsertSqlTemp);
                //return vltWriter.GetStringBuilder().ToString();
                var sb = new StringBuilder();
                sb.Append("INSERT INTO " + CommandText + " (");
                foreach (KeyValuePair<string, string> pair in pairs)
                {
                    sb.Append(ado.FieldPrefix + pair.Key + ado.FieldSuffix + ",");
                }
                sb.Append(") values (");
                foreach (KeyValuePair<string, string> pair in pairs)
                {
                    sb.Append(ado.ParamPrefix + pair.Value + ",");
                }
                sb.Append(");");
                return sb.ToString().Replace(",) values (", ") values (").Replace(",);", ");");
            }
            return string.Empty;
        }

        /// <summary>
        ///   判断实体类型和数据库命令配置中的类型是否匹配
        /// </summary>
        /// <param name="t"> </param>
        /// <returns> </returns>
        private bool EqualsType(Type t)
        {
            if (!string.IsNullOrEmpty(t.FullName))
            {
                if (t.FullName.Equals(_autoEntityType, StringComparison.OrdinalIgnoreCase))
                    return true;
                if (t.FullName.Contains('`') && _autoEntityType.Contains('`'))
                    return t.FullName.StartsWith(_autoEntityType);
            }
            return false;
        }

        /// <summary>
        ///   根据实体类型、数据库类型以及命令配置信息（参数映射关系、排除的实体属性）生成实体更新或者删除的SQL语句
        /// </summary>
        /// <param name="type"> 实体类型 </param>
        /// <param name="ado"> 数据库访问底层封装类 </param>
        /// <param name="whereConditions"> Where条件列表 </param>
        /// <param name="includeProperty"> 运行时传入的包含属性 </param>
        /// <param name="excludeProperty"> 运行时传入的排除属性 </param>
        /// <returns> 实体更新或者删除的SQL语句 </returns>
        public string RealCommandText(Type type, AdoHelper ado, IList<IWhereCondition> whereConditions, IEnumerable<string> includeProperty = null, IEnumerable<string> excludeProperty = null)
        {
            var boolFlag = EqualsType(type);
            if (boolFlag && _autoType == AutoType.AutoUpdate)
            {
                var pairs = GetParamMapping(type, includeProperty, excludeProperty);

                var sb = new StringBuilder();
                sb.Append("UPDATE ");
                sb.Append(CommandText + " SET ");
                foreach (KeyValuePair<string, string> pair in pairs)
                {
                    sb.Append(ado.FieldPrefix + pair.Key + ado.FieldSuffix + "=" + ado.ParamPrefix + pair.Value + ", ");
                }
                sb.Append(GetWhereStr(whereConditions));

                return sb.ToString().Replace(",  WHERE", " WHERE").Replace("WHERE  AND", "WHERE");
            }
            if (boolFlag && _autoType == AutoType.AutoDelete)
            {
                var sb = new StringBuilder();
                sb.Append("DELETE FROM ");
                sb.Append(CommandText + " ");
                sb.Append(GetWhereStr(whereConditions));

                return sb.ToString().Replace("WHERE  AND", "WHERE");
            }
            return string.Empty;
        }

        /// <summary>
        ///   根据实体类型、数据库类型以及命令配置信息（参数映射关系、排除的实体属性）生成实体查询的SQL语句
        /// </summary>
        /// <param name="type"> 实体类型 </param>
        /// <param name="ado"> 数据库访问底层封装类 </param>
        /// <param name="whereConditions"> Where条件列表 </param>
        /// <param name="orderByConditions"> OrderBy条件列表 </param>
        /// <param name="isDistinct"> 是否去重 </param>
        /// <param name="skipNum"> 跳过的记录数 </param>
        /// <param name="tackNum"> 返回的记录数 </param>
        /// <param name="includeProperty"> 运行时传入的包含属性 </param>
        /// <param name="excludeProperty"> 运行时传入的排除属性 </param>
        /// <returns> 实体查询的SQL语句 </returns>
        /// <remarks>
        ///   如果skipNum为-1而tackNum不为-1，仍然是通过通用分页的方式来生成语句的，也就是说会生成RowNumber BETWEEN 0 AND tackNum的语句而不会生成TOP tackNum的语句
        /// </remarks>
        public string RealCommandText(Type type, AdoHelper ado, IList<IWhereCondition> whereConditions,
                                      IList<OrderByCondition> orderByConditions, bool isDistinct, int skipNum,
                                      int tackNum, IEnumerable<string> includeProperty = null, IEnumerable<string> excludeProperty = null)
        {
            if (EqualsType(type) && _autoType == AutoType.AutoSelect)
            {
                var pairs = GetParamMapping(type, includeProperty, excludeProperty);

                var sb = new StringBuilder();
                sb.Append("SELECT " + (isDistinct ? "DISTINCT " : ""));

                foreach (KeyValuePair<string, string> pair in pairs)
                {
                    sb.Append(ado.FieldPrefix + pair.Key + ado.FieldSuffix + ", ");
                }

                if (!(tackNum == -1 || ado is MySql))
                    sb.Append(" ROW_NUMBER() OVER ( " + GetOrderByStr(orderByConditions) + " ) AS RowNumber ");

                sb.Append(" FROM " + CommandText + " ");

                sb.Append(GetWhereStr(whereConditions));

                if (tackNum == -1 || ado is MySql)
                    sb.Append(GetOrderByStr(orderByConditions));

                var ret = sb.ToString().Replace(",  FROM", " FROM").Replace("WHERE  AND", "WHERE");

                var limitStr = "";
                if (ado is MySql && tackNum != -1)
                {
                    if (skipNum != -1)
                        limitStr += (" LIMIT " + skipNum + " ");
                    limitStr += (String.IsNullOrEmpty(limitStr) ? (" LIMIT " + tackNum + " ") : (", " + tackNum + " "));
                }
                else
                {
                    if (tackNum != -1)
                    {
                        ret = "SELECT xx.* FROM ( " + ret;
                        limitStr += " ) xx WHERE RowNumber BETWEEN " + (skipNum == -1 ? 0 : skipNum) + " AND " +
                                    (tackNum + (skipNum == -1 ? 0 : skipNum));
                    }
                }
                ret += limitStr;
                return ret;
            }
            return string.Empty;
        }

        /// <summary>
        ///   找到实体类型的参数映射关系
        /// </summary>
        /// <param name="type"> 实体类型 </param>
        /// <param name="includeProperty"> 运行时传入的包含属性 </param>
        /// <param name="excludeProperty"> 运行时传入的排除属性 </param>
        /// <returns> 以数据库命令的参数名为Key,以实体的属性名为Value的键值对列表 </returns>
        public IEnumerable<KeyValuePair<string, string>> GetParamMapping(Type type, IEnumerable<string> includeProperty = null, IEnumerable<string> excludeProperty = null)
        {
            var pairs = new List<KeyValuePair<string, string>>();

            var list = DbUtil.GetProperties(type);
            if (list != null)
            {
                foreach (PropertyInfo t in list)
                {
                    if (_includeProperty == null && includeProperty == null)
                    {
                        var exs = new List<string>();
                        exs.AddRange(excludeProperty ?? new List<string>());
                        exs.AddRange(_excludeProperty ?? new List<string>());

                        if (exs.Contains(t.Name))
                        {
                            continue;
                        }
                    }
                    else
                    {
                        var ins = new List<string>();
                        ins.AddRange(includeProperty ?? new List<string>());
                        ins.AddRange(_includeProperty ?? new List<string>());

                        if (!ins.Contains(t.Name))
                        {
                            continue;
                        }
                    }
                    KeyValuePair<string, string> pair = new KeyValuePair<string, string>(t.Name, t.Name);
                    foreach (KeyValuePair<string, string> p in ParamMappings)
                    {
                        if (p.Value.Equals(t.Name, StringComparison.OrdinalIgnoreCase))
                        {
                            pair = p;
                            break;
                        }
                    }
                    pairs.Add(pair);
                }
            }

            return pairs;
        }

        /// <summary>
        ///   根据传入的Where条件生成SQL命令语句的Where子句（带参数映射）
        /// </summary>
        /// <param name="whereConditions"> Where条件列表 </param>
        /// <returns> SQL命令语句的Where子句 </returns>
        public string GetWhereStr(IEnumerable<IWhereCondition> whereConditions)
        {
            StringBuilder sb = new StringBuilder();

            if (whereConditions != null && whereConditions.Any())
            {
                sb.Append(" WHERE ");
                foreach (var c in whereConditions)
                {
                    if (c is ConditionBase)
                    {
                        var cb = c as ConditionBase;
                        KeyValuePair<string, string> pair = new KeyValuePair<string, string>(cb.Column as string,
                                                                                             cb.Column as string);
                        foreach (KeyValuePair<string, string> p in ParamMappings)
                        {
                            if (p.Value.Equals(cb.Column as string, StringComparison.OrdinalIgnoreCase))
                            {
                                pair = p;
                                break;
                            }
                        }
                        if (c is WhereConditionBase)
                        {
                            var wc = c as WhereConditionBase;
                            if (wc.ObjValue != null && !ProcessSqlStr(wc.ObjValue.ToString()))
                                throw new Exception("发现有SQL注入攻击代码" + pair.Key + ":" + wc.ObjValue);
                            string tmp = "=";
                            switch (wc.Type)
                            {
                                case WhereConditionType.Equal:
                                    tmp = "=";
                                    break;
                                case WhereConditionType.NotEqual:
                                    tmp = "<>";
                                    break;
                                case WhereConditionType.Bigger:
                                    tmp = ">";
                                    break;
                                case WhereConditionType.NotBigger:
                                    tmp = "<=";
                                    break;
                                case WhereConditionType.Smaller:
                                    tmp = "<";
                                    break;
                                case WhereConditionType.NotSmaller:
                                    tmp = ">=";
                                    break;
                                case WhereConditionType.Like:
                                    tmp = "LIKE";
                                    break;
                                case WhereConditionType.NotLike:
                                    tmp = "NOT LIKE";
                                    break;
                                case WhereConditionType.AntiLike:
                                    tmp = "LIKE";
                                    break;
                                case WhereConditionType.NotAntiLike:
                                    tmp = "NOT LIKE";
                                    break;
                            }
                            if (!(wc is WhereAntiLikeCondition))
                            {
                                sb.Append(" AND " + pair.Key + " " + tmp + " " +
                                          ((wc.ObjValue != null &&
                                            (wc.ObjValue is DateTime || wc.ObjValue is string || wc.ObjValue is Guid))
                                               ? "'"
                                               : "") +
                                          ((wc.ObjValue != null && (wc.ObjValue.GetType().IsEnum || wc.ObjValue is bool))
                                               ? Convert.ToInt32(wc.ObjValue)
                                               : wc.ObjValue) +
                                          ((wc.ObjValue != null &&
                                            (wc.ObjValue is DateTime || wc.ObjValue is string || wc.ObjValue is Guid))
                                               ? "'"
                                               : "") + " ");
                            }
                            else
                            {
                                sb.Append(" AND " + ("'" + wc.ObjValue + "'") + " " + tmp + " " + pair.Key + " ");
                            }
                        }
                        else if (c is WhereNullCondition)
                        {
                            var wc = c as WhereNullCondition;
                            sb.Append(" AND " + pair.Key + " " + (wc.NullOrNot ? "IS" : "IS NOT") + " " + "NULL ");
                        }
                        else if (c is WhereInConditionBase)
                        {
                            var wc = c as WhereInConditionBase;
                            //if (wc.ObjValueList != null)
                            //{
                            //    var sqlInjectValues =
                            //        wc.ObjValueList.Where(v => v != null && !ProcessSqlStr(v.ToString()));
                            //    if (sqlInjectValues.Any())
                            //        throw new Exception("发现有SQL注入攻击代码" + pair.Key + ":" + sqlInjectValues.First());
                            //}
                            sb.Append(" AND " + DACBase<object>.SplitSourceInDc(wc.ObjValueList,
                                                                                wc.InOrNot,
                                                                                pair.Key) +
                                      " ");
                        }
                    }
                }
            }

            return sb.ToString();
        }

        /// <summary>
        ///   根据传入的OrderBy条件生成SQL命令语句的OrderBy子句（带参数映射）
        /// </summary>
        /// <param name="orderByConditions"> OrderBy条件列表 </param>
        /// <returns> SQL命令语句的OrderBy子句 </returns>
        private string GetOrderByStr(IEnumerable<OrderByCondition> orderByConditions)
        {
            StringBuilder sb = new StringBuilder();
            if (orderByConditions != null && orderByConditions.Any())
            {
                sb.Append(" ORDER BY ");
                foreach (var o in orderByConditions)
                {
                    KeyValuePair<string, string> pair = new KeyValuePair<string, string>(o.Column as string,
                                                                                         o.Column as string);
                    foreach (KeyValuePair<string, string> p in ParamMappings)
                    {
                        if (p.Value.Equals(o.Column as string, StringComparison.OrdinalIgnoreCase))
                        {
                            pair = p;
                            break;
                        }
                    }
                    sb.Append(" " + pair.Key + " " + (o.AscOrNot ? "ASC," : "DESC,") + " ");
                }
            }
            return sb.ToString().TrimEnd(", ".ToArray());
        }

        /// <summary>
        ///   替换SQL命令语句中的占位符
        /// </summary>
        /// <param name="context"> 替换参数 </param>
        /// <returns> 替换后的SQL命令语句 </returns>
        public string RealCommandText(IDictionary<string, Object> context)
        {
            string cmdText = _commandText.Replace("\r\n", " ");
            if (FormatCommandText(ref cmdText, context))
                return cmdText;
            throw new Exception("映射无效:参数的长度或者CommandName(" + _commandAlians + ")错误"); //参数长度不匹配
        }

        /// <summary>
        ///   替换SQL命令语句中的占位符
        /// </summary>
        /// <param name="cmdText"> SQL命令语句 </param>
        /// <param name="context"> 替换参数 </param>
        /// <param name="checkSqlInject"> 是否检查SQL注入攻击 </param>
        /// <returns> 是否替换成功 </returns>
        /// <remarks>
        ///   checkSqlInject设为false时请在上层做好防SQL注入的工作
        /// </remarks>
        public bool FormatCommandText(ref string cmdText, IEnumerable<KeyValuePair<string, object>> context,
                                      bool checkSqlInject = true)
        {
            if (context != null)
            {
                foreach (var pair in context)
                {
                    if (pair.Value == null)
                        continue;
                    var list = DbUtil.GetProperties(pair.Value.GetType());
                    foreach (PropertyInfo t in list)
                    {
                        string param = "{" + pair.Key + "." + t.Name + "}";

                        if (cmdText.Contains(param))
                        {
                            object fieldValue = TypeUtility.GetMemberGetDelegate(t)(pair.Value);
                            if (fieldValue == null)
                                cmdText = cmdText.Replace(param, string.Empty);
                            else
                            {
                                if (!checkSqlInject || ProcessSqlStr(fieldValue.ToString()))
                                    cmdText = cmdText.Replace(param, fieldValue.ToString());
                                else
                                    throw new Exception("发现有SQL注入攻击代码" + param + ":" + fieldValue);
                            }
                        }
                    }
                }
            }
            if (cmdText.Contains("{") && cmdText.Contains("}")) //如果还有没有匹配的参数,则返回错误
                return false;
            return true;
        }

        /// <summary>
        ///   根据类型名返回数据库命令的返回实体配置信息
        /// </summary>
        /// <param name="typeName"> </param>
        /// <returns> </returns>
        public ReturnObject GetReturnObject(string typeName)
        {
            return _returnObjectMappings[typeName.ToLower()];
        }

        #region 防止sql注入式攻击

        /// <summary>
        ///   判断字符串中是否有SQL注入攻击代码
        /// </summary>
        /// <param name="inputString"> 传入用户提交数据 </param>
        /// <returns> </returns>
        public static bool ProcessSqlStr(string inputString)
        {
            const string sqlStr =
                @"and|or|exec|execute|insert|select|delete|update|alter|create|drop|count|\*|chr|char|asc|mid|substring|master|truncate|declare|xp_cmdshell|restore|backup|net +user|net +localgroup +administrators";
            try
            {
                if (!string.IsNullOrEmpty(inputString))
                {
                    const string strRegex = @"\b(" + sqlStr + @")\b";

                    Regex regex = new Regex(strRegex, RegexOptions.IgnoreCase);
                    //string s = Regex.Match(inputString).Value;
                    if (regex.IsMatch(inputString))
                        return false;
                }
            }
            catch
            {
                return false;
            }
            return true;
        }

        #endregion
    }

    /// <summary>
    ///   数据库命令的返回实体配置信息类
    /// </summary>
    public class ReturnObject
    {
        private readonly string _mappingFor = string.Empty;
        private readonly string _namespace = string.Empty;
        private readonly IDictionary<string, string> _paramMappings = new Dictionary<string, string>();

        public ReturnObject(string entityType, IEnumerable<KeyValuePair<string, string>> paramMappings)
        {
            var tmpInt = entityType.LastIndexOf('.');
            _namespace = entityType.Substring(0, tmpInt);
            _mappingFor = entityType.Substring(tmpInt + 1);
            _paramMappings = paramMappings.ToDictionary(pair => pair.Key, pair => pair.Value);
        }

        /// <summary>
        ///   根据XML格式的配置文件片段初始化数据库命令的返回实体配置信息
        /// </summary>
        /// <param name="data"> </param>
        public ReturnObject(string data)
        {
            var doc = new XmlDocument();
            doc.LoadXml(data);

            if (doc.DocumentElement != null)
            {
                _mappingFor = doc.DocumentElement.Attributes["for"].Value;
                _namespace = doc.DocumentElement.Attributes["namespace"].Value;
            }

            XmlNodeList parameters = doc.SelectNodes("//returnobject/add");
            if (parameters != null)
                foreach (XmlNode param in parameters)
                {
                    string dbMember = param.Attributes["dbMember"].Value.ToLower();
                    string objMember = param.Attributes["objMember"].Value.ToLower();

                    _paramMappings.Add(objMember, dbMember);
                }
        }

        /// <summary>
        ///   返回的实体类的类名
        /// </summary>
        public string For
        {
            get { return _mappingFor; }
        }

        /// <summary>
        ///   返回的实体类所属的命名空间
        /// </summary>
        public string Namespace
        {
            get { return _namespace; }
        }

        /// <summary>
        ///   数据库命令的返回实体属性映射关系（以实体的属性名为Key,以数据库命令的结果集列名为Value的键值对列表）
        /// </summary>
        public IDictionary<string, string> ParamMappings
        {
            get { return _paramMappings; }
        }
    }
}