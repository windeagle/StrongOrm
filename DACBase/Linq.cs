using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace DianPing.BA.Framework.DAL.DACBase
{
    public enum WhereConditionType
    {
        Equal,
        NotEqual,
        Bigger,
        NotBigger,
        Smaller,
        NotSmaller,
        Like,
        NotLike,
        AntiLike,
        NotAntiLike
    }

    public interface IWhereCondition
    {
    }

    public abstract class ConditionBase
    {
        public object Column { get; set; }
    }

    public abstract class WhereConditionBase : ConditionBase, IWhereCondition
    {
        public WhereConditionType Type { get; set; }
        public object ObjValue { get; set; }
    }

    public abstract class WhereInConditionBase : ConditionBase, IWhereCondition
    {
        public bool InOrNot { get; set; }
        public IEnumerable<object> ObjValueList { get; set; }
    }

    public class WhereCondition<T> : WhereConditionBase
    {
        private T _value;

        public T Value
        {
            get { return _value; }
            set
            {
                ObjValue = value;
                _value = value;
            }
        }
    }

    public class WhereLikeCondition : WhereCondition<string>
    {
    }

    public class WhereAntiLikeCondition : WhereCondition<string>
    {
    }

    public class WhereInCondition<T> : WhereInConditionBase
    {
        private IEnumerable<T> _valueList;

        public IEnumerable<T> ValueList
        {
            get { return _valueList; }
            set
            {
                ObjValueList = value.Cast<object>();
                _valueList = value;
            }
        }

        //public bool AddQuotation { get; set; }
    }

    public class WhereNullCondition : ConditionBase, IWhereCondition
    {
        public bool NullOrNot { get; set; }
    }

    public class OrderByCondition
    {
        public object Column { get; set; }
        public bool AscOrNot { get; set; }
    }

    public static class SqlExtension
    {
        public static IWhereCondition SqlEqual<T>(this object column, T value)
        {
            return new WhereCondition<T>
                       {
                           Column = column,
                           Value = value,
                           Type = WhereConditionType.Equal
                       };
        }

        public static IWhereCondition SqlNotEqual<T>(this object column, T value)
        {
            return new WhereCondition<T>
                       {
                           Column = column,
                           Value = value,
                           Type = WhereConditionType.NotEqual
                       };
        }

        public static IWhereCondition SqlBigger<T>(this object column, T value)
        {
            return new WhereCondition<T>
                       {
                           Column = column,
                           Value = value,
                           Type = WhereConditionType.Bigger
                       };
        }

        public static IWhereCondition SqlNotBigger<T>(this object column, T value)
        {
            return new WhereCondition<T>
                       {
                           Column = column,
                           Value = value,
                           Type = WhereConditionType.NotBigger
                       };
        }

        public static IWhereCondition SqlSmaller<T>(this object column, T value)
        {
            return new WhereCondition<T>
                       {
                           Column = column,
                           Value = value,
                           Type = WhereConditionType.Smaller
                       };
        }

        public static IWhereCondition SqlNotSmaller<T>(this object column, T value)
        {
            return new WhereCondition<T>
                       {
                           Column = column,
                           Value = value,
                           Type = WhereConditionType.NotSmaller
                       };
        }

        public static IWhereCondition SqlLike(this object column, string value)
        {
            return new WhereLikeCondition
                       {
                           Column = column,
                           Value = value,
                           Type = WhereConditionType.Like
                       };
        }

        public static IWhereCondition SqlNotLike(this object column, string value)
        {
            return new WhereLikeCondition
                       {
                           Column = column,
                           Value = value,
                           Type = WhereConditionType.NotLike
                       };
        }

        public static IWhereCondition SqlAntiLike(this object column, string value)
        {
            return new WhereAntiLikeCondition
            {
                Column = column,
                Value = value,
                Type = WhereConditionType.AntiLike
            };
        }

        public static IWhereCondition SqlNotAntiLike(this object column, string value)
        {
            return new WhereAntiLikeCondition
            {
                Column = column,
                Value = value,
                Type = WhereConditionType.NotAntiLike
            };
        }

        public static IWhereCondition SqlIn<T>(this object column, IEnumerable<T> valueList)
        {
            return new WhereInCondition<T>
                       {
                           Column = column,
                           ValueList = valueList,
                           InOrNot = true
                       };
        }

        public static IWhereCondition SqlNotIn<T>(this object column, IEnumerable<T> valueList)
        {
            return new WhereInCondition<T>
                       {
                           Column = column,
                           ValueList = valueList,
                           InOrNot = false
                       };
        }

        public static IWhereCondition SqlNull(this object column)
        {
            return new WhereNullCondition
                       {
                           Column = column,
                           NullOrNot = true
                       };
        }

        public static IWhereCondition SqlNotNull(this object column)
        {
            return new WhereNullCondition
                       {
                           Column = column,
                           NullOrNot = false
                       };
        }

        [Obsolete("不建议使用，请使用OrderBySimpleDelegate")]
        public static OrderByCondition SqlOrderBy(this object column)
        {
            return new OrderByCondition
                       {
                           Column = column,
                           AscOrNot = true
                       };
        }

        [Obsolete("不建议使用，请使用OrderBySimpleDelegate")]
        public static OrderByCondition SqlOrderByDesc(this object column)
        {
            return new OrderByCondition
                       {
                           Column = column,
                           AscOrNot = false
                       };
        }

        //[Obsolete("不建议使用，请使用OrderBySimpleDelegate")]
        //public static OrderByCondition Asc(this object column)
        //{
        //    return new OrderByCondition
        //               {
        //                   Column = column,
        //                   AscOrNot = true
        //               };
        //}

        //[Obsolete("不建议使用，请使用OrderBySimpleDelegate")]
        //public static OrderByCondition Desc(this object column)
        //{
        //    return new OrderByCondition
        //               {
        //                   Column = column,
        //                   AscOrNot = false
        //               };
        //}
    }

    public delegate IWhereCondition WhereDelegate<TK>(TK p);

    [Obsolete("不建议使用，请使用OrderBySimpleDelegate")]
    public delegate OrderByCondition OrderByDelegate<TK>(TK p);

    public delegate object[] ColumnListDelegate<TK>(TK p);

    public delegate object OrderBySimpleDelegate<TK>(TK p);

    //实体
    public class TestE
    {
        public int ColumnA { get; set; }
        public int ColumnB { get; set; }
    }

    ////实体的帮助类，与实体有一样的属性列表，属性值都是属性名的字符串
    //public class MetaData<T>
    //{
    //    //public static T Instance = new T();

    //    public string ColumnA
    //    {
    //        get { return "ColumnA"; }
    //    }
    //    public string ColumnB
    //    {
    //        get { return "ColumnB"; }
    //    }
    //}

    public class LinqDAC<T> : DACBase<T>, IEnumerable<T> where T : class, new()
    {
        private string _cmdAlians;
        protected bool _isDistinct;
        protected IList<OrderByCondition> _orderByConditions = new List<OrderByCondition>();
        protected int _skipNum = -1;
        protected int _tackNum = -1;

        protected IList<IWhereCondition> _whereConditions = new List<IWhereCondition>();
        protected IList<string> _includeColumnList;
        protected IList<string> _excludeColumnList;
        
        public LinqDAC<T> SqlInclude(Expression<ColumnListDelegate<T>> columnListDelegate)
        {
            _includeColumnList = new List<string>();

            var expression = columnListDelegate as LambdaExpression;
            var newArrayExpression = expression.Body as NewArrayExpression;
            if (newArrayExpression == null)
                throw new Exception("错误/不支持的Include表达式");
            foreach (var ex in newArrayExpression.Expressions)
            {
                var column = ex as MemberExpression;
                if (column == null)
                {
                    var unaryExpression = ex as UnaryExpression;
                    if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
                }
                if (column == null)
                    throw new Exception("错误/不支持的Include表达式");
                _includeColumnList.Add(column.Member.Name);
            }

            return this;
        }

        public LinqDAC<T> SqlExclude(Expression<ColumnListDelegate<T>> columnListDelegate)
        {
            _excludeColumnList = new List<string>();

            var expression = columnListDelegate as LambdaExpression;
            var newArrayExpression = expression.Body as NewArrayExpression;
            if (newArrayExpression == null)
                throw new Exception("错误/不支持的Exclude表达式");
            foreach (var ex in newArrayExpression.Expressions)
            {
                var column = ex as MemberExpression;
                if (column == null)
                {
                    var unaryExpression = ex as UnaryExpression;
                    if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
                }
                if (column == null)
                    throw new Exception("错误/不支持的Exclude表达式");
                _excludeColumnList.Add(column.Member.Name);
            }

            return this;
        }

        #region IEnumerable<T> Members

        public IEnumerator<T> GetEnumerator()
        {
            //得到语句并且查询
            return ToList().GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }

        #endregion

        public LinqDAC<T> SqlDistinct()
        {
            _isDistinct = true;
            return this;
        }

        public LinqDAC<T> SqlSkip(int skipNum)
        {
            _skipNum = skipNum;
            return this;
        }

        public LinqDAC<T> SqlTack(int tackNum)
        {
            _tackNum = tackNum;
            return this;
        }

        public LinqDAC<T> SqlSelect(string cmdAlians)
        {
            _cmdAlians = cmdAlians;
            return this;
        }

        public LinqDAC<T> SqlWhere(Expression<WhereDelegate<T>> whereDelegate)
        {
            string argumentName = string.Empty;

            var expression = whereDelegate as LambdaExpression;
            var expressionBody = expression.Body as MethodCallExpression;
            if (expressionBody != null)
            {
                var arguments = expressionBody.Arguments;
                var argument0 = arguments[0] as MemberExpression;
                if (argument0 == null)
                {
                    var unaryExpression = arguments[0] as UnaryExpression;
                    if (unaryExpression != null) argument0 = unaryExpression.Operand as MemberExpression;
                }
                if (argument0 != null)
                {
                    argumentName = argument0.Member.Name;
                }
            }

            if (string.IsNullOrEmpty(argumentName))
                throw new Exception("错误/不支持的Where表达式");

            var whereCondition = whereDelegate.Compile()(new T());
            var conditionBase = whereCondition as ConditionBase;
            if (conditionBase == null)
                throw new Exception("错误/不支持的Where表达式");

            //var expStr = whereDelegate.Body.ToString();
            //var dotIndex = expStr.IndexOf('.');
            //var endIndex = expStr.IndexOf('.', dotIndex + 1);
            //var endIndex2 = expStr.IndexOf(')');
            //endIndex = endIndex < endIndex2 ? endIndex : endIndex2;
            //conditionBase.Column = expStr.Substring(dotIndex + 1, endIndex - dotIndex - 1);

            conditionBase.Column = argumentName;

            _whereConditions.Add(whereCondition);
            return this;
        }

        [Obsolete("不建议使用，请直接使用SqlOrderBy(Expression<OrderBySimpleDelegate<T>>)和SqlOrderByDesc(Expression<OrderBySimpleDelegate<T>>)")]
        public LinqDAC<T> SqlOrderBy(Expression<OrderByDelegate<T>> orderByDelegate)
        {
            string argumentName = string.Empty;

            var expression = orderByDelegate as LambdaExpression;
            var expressionBody = expression.Body as MethodCallExpression;
            if (expressionBody != null)
            {
                var arguments = expressionBody.Arguments;
                var argument0 = arguments[0] as MemberExpression;
                if (argument0 == null)
                {
                    var unaryExpression = arguments[0] as UnaryExpression;
                    if (unaryExpression != null) argument0 = unaryExpression.Operand as MemberExpression;
                }
                if (argument0 != null)
                {
                    argumentName = argument0.Member.Name;
                }
            }

            if (string.IsNullOrEmpty(argumentName))
                throw new Exception("错误/不支持的OrderBy表达式");

            var orderByCondition = orderByDelegate.Compile()(new T());
            if (orderByCondition == null)
                throw new Exception("错误/不支持的OrderBy表达式");

            //var expStr = orderByDelegate.Body.ToString();
            //var dotIndex = expStr.IndexOf('.');
            //var endIndex = expStr.IndexOf('.', dotIndex + 1);
            //var endIndex2 = expStr.IndexOf(')');
            //endIndex = endIndex < endIndex2 ? endIndex : endIndex2;
            //orderByCondition.Column = expStr.Substring(dotIndex + 1, endIndex - dotIndex - 1);

            orderByCondition.Column = argumentName;

            _orderByConditions.Add(orderByCondition);
            return this;
        }

        public LinqDAC<T> SqlOrderBy(Expression<OrderBySimpleDelegate<T>> orderByDelegate)
        {
            var argumentName = GetOrderByColumnName(orderByDelegate);

            var orderByCondition = new OrderByCondition
                                       {
                                           Column = argumentName,
                                           AscOrNot = true
                                       };
            _orderByConditions.Add(orderByCondition);
            return this;
        }

        public LinqDAC<T> SqlOrderByDesc(Expression<OrderBySimpleDelegate<T>> orderByDelegate)
        {
            var argumentName = GetOrderByColumnName(orderByDelegate);

            var orderByCondition = new OrderByCondition
            {
                Column = argumentName,
                AscOrNot = false
            };
            _orderByConditions.Add(orderByCondition);
            return this;
        }

        private static string GetOrderByColumnName(Expression<OrderBySimpleDelegate<T>> orderByDelegate)
        {
            string argumentName = string.Empty;

            var expression = orderByDelegate as LambdaExpression;
            var argument0 = expression.Body as MemberExpression;
            if (argument0 == null)
            {
                var unaryExpression = expression.Body as UnaryExpression;
                if (unaryExpression != null) argument0 = unaryExpression.Operand as MemberExpression;
            }
            if (argument0 != null)
            {
                argumentName = argument0.Member.Name;
            }

            if (string.IsNullOrEmpty(argumentName))
                throw new Exception("错误/不支持的OrderBy表达式");
            return argumentName;
        }

        public virtual IList<T> ToList()
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Select" + typeof (T).Name;
            var comtext = GetSelectSqlStr();
            List<IDataParameter> outP;
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);

            return ado.QuerySqlForList<T, string, T>(conStr, comtext, null, null, out outP,
                                                     connAlians, _cmdAlians);
        }

        public virtual string GetSelectSqlStr()
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);

            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Select" + typeof (T).Name;
            var com = AdoHelper.GetCommandInfo(connAlians, _cmdAlians);
            return com.RealCommandText(typeof (T), ado, _whereConditions, _orderByConditions, _isDistinct, _skipNum,
                                       _tackNum, _includeColumnList, _excludeColumnList);
        }

        public string GetDeleteSqlStr(string cmdAlians = null)
        {
            _cmdAlians = cmdAlians;
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Delete" + typeof (T).Name;
            var com = AdoHelper.GetCommandInfo(connAlians, _cmdAlians);
            return com.RealCommandText(typeof(T), ado, _whereConditions, _includeColumnList, _excludeColumnList);
        }

        public string GetUpdateSqlStr(string cmdAlians = null)
        {
            _cmdAlians = cmdAlians;
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Update" + typeof (T).Name;
            var com = AdoHelper.GetCommandInfo(connAlians, _cmdAlians);
            return com.RealCommandText(typeof(T), ado, _whereConditions, _includeColumnList, _excludeColumnList);
        }

        public string GetInsertSqlStr(string cmdAlians = null)
        {
            _cmdAlians = cmdAlians;
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Insert" + typeof (T).Name;
            var com = AdoHelper.GetCommandInfo(connAlians, _cmdAlians);
            var comtext = com.RealCommandText(typeof(T), ado, _includeColumnList, _excludeColumnList);
            comtext += "select @@identity";
            return comtext;
        }

        public int SqlDelete(string cmdAlians = null)
        {
            _cmdAlians = cmdAlians;
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Delete" + typeof (T).Name;
            var comtext = GetDeleteSqlStr(_cmdAlians);
            List<IDataParameter> outP;

            return ado.ExecuteNonQuery<string, IDbDataParameter[]>(conStr, comtext, null, CommandType.Text, out outP,
                                                                   connAlians, _cmdAlians);
        }

        public int SqlUpdate(T record, string cmdAlians = null)
        {
            _cmdAlians = cmdAlians;
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Update" + typeof (T).Name;
            var comtext = GetUpdateSqlStr(_cmdAlians);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.GetParams(record);

            var excludeParams = p.Where(param => !comtext.ToLower().Contains(param.ParameterName.ToLower())).ToList();
            var needP = p.Except(excludeParams);

            AdoHelper.AssignParameterValues(needP.ToArray(), record, connAlians, _cmdAlians);
            return ado.ExecuteNonQuery(conStr, comtext, needP.ToArray(), CommandType.Text, out outP,
                                       connAlians, _cmdAlians);
        }

        public int SqlInsert(T record, string cmdAlians = null)
        {
            _cmdAlians = cmdAlians;
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Insert" + typeof (T).Name;
            var comtext = GetInsertSqlStr(_cmdAlians);
            List<IDataParameter> outP;

            Param p = ado.CreateParam() as Param;
            p.GetParams(record);
            var excludeParams = p.Where(param => !comtext.ToLower().Contains(param.ParameterName.ToLower())).ToList();
            var needP = p.Except(excludeParams);

            AdoHelper.AssignParameterValues(needP.ToArray(), record, connAlians, _cmdAlians);
            var obj = ado.ExecuteScalar(conStr, comtext, needP.ToArray(), CommandType.Text, out outP,
                                        connAlians, _cmdAlians);
            int ret;
            return !int.TryParse(obj.ToString(), out ret) ? 0 : ret;
        }

        public virtual string GetDBFieldNameByEntityPropertyName(string propertyName)
        {
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Select" + typeof(T).Name;
            var com = AdoHelper.GetCommandInfo(DataBaseInstance.ConnAlians, _cmdAlians);
            foreach (KeyValuePair<string, string> p in com.ParamMappings.Where(p => p.Value.Equals(propertyName, StringComparison.OrdinalIgnoreCase)))
            {
                return p.Key;
            }
            return propertyName;
        }

        public virtual IEnumerable<KeyValuePair<string, string>> GetResultSetColumnMappings()
        {
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Select" + typeof(T).Name;
            var com = AdoHelper.GetCommandInfo(DataBaseInstance.ConnAlians, _cmdAlians);
            return com.GetParamMapping(typeof (T), _includeColumnList, _excludeColumnList).Select(p=>new KeyValuePair<string, string>(p.Value, p.Key));
        }

        public virtual string GetDBTableName()
        {
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Select" + typeof(T).Name;
            var com = AdoHelper.GetCommandInfo(DataBaseInstance.ConnAlians, _cmdAlians);
            return com.TableName;
        }

        /// <summary>
        /// 如果作为GroupBy查询的基础，是否需要生成子查询
        /// </summary>
        /// <returns></returns>
        public virtual bool NeedChildQuery()
        {
            return _isDistinct || _tackNum != -1;
        }

        public virtual bool JoinNeedChildQuery()
        {
            return _isDistinct || _tackNum != -1 || _whereConditions.Any();
        }

        public virtual string GetWhereStr()
        {
            if (string.IsNullOrEmpty(_cmdAlians))
                _cmdAlians = "Select" + typeof(T).Name;
            var com = AdoHelper.GetCommandInfo(DataBaseInstance.ConnAlians, _cmdAlians);
            return com.GetWhereStr(_whereConditions);
        }

        public virtual void Clear()
        {
            _whereConditions = new List<IWhereCondition>();
            _orderByConditions = new List<OrderByCondition>();
            _isDistinct = false;
            _skipNum = -1;
            _tackNum = -1;
            _includeColumnList = null;
            _excludeColumnList = null;
            _cmdAlians = null;
        }
    }
}