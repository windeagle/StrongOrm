using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace DianPing.BA.Framework.DAL.DACBase
{
    public enum JoinType
    {
        Inner,
        Left,
        Right,
        Full
    }
    public class JoinOrderByCondition : OrderByCondition
    {
        public bool IsRight { get; set; }
    }
    public class JoinFieldMapping
    {
        public bool IsRight { get; set; }
        public object SrcColumn { get; set; }
        public object ObjColumn { get; set; }
    }
    public delegate IWhereCondition OnDelegate<TL, TR>(TL t, TR p);
    public delegate object JoinOrderByDelegate<TK>(TK p);
    public delegate JoinFieldMapping[] JoinFieldMappingDelegate<T, TK>(T t, TK p);
    public static class JoinSqlExtension
    {
        public static JoinFieldMapping SqlFieldMapping<T>(this object column, T value)
        {
            return new JoinFieldMapping
            {
                SrcColumn = value,
                ObjColumn = column
            };
        }
    }

    public class JoinDAC<T, TL, TR> : LinqDAC<T>
        where T : class, new()
        where TL : class, new()
        where TR : class, new()
    {
        private IList<JoinOrderByCondition> _joinOrderByConditions = new List<JoinOrderByCondition>();
        private IList<WhereConditionBase> _onConditions = new List<WhereConditionBase>();
        private List<JoinFieldMapping> _joinFieldMappings = new List<JoinFieldMapping>();

        //不需要INCLUDE EXCLUDE
        //WHERE、OrderBy也不需要
        //ORDERBY 放一起 变成 JoinOrderByCondition

        private JoinType _type;
        private LinqDAC<TL> _leftDAC;
        private LinqDAC<TR> _rightDAC;
        
        public JoinDAC()
        {
            _leftDAC = new LinqDAC<TL>();
            _rightDAC = new LinqDAC<TR>();
        }

        public JoinDAC<T, TL, TR> SetJoinType(JoinType joinType)
        {
            _type = joinType;
            return this;
        }
        public JoinDAC<T, TL, TR> SetLeftDAC(LinqDAC<TL> innerDAC)
        {
            _leftDAC = innerDAC;
            return this;
        }
        public JoinDAC<T, TL, TR> SetRightDAC(LinqDAC<TR> innerDAC)
        {
            _rightDAC = innerDAC;
            return this;
        }

        public JoinDAC<T, TL, TR> SqlMapping(Expression<JoinFieldMappingDelegate<T, TL>> mappingDelegate)
        {
            return SqlMapping(mappingDelegate, false);
        }

        private JoinDAC<T, TL, TR> SqlMapping<TK>(Expression<JoinFieldMappingDelegate<T, TK>> mappingDelegate, bool isRight)
        {
            var expression = mappingDelegate as LambdaExpression;
            var newArrayExpression = expression.Body as NewArrayExpression;
            if (newArrayExpression == null)
                throw new Exception("错误/不支持的列映射表达式");

            foreach (var ex in newArrayExpression.Expressions)
            {
                var mapping = new JoinFieldMapping();
                var expressionBody = ex as MethodCallExpression;
                if (expressionBody == null)
                    throw new Exception("错误/不支持的列映射表达式");
                var arguments = expressionBody.Arguments;
                if (arguments.Count != 2)
                    throw new Exception("错误/不支持的列映射表达式");

                var column = arguments[0] as MemberExpression;
                if (column == null)
                {
                    var unaryExpression = arguments[0] as UnaryExpression;
                    if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
                }
                if (column == null)
                    throw new Exception("错误/不支持的列映射表达式");
                mapping.ObjColumn = column.Member.Name;

                column = arguments[1] as MemberExpression;
                if (column == null)
                {
                    var unaryExpression = arguments[1] as UnaryExpression;
                    if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
                }
                if (column == null)
                    throw new Exception("错误/不支持的列映射表达式");
                mapping.SrcColumn = column.Member.Name;

                mapping.IsRight = isRight;
                _joinFieldMappings.Add(mapping);
            }

            return this;
        }

        public JoinDAC<T, TL, TR> SqlMappingRight(Expression<JoinFieldMappingDelegate<T, TR>> mappingDelegate)
        {
            return SqlMapping(mappingDelegate, true);
        }

        private static string GetOrderByColumnName<TK>(Expression<JoinOrderByDelegate<TK>> orderByDelegate)
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

        /// <summary>
        /// 添加左表排序条件
        /// </summary>
        /// <param name="orderByDelegate"></param>
        /// <returns></returns>
        /// <remarks>因为TL和TR可能是同一类型，所以不能靠JoinOrderByDelegate委托的泛型参数判断左右</remarks>
        public JoinDAC<T, TL, TR> SqlOrderBy(Expression<JoinOrderByDelegate<TL>> orderByDelegate)
        {
            var argumentName = GetOrderByColumnName(orderByDelegate);

            var orderByCondition = new JoinOrderByCondition
                                       {
                                           Column = argumentName,
                                           IsRight = false,
                                           AscOrNot = true
                                       };
            _joinOrderByConditions.Add(orderByCondition);
            return this;
        }

        /// <summary>
        /// 添加右表排序条件
        /// </summary>
        /// <param name="orderByDelegate"></param>
        /// <returns></returns>
        /// <remarks>因为TL和TR可能是同一类型，所以不能靠JoinOrderByDelegate委托的泛型参数判断左右</remarks>
        public JoinDAC<T, TL, TR> SqlOrderByRight(Expression<JoinOrderByDelegate<TR>> orderByDelegate)
        {
            var argumentName = GetOrderByColumnName(orderByDelegate);

            var orderByCondition = new JoinOrderByCondition
            {
                Column = argumentName,
                IsRight = true,
                AscOrNot = true
            };
            _joinOrderByConditions.Add(orderByCondition);
            return this;
        }

        /// <summary>
        /// 添加左表倒序排序条件
        /// </summary>
        /// <param name="orderByDelegate"></param>
        /// <returns></returns>
        /// <remarks>因为TL和TR可能是同一类型，所以不能靠JoinOrderByDelegate委托的泛型参数判断左右</remarks>
        public JoinDAC<T, TL, TR> SqlOrderByDesc(Expression<JoinOrderByDelegate<TL>> orderByDelegate)
        {
            var argumentName = GetOrderByColumnName(orderByDelegate);

            var orderByCondition = new JoinOrderByCondition
            {
                Column = argumentName,
                IsRight = false,
                AscOrNot = false
            };
            _joinOrderByConditions.Add(orderByCondition);
            return this;
        }

        /// <summary>
        /// 添加右表倒序排序条件
        /// </summary>
        /// <param name="orderByDelegate"></param>
        /// <returns></returns>
        /// <remarks>因为TL和TR可能是同一类型，所以不能靠JoinOrderByDelegate委托的泛型参数判断左右</remarks>
        public JoinDAC<T, TL, TR> SqlOrderByRightDesc(Expression<JoinOrderByDelegate<TR>> orderByDelegate)
        {
            var argumentName = GetOrderByColumnName(orderByDelegate);

            var orderByCondition = new JoinOrderByCondition
            {
                Column = argumentName,
                IsRight = true,
                AscOrNot = false
            };
            _joinOrderByConditions.Add(orderByCondition);
            return this;
        }

        public JoinDAC<T, TL, TR> SqlOn(Expression<OnDelegate<TL, TR>> onDelegate)
        {
            var expression = onDelegate as LambdaExpression;
            var expressionBody = expression.Body as MethodCallExpression;
            if (expressionBody == null)
                throw new Exception("错误/不支持的On表达式");

            var conditionBase = onDelegate.Compile()(new TL(), new TR()) as WhereConditionBase;
            if (conditionBase == null)
                throw new Exception("错误/不支持的On表达式");
            var arguments = expressionBody.Arguments;
            if (arguments.Count != 2)
                throw new Exception("错误/不支持的On表达式");

            var column = arguments[0] as MemberExpression;
            if (column == null)
            {
                var unaryExpression = arguments[0] as UnaryExpression;
                if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
            }
            if (column == null)
                throw new Exception("错误/不支持的On表达式");
            string argumentName = column.Member.Name;
            if (string.IsNullOrEmpty(argumentName))
                throw new Exception("错误/不支持的On表达式");
            conditionBase.Column = argumentName;

            column = arguments[1] as MemberExpression;
            if (column == null)
            {
                var unaryExpression = arguments[1] as UnaryExpression;
                if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
            }
            if (column == null)
                throw new Exception("错误/不支持的On表达式");
            argumentName = column.Member.Name;
            if (string.IsNullOrEmpty(argumentName))
                throw new Exception("错误/不支持的On表达式");
            conditionBase.ObjValue = argumentName;

            _onConditions.Add(conditionBase);
            return this;
        }

        /// <summary>
        ///   根据On条件生成SQL命令语句的On子句
        /// </summary>
        /// <returns> SQL命令语句的On子句 </returns>
        private string GetOnStr(string leftInnerTableAlians, string rightInnerTableAlians)
        {
            StringBuilder sb = new StringBuilder();

            if (_onConditions != null && _onConditions.Any())
            {
                sb.Append(" ON ");
                var leftInnerMappings = _leftDAC.GetResultSetColumnMappings().ToDictionary(p => p.Key.ToLower(), p => p.Value);
                var rightInnerMappings = _rightDAC.GetResultSetColumnMappings().ToDictionary(p => p.Key.ToLower(), p => p.Value);
                foreach (var wc in _onConditions)
                {
                    if (wc != null)
                    {
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
                            sb.Append(" AND " + leftInnerTableAlians + "." + leftInnerMappings[wc.Column.ToString().ToLower()] +
                                      " " + tmp +
                                      " " + rightInnerTableAlians + "." + rightInnerMappings[wc.ObjValue.ToString().ToLower()] +
                                      " ");
                        }
                        else
                        {
                            sb.Append(" AND " + rightInnerTableAlians + "." + rightInnerMappings[wc.Column.ToString().ToLower()] +
                                      " " + tmp +
                                      " " + leftInnerTableAlians + "." + leftInnerMappings[wc.ObjValue.ToString().ToLower()] +
                                      " ");
                        }
                    }
                }
            }

            return sb.ToString();
        }

        public override IEnumerable<KeyValuePair<string, string>> GetResultSetColumnMappings()
        {
            return GetResultSetColumnMappings(string.Empty, string.Empty).Select(p => new KeyValuePair<string, string>(p.Key, p.Key));
        }

        private IEnumerable<KeyValuePair<string, string>> GetResultSetColumnMappings(string leftInnerTableAlians, string rightInnerTableAlians)
        {
            var pairs = new Dictionary<string, string>();
            var leftInnerMappings = _leftDAC.GetResultSetColumnMappings();
            foreach (var columnMapping in leftInnerMappings)
            {
                var tmpMapping =
                    _joinFieldMappings.FirstOrDefault(
                        m => !m.IsRight && m.SrcColumn.ToString().Equals(columnMapping.Key, StringComparison.OrdinalIgnoreCase));
                string tmpColumnName;
                if (tmpMapping != null)
                {
                    tmpColumnName = tmpMapping.ObjColumn.ToString();
                    if (!pairs.ContainsKey(tmpColumnName))
                        pairs.Add(tmpColumnName, leftInnerTableAlians + "." + columnMapping.Value);
                    else
                        pairs[tmpColumnName] = leftInnerTableAlians + "." + columnMapping.Value;
                }
                else
                {
                    tmpColumnName = columnMapping.Key;
                    if (!pairs.ContainsKey(tmpColumnName))
                        pairs.Add(tmpColumnName, leftInnerTableAlians + "." + columnMapping.Value);
                }
            }
            
            var rightInnerMappings = _rightDAC.GetResultSetColumnMappings();
            foreach (var columnMapping in rightInnerMappings)
            {
                var tmpMapping =
                    _joinFieldMappings.FirstOrDefault(
                        m => m.IsRight && m.SrcColumn.ToString().Equals(columnMapping.Key, StringComparison.OrdinalIgnoreCase));
                string tmpColumnName;
                if (tmpMapping != null)
                {
                    tmpColumnName = tmpMapping.ObjColumn.ToString();
                    if (!pairs.ContainsKey(tmpColumnName))
                        pairs.Add(tmpColumnName, rightInnerTableAlians + "." + columnMapping.Value);
                    else
                        pairs[tmpColumnName] = rightInnerTableAlians + "." + columnMapping.Value;
                }
                else
                {
                    tmpColumnName = columnMapping.Key;
                    if (!pairs.ContainsKey(tmpColumnName))
                        pairs.Add(tmpColumnName, rightInnerTableAlians + "." + columnMapping.Value);
                }
            }
            return pairs;
        }

        private string GetOrderByStr(string leftInnerTableAlians, string rightInnerTableAlians)
        {
            var leftInnerMappings = _leftDAC.GetResultSetColumnMappings().ToDictionary(p => p.Key.ToLower(), p => p.Value);
            var rightInnerMappings = _rightDAC.GetResultSetColumnMappings().ToDictionary(p => p.Key.ToLower(), p => p.Value);
            StringBuilder sb = new StringBuilder();
            if (_joinOrderByConditions != null && _joinOrderByConditions.Any())
            {
                sb.Append(" ORDER BY ");
                foreach (var o in _joinOrderByConditions)
                {
                    var tableAlians = o.IsRight ? rightInnerTableAlians : leftInnerTableAlians;
                    var columnMappings = o.IsRight ? rightInnerMappings : leftInnerMappings;
                    sb.Append(" " + tableAlians + "." + columnMappings[o.Column.ToString().ToLower()] + " " + (o.AscOrNot ? "ASC," : "DESC,") + " ");
                }
            }
            return sb.ToString().TrimEnd(", ".ToArray());
        }

        public override string GetSelectSqlStr()
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);

            string leftInnerQueryStr;
            string leftTableAlians;
            if (_leftDAC.JoinNeedChildQuery())
            {
                leftInnerQueryStr = "(" + _leftDAC.GetSelectSqlStr() + ") ";
                leftTableAlians = "Tmp_LeftTable_" + Math.Abs(leftInnerQueryStr.GetHashCode());
                leftInnerQueryStr += leftTableAlians;
            }
            else
            {
                leftInnerQueryStr = _leftDAC.GetDBTableName() + " ";
                leftTableAlians = "Tmp_LeftTable_" + Math.Abs(leftInnerQueryStr.GetHashCode());
                leftInnerQueryStr += leftTableAlians;
            }

            string rightInnerQueryStr;
            string rightTableAlians;
            if (_rightDAC.JoinNeedChildQuery())
            {
                rightInnerQueryStr = "(" + _rightDAC.GetSelectSqlStr() + ") ";
                rightTableAlians = "Tmp_RightTable_" + Math.Abs(rightInnerQueryStr.GetHashCode());
                rightInnerQueryStr += rightTableAlians;
            }
            else
            {
                rightInnerQueryStr = _rightDAC.GetDBTableName() + " ";
                rightTableAlians = "Tmp_RightTable_" + Math.Abs(rightInnerQueryStr.GetHashCode());
                rightInnerQueryStr += rightTableAlians;
            }

            var pairs = GetResultSetColumnMappings(leftTableAlians, rightTableAlians);

            var sb = new StringBuilder();
            sb.Append("SELECT " + (_isDistinct ? "DISTINCT " : ""));

            foreach (KeyValuePair<string, string> pair in pairs)
            {
                sb.Append(pair.Value + " AS " + pair.Key + ", ");
            }

            if (!(_tackNum == -1 || ado is MySql))
                sb.Append(" ROW_NUMBER() OVER ( " + GetOrderByStr(leftTableAlians, rightTableAlians) + " ) AS RowNumber ");

            sb.Append(" FROM " + leftInnerQueryStr + " " + _type + " JOIN " + rightInnerQueryStr + " ");

            sb.Append(GetOnStr(leftTableAlians, rightTableAlians));

            if (_tackNum == -1 || ado is MySql)
                sb.Append(GetOrderByStr(leftTableAlians, rightTableAlians));

            var ret = sb.ToString().Replace(",  FROM", " FROM").Replace("WHERE  AND", "WHERE").Replace("ON  AND", "ON");

            var limitStr = "";
            if (ado is MySql && _tackNum != -1)
            {
                if (_skipNum != -1)
                    limitStr += (" LIMIT " + _skipNum + " ");
                limitStr += (String.IsNullOrEmpty(limitStr) ? (" LIMIT " + _tackNum + " ") : (", " + _tackNum + " "));
            }
            else
            {
                if (_tackNum != -1)
                {
                    ret = "SELECT xx.* FROM ( " + ret;
                    limitStr += " ) xx WHERE RowNumber BETWEEN " + (_skipNum == -1 ? 0 : _skipNum) + " AND " +
                                (_tackNum + (_skipNum == -1 ? 0 : _skipNum));
                }
            }
            ret += limitStr;
            return ret;
        }

        public override IList<T> ToList()
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            string connAlians = DataBaseInstance.ConnAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);
            var comtext = GetSelectSqlStr();
            List<IDataParameter> outP;
            string conStr = DbConnectionStore.TheInstance.GetConnection(connAlians);

            return ado.QuerySqlForList<T, string, T>(conStr, comtext, null, null, out outP);
        }

        public override void Clear()
        {
            base.Clear();
            _type = JoinType.Inner;
            _rightDAC = null;
            _leftDAC = null;
            _joinOrderByConditions = new List<JoinOrderByCondition>();
            _onConditions = new List<WhereConditionBase>();
            _joinFieldMappings = new List<JoinFieldMapping>();
        }

        public override string GetDBFieldNameByEntityPropertyName(string propertyName)
        {
            return propertyName;
        }

        public override string GetDBTableName()
        {
            return string.Empty;
        }

        public override bool NeedChildQuery()
        {
            return true;
        }

        public override bool JoinNeedChildQuery()
        {
            return true;
        }

        public override string GetWhereStr()
        {
            return string.Empty;
        }
    }
}