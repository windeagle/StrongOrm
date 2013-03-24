using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace DianPing.BA.Framework.DAL.DACBase
{
    public class AgrCondition
    {
        public object SrcColumn { get; set; }

        public object AgrColumn { get; set; }

        public AgrConditionType Type { get; set; }
    }
    public enum AgrConditionType
    {
        Count,
        Sum,
        Max,
        Min,
        Avg
    }
    public delegate AgrCondition[] AgrDelegate<T, TK>(T t, TK p);
    public static class GroupBySqlExtension
    {
        public static AgrCondition SqlCount(this object column)
        {
            return new AgrCondition
            {
                SrcColumn = column,
                Type = AgrConditionType.Count
            };
        }
        public static AgrCondition SqlSum<T>(this object column, T value)
        {
            return new AgrCondition
            {
                SrcColumn = column,
                AgrColumn = value,
                Type = AgrConditionType.Sum
            };
        }
        public static AgrCondition SqlMax<T>(this object column, T value)
        {
            return new AgrCondition
            {
                SrcColumn = column,
                AgrColumn = value,
                Type = AgrConditionType.Max
            };
        }
        public static AgrCondition SqlMin<T>(this object column, T value)
        {
            return new AgrCondition
            {
                SrcColumn = column,
                AgrColumn = value,
                Type = AgrConditionType.Min
            };
        }
        public static AgrCondition SqlAvg<T>(this object column, T value)
        {
            return new AgrCondition
            {
                SrcColumn = column,
                AgrColumn = value,
                Type = AgrConditionType.Avg
            };
        }
    }

    public class GroupByDAC<T, TK> : LinqDAC<T>
        where T : class, new()
        where TK : class, new()
    {
        private LinqDAC<TK> _innerDAC;
        private IList<string> _groupByColumnList = new List<string>();
        private IList<AgrCondition> _agrColumnList = new List<AgrCondition>();

        public GroupByDAC()
        {
            _innerDAC = new LinqDAC<TK>();
        }

        //GROUPDAC中的HAVING和ORDER BY直接对应LinqDAC<T>中的WHERE 和 ORDER BY

        public GroupByDAC<T, TK> SetInnerDAC(LinqDAC<TK> innerDAC)
        {
            _innerDAC = innerDAC;
            return this;
        }

        public GroupByDAC<T, TK> SqlGroupBy(Expression<ColumnListDelegate<TK>> columnListDelegate)
        {
            var expression = columnListDelegate as LambdaExpression;
            var newArrayExpression = expression.Body as NewArrayExpression;
            if (newArrayExpression == null)
                throw new Exception("错误/不支持的GroupBy表达式");
            foreach (var ex in newArrayExpression.Expressions)
            {
                var column = ex as MemberExpression;
                if (column == null)
                {
                    var unaryExpression = ex as UnaryExpression;
                    if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
                }
                if (column == null)
                    throw new Exception("错误/不支持的GroupBy表达式");
                _groupByColumnList.Add(column.Member.Name);
            }

            return this;
        }

        public GroupByDAC<T, TK> SqlAgr(Expression<AgrDelegate<T, TK>> agrDelegate)
        {
            var expression = agrDelegate as LambdaExpression;
            var newArrayExpression = expression.Body as NewArrayExpression;
            if (newArrayExpression == null)
                throw new Exception("错误/不支持的聚合表达式");

            var agr = agrDelegate.Compile()(new T(), new TK());
            var i = 0;
            foreach (var ex in newArrayExpression.Expressions)
            {
                var expressionBody = ex as MethodCallExpression;
                if (expressionBody == null)
                    throw new Exception("错误/不支持的聚合表达式");
                var arguments = expressionBody.Arguments;
                if (arguments.Count == 0)
                    throw new Exception("错误/不支持的聚合表达式");

                var column = arguments[0] as MemberExpression;
                if (column == null)
                {
                    var unaryExpression = arguments[0] as UnaryExpression;
                    if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
                }
                if (column == null)
                    throw new Exception("错误/不支持的聚合表达式");
                agr[i].AgrColumn = column.Member.Name;

                if (agr[i].Type != AgrConditionType.Count)
                {
                    column = arguments[1] as MemberExpression;
                    if (column == null)
                    {
                        var unaryExpression = arguments[1] as UnaryExpression;
                        if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
                    }
                    if (column == null)
                        throw new Exception("错误/不支持的聚合表达式");
                    agr[i].SrcColumn = column.Member.Name;
                }

                _agrColumnList.Add(agr[i]);
                i++;
            }

            return this;
        }

        public override IEnumerable<KeyValuePair<string, string>> GetResultSetColumnMappings()
        {
            return GetResultSetColumnMappings(GetParamMapping(string.Empty)).Select(p=>new KeyValuePair<string, string>(p.Key, p.Key));
        }

        private IEnumerable<KeyValuePair<string, string>> GetResultSetColumnMappings(IEnumerable<KeyValuePair<string, string>> columnMappings)
        {
            var pairs = new Dictionary<string, string>();
            
            foreach (var t in columnMappings)
            {
                if (_includeColumnList == null)
                {
                    var exs = new List<string>();
                    exs.AddRange(_excludeColumnList ?? new List<string>());
                    if (exs.Contains(t.Key))
                    {
                        continue;
                    }
                }
                else
                {
                    if (!_includeColumnList.Contains(t.Key))
                    {
                        continue;
                    }
                }
                pairs.Add(t.Key, t.Value);
            }

            return pairs;
        }

        /// <summary>
        /// 获取结果集的列与innerTable的列的映射关系
        /// </summary>
        /// <param name="innerTableAlians"></param>
        /// <returns></returns>
        private IDictionary<string, string> GetParamMapping(string innerTableAlians)
        {
            var pairs = new Dictionary<string, string>();

            var list = new List<string>();
            list.AddRange(_groupByColumnList);
            list.AddRange(_agrColumnList.Select(a => a.AgrColumn).Cast<string>());
            foreach (var t in list)
            {
                string fieldName;
                var agrColumn =
                    _agrColumnList.FirstOrDefault(
                        a => t.Equals(a.AgrColumn.ToString(), StringComparison.OrdinalIgnoreCase));
                if (agrColumn != null)
                {
                    fieldName = innerTableAlians + "." +
                                _innerDAC.GetDBFieldNameByEntityPropertyName(agrColumn.SrcColumn.ToString());
                    switch (agrColumn.Type)
                    {
                        case AgrConditionType.Count:
                            fieldName = "Count(1)";
                            break;
                        case AgrConditionType.Sum:
                            fieldName = "Sum(" + fieldName + ")";
                            break;
                        case AgrConditionType.Avg:
                            fieldName = "Avg(" + fieldName + ")";
                            break;
                        case AgrConditionType.Max:
                            fieldName = "Max(" + fieldName + ")";
                            break;
                        case AgrConditionType.Min:
                            fieldName = "Min(" + fieldName + ")";
                            break;
                    }
                }
                else
                {
                    fieldName = innerTableAlians + "." + _innerDAC.GetDBFieldNameByEntityPropertyName(t);
                }

                pairs.Add(t, fieldName);
            }

            return pairs;
        }

        private string GetHavingStr(IDictionary<string, string> columnMappings)
        {
            StringBuilder sb = new StringBuilder();

            if (_whereConditions != null && _whereConditions.Any())
            {
                sb.Append(" HAVING ");
                foreach (var c in _whereConditions)
                {
                    if (c is ConditionBase)
                    {
                        if (c is WhereConditionBase)
                        {
                            var wc = c as WhereConditionBase;
                            if (wc.ObjValue != null && !CommandInfo.ProcessSqlStr(wc.ObjValue.ToString()))
                                throw new Exception("发现有SQL注入攻击代码" + wc.Column + ":" + wc.ObjValue);
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
                                sb.Append(" AND " + columnMappings[wc.Column.ToString()] + " " + tmp + " " +
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
                                sb.Append(" AND " + ("'" + wc.ObjValue + "'") + " " + tmp + " " + columnMappings[wc.Column.ToString()] + " ");
                            }
                        }
                        else if (c is WhereNullCondition)
                        {
                            var wc = c as WhereNullCondition;
                            sb.Append(" AND " + columnMappings[wc.Column.ToString()] + " " + (wc.NullOrNot ? "IS" : "IS NOT") + " " + "NULL ");
                        }
                        else if (c is WhereInConditionBase)
                        {
                            var wc = c as WhereInConditionBase;
                            //if (wc.ObjValueList != null)
                            //{
                            //    var sqlInjectValues =
                            //        wc.ObjValueList.Where(v => v != null && !CommandInfo.ProcessSqlStr(v.ToString()));
                            //    if (sqlInjectValues.Any())
                            //        throw new Exception("发现有SQL注入攻击代码" + wc.Column + ":" + sqlInjectValues.First());
                            //}
                            sb.Append(" AND " + DACBase<object>.SplitSourceInDc(wc.ObjValueList,
                                                                                wc.InOrNot,
                                                                                columnMappings[wc.Column.ToString()]) +
                                      " ");
                        }
                    }
                }
            }

            return sb.ToString();
        }

        private string GetGroupByStr(IDictionary<string, string> columnMappings)
        {
            StringBuilder sb = new StringBuilder();
            if (_groupByColumnList != null && _groupByColumnList.Any())
            {
                sb.Append(" GROUP BY ");
                foreach (var o in _groupByColumnList)
                {
                    sb.Append(" " + columnMappings[o] + ", ");
                }
            }
            return sb.ToString().TrimEnd(", ".ToArray());
        }

        private string GetOrderByStr(IDictionary<string, string> columnMappings)
        {
            StringBuilder sb = new StringBuilder();
            if (_orderByConditions != null && _orderByConditions.Any())
            {
                sb.Append(" ORDER BY ");
                foreach (var o in _orderByConditions)
                {
                    sb.Append(" " + columnMappings[o.Column.ToString()] + " " + (o.AscOrNot ? "ASC," : "DESC,") + " ");
                }
            }
            return sb.ToString().TrimEnd(", ".ToArray());
        }

        public override string GetSelectSqlStr()
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);

            string innerQueryStr;
            string tableAlians;
            if (_innerDAC.NeedChildQuery())
            {
                innerQueryStr = "(" + _innerDAC.GetSelectSqlStr() + ") ";
                tableAlians = "Tmp_Table_" + Math.Abs(innerQueryStr.GetHashCode());
                innerQueryStr += tableAlians;
            }
            else
            {
                innerQueryStr = _innerDAC.GetDBTableName() + " ";
                tableAlians = "Tmp_Table_" + Math.Abs((innerQueryStr + _innerDAC.GetWhereStr()).GetHashCode());
                innerQueryStr += tableAlians + " ";
                innerQueryStr += _innerDAC.GetWhereStr();
            }
            var paramMappings = GetParamMapping(tableAlians);
            var pairs = GetResultSetColumnMappings(paramMappings);

            var sb = new StringBuilder();
            sb.Append("SELECT " + (_isDistinct ? "DISTINCT " : ""));

            foreach (KeyValuePair<string, string> pair in pairs)
            {
                sb.Append(pair.Value + " AS " + pair.Key + ", ");
            }

            if (!(_tackNum == -1 || ado is MySql))
                sb.Append(" ROW_NUMBER() OVER ( " + GetOrderByStr(paramMappings) + " ) AS RowNumber ");

            sb.Append(" FROM " + innerQueryStr + " ");

            sb.Append(GetGroupByStr(paramMappings));

            sb.Append(GetHavingStr(paramMappings));

            if (_tackNum == -1 || ado is MySql)
                sb.Append(GetOrderByStr(paramMappings));

            var ret = sb.ToString().Replace(",  FROM", " FROM").Replace("WHERE  AND", "WHERE").Replace("HAVING  AND", "HAVING");

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
            _agrColumnList = new List<AgrCondition>();
            _groupByColumnList = new List<string>();
            _innerDAC = null;
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