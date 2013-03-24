using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using StrongCutIn.Util;

namespace DianPing.BA.Framework.DAL.DACBase
{
    public abstract class SimpleJoinConditionBase : ConditionBase
    {
        public string ItemName { get; set; }
    }
    public class SimpleJoinWhereCondition : SimpleJoinConditionBase, IWhereCondition
    {
        public WhereConditionType Type { get; set; }
        public object ObjValue { get; set; }
    }
    public class OnCondition : SimpleJoinWhereCondition
    {
        public string ObjName { get; set; }
    }
    public class SimpleJoinWhereNullCondition : SimpleJoinConditionBase, IWhereCondition
    {
        public bool NullOrNot { get; set; }
    }
    public class SimpleJoinWhereInCondition : SimpleJoinConditionBase, IWhereCondition
    {
        public bool InOrNot { get; set; }
        public IEnumerable<object> ObjValueList { get; set; }
    }
    public class SimpleJoinOrderByCondition : OrderByCondition
    {
        public string ItemName { get; set; }
    }
    public class SimpleJoinFieldMapping
    {
        public string ItemName { get; set; }
        public object SrcColumn { get; set; }
        public object ObjColumn { get; set; }
    }
    public static class SimpleJoinSqlExtension
    {
    //    public static SimpleJoinConditionBase SqlEqual<T>(this object column, T value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.Equal
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlNotEqual<T>(this object column, T value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.NotEqual
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlBigger<T>(this object column, T value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.Bigger
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlNotBigger<T>(this object column, T value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.NotBigger
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlSmaller<T>(this object column, T value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.Smaller
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlNotSmaller<T>(this object column, T value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.NotSmaller
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlLike(this object column, string value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.Like
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlNotLike(this object column, string value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.NotLike
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlAntiLike(this object column, string value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.AntiLike
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlNotAntiLike(this object column, string value)
    //    {
    //        return new SimpleJoinWhereCondition
    //        {
    //            Column = column,
    //            ObjValue = value,
    //            Type = WhereConditionType.NotAntiLike
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlIn<T>(this object column, IEnumerable<T> valueList)
    //    {
    //        return new SimpleJoinWhereInCondition
    //        {
    //            Column = column,
    //            ValueList = valueList.Cast<object>().ToArray(),
    //            InOrNot = true
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlNotIn<T>(this object column, IEnumerable<T> valueList)
    //    {
    //        return new SimpleJoinWhereInCondition
    //        {
    //            Column = column,
    //            ValueList = valueList.Cast<object>().ToArray(),
    //            InOrNot = false
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlNull(this object column)
    //    {
    //        return new SimpleJoinWhereNullCondition
    //        {
    //            Column = column,
    //            NullOrNot = true
    //        };
    //    }

    //    public static SimpleJoinConditionBase SqlNotNull(this object column)
    //    {
    //        return new SimpleJoinWhereNullCondition
    //        {
    //            Column = column,
    //            NullOrNot = false
    //        };
    //    }

        public static OrderByCondition Asc(this object column)
        {
            return new SimpleJoinOrderByCondition
            {
                Column = column,
                AscOrNot = true
            };
        }
        public static OrderByCondition Desc(this object column)
        {
            return new SimpleJoinOrderByCondition
            {
                Column = column,
                AscOrNot = false
            };
        }
        public static SimpleJoinFieldMapping SqlSimpleJoinFieldMapping<T>(this object column, T value)
        {
            return new SimpleJoinFieldMapping
            {
                SrcColumn = value,
                ObjColumn = column
            };
        }
    }

    public delegate OrderByCondition[] SimpleJoinOrderByDelegate<TL>(TL p);
    public delegate IWhereCondition[] SimpleJoinWhereDelegate<TL>(TL p);
    public delegate SimpleJoinFieldMapping[] SimpleJoinFieldMappingDelegate<T, TL>(T t, TL p);

    public class SimpleJoinDAC<T, TL> : LinqDAC<T>
        where T : class, new()
        where TL : class
    {

        public List<OnCondition> OnConditions { get; private set; }
        //public TL ChildQuery { get; private set; }
        public List<SimpleJoinConditionBase> WhereConditions { get; private set; }
        public List<SimpleJoinOrderByCondition> OrderByConditions { get; private set; }
        public List<SimpleJoinFieldMapping> JoinFieldMappings { get; private set; }
        private readonly string _tupleType;
        private readonly IDictionary<string, object> DACItems;

        private bool IsTuple(Type t)
        {
            if (!string.IsNullOrEmpty(t.FullName))
            {
                if (t.FullName.Contains('`'))
                {
                    return t.FullName.StartsWith(_tupleType);
                }
            }
            return false;
        }
        private bool IsDACType(Type t)
        {
            if (t.IsClass) return true;

            if (!t.IsGenericType) return false;
            var tmpType = t.GetGenericTypeDefinition();
            var ret = tmpType.IsSubclassOf(typeof (LinqDAC<object>));
            return ret;
        }

        public SimpleJoinDAC()
        {
            _tupleType = typeof(Tuple<int>).FullName;
            if (_tupleType != null)
                _tupleType = _tupleType.Substring(0, _tupleType.IndexOf('`'));

            var type = typeof(TL);
            if(!IsTuple(type))
                throw new Exception("TL 类型必须是元组！");

            var plist = DbUtil.GetProperties(type).ToList();
            plist = plist.Where(p => p.Name.StartsWith("Item")).ToList();
            if(plist.Any(p=>!IsDACType(p.PropertyType)))
                throw new Exception("TL 元组类的每个项的类型必须是DAC类型！");

            DACItems = new Dictionary<string, object>();
            var dacBaseType = typeof (LinqDAC<object>).GetGenericTypeDefinition();
            foreach (var propertyInfo in plist)
            {
                DACItems.Add(propertyInfo.Name, Activator.CreateInstance(dacBaseType.MakeGenericType(propertyInfo.PropertyType)));
            }
        }
        
        public SimpleJoinDAC<T, TL> SetChildQuery<TK>(int itemIndex, LinqDAC<TK> child) where TK : class, new()
        {
            var oldValue = DACItems["Item" + itemIndex];
            if (!(oldValue is LinqDAC<TK>))
                throw new Exception("设置子查询出错！");
            DACItems["Item" + itemIndex] = child;
            return this;
        }

        public SimpleJoinDAC<T, TL> SqlMapping(Expression<SimpleJoinFieldMappingDelegate<T, TL>> mappingDelegate)
        {
            JoinFieldMappings = new List<SimpleJoinFieldMapping>();

            var expression = mappingDelegate as LambdaExpression;
            var newArrayExpression = expression.Body as NewArrayExpression;
            if (newArrayExpression == null)
                throw new Exception("错误/不支持的列映射表达式");

            foreach (var ex in newArrayExpression.Expressions)
            {
                var mapping = new SimpleJoinFieldMapping();
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

                var item = column.Expression as MemberExpression;
                if (item == null)
                {
                    var unaryExpression = column.Expression as UnaryExpression;
                    if (unaryExpression != null) item = unaryExpression.Operand as MemberExpression;
                }
                if (item == null)
                    throw new Exception("错误/不支持的列映射表达式");
                mapping.ItemName = item.Member.Name;

                JoinFieldMappings.Add(mapping);
            }

            return this;
        }

        private static SimpleJoinConditionBase ConvertToSimpleJoinCondition(IWhereCondition conditionBase)
        {
            if(conditionBase is WhereConditionBase)
            {
                var whereConditionBase = conditionBase as WhereConditionBase;
                return new SimpleJoinWhereCondition
                           {
                               Column = whereConditionBase.Column,
                               ObjValue = whereConditionBase.ObjValue,
                               Type = whereConditionBase.Type
                           };
            }
            if (conditionBase is WhereInConditionBase)
            {
                var whereInConditionBase = conditionBase as WhereInConditionBase;
                return new SimpleJoinWhereInCondition
                {
                    Column = whereInConditionBase.Column,
                    ObjValueList = whereInConditionBase.ObjValueList,
                    InOrNot = whereInConditionBase.InOrNot
                };
            }
            if (conditionBase is WhereNullCondition)
            {
                var whereNullCondition = conditionBase as WhereNullCondition;
                return new SimpleJoinWhereNullCondition
                {
                    Column = whereNullCondition.Column,
                    NullOrNot = whereNullCondition.NullOrNot
                };
            }
            return null;
        }

        private static OnCondition ConvertToOnCondition(IWhereCondition conditionBase)
        {
            if (conditionBase is WhereConditionBase)
            {
                var whereConditionBase = conditionBase as WhereConditionBase;
                return new OnCondition
                {
                    Column = whereConditionBase.Column,
                    ObjValue = whereConditionBase.ObjValue,
                    Type = whereConditionBase.Type
                };
            }
            return null;
        }

        private static object[] GetTupleCtorParams(Type t)
        {
            var plist = DbUtil.GetProperties(t).ToList();
            plist = plist.Where(p => p.Name.StartsWith("Item")).ToList();
            return plist.Select(p => Activator.CreateInstance(p.PropertyType)).ToArray();
        }

        public SimpleJoinDAC<T, TL> SqlWhere(Expression<SimpleJoinWhereDelegate<TL>> whereDelegate)
        {
            var t = typeof (TL);
            var conditionBases = whereDelegate.Compile()(Activator.CreateInstance(t, GetTupleCtorParams(t)) as TL);
            WhereConditions = conditionBases.Select(ConvertToSimpleJoinCondition).ToList();

            var expression = whereDelegate as LambdaExpression;
            var newArrayExpression = expression.Body as NewArrayExpression;
            if (newArrayExpression == null)
                throw new Exception("错误/不支持的Where表达式");

            var i = 0;
            foreach (var ex in newArrayExpression.Expressions)
            {
                var expressionBody = ex as MethodCallExpression;
                if (expressionBody == null)
                    throw new Exception("错误/不支持的Where表达式");
                var arguments = expressionBody.Arguments;
                if (arguments.Count != 2)
                    throw new Exception("错误/不支持的Where表达式");

                var column = arguments[0] as MemberExpression;
                if (column == null)
                {
                    var unaryExpression = arguments[0] as UnaryExpression;
                    if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
                }
                if (column == null)
                    throw new Exception("错误/不支持的Where表达式");
                WhereConditions[i].Column = column.Member.Name;

                var item = column.Expression as MemberExpression;
                if (item == null)
                {
                    var unaryExpression = column.Expression as UnaryExpression;
                    if (unaryExpression != null) item = unaryExpression.Operand as MemberExpression;
                }
                if (item == null)
                    throw new Exception("错误/不支持的Where表达式");
                WhereConditions[i].ItemName = item.Member.Name;

                i++;
            }

            return this;
        }

        public SimpleJoinDAC<T, TL> SqlOn(Expression<SimpleJoinWhereDelegate<TL>> whereDelegate)
        {
            OnConditions = new List<OnCondition>();

            var expression = whereDelegate as LambdaExpression;
            var newArrayExpression = expression.Body as NewArrayExpression;
            if (newArrayExpression == null)
                throw new Exception("错误/不支持的On表达式");

            foreach (var ex in newArrayExpression.Expressions)
            {
                var expressionBody = ex as MethodCallExpression;
                if (expressionBody == null)
                    throw new Exception("错误/不支持的On表达式");
                var conditionBase = new DynamicMethodExecutor(expressionBody.Method).Execute(1, new object[]
                                                                                                 {
                                                                                                     1,
                                                                                                     1
                                                                                                 }) as IWhereCondition;
                var onCondition = ConvertToOnCondition(conditionBase);
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
                onCondition.Column = column.Member.Name;

                var item = column.Expression as MemberExpression;
                if (item == null)
                {
                    var unaryExpression = column.Expression as UnaryExpression;
                    if (unaryExpression != null) item = unaryExpression.Operand as MemberExpression;
                }
                if (item == null)
                    throw new Exception("错误/不支持的On表达式");
                onCondition.ItemName = item.Member.Name;

                column = arguments[1] as MemberExpression;
                if (column == null)
                {
                    var unaryExpression = arguments[1] as UnaryExpression;
                    if (unaryExpression != null) column = unaryExpression.Operand as MemberExpression;
                }
                if (column == null)
                    throw new Exception("错误/不支持的On表达式");
                onCondition.ObjValue = column.Member.Name;

                item = column.Expression as MemberExpression;
                if (item == null)
                {
                    var unaryExpression = column.Expression as UnaryExpression;
                    if (unaryExpression != null) item = unaryExpression.Operand as MemberExpression;
                }
                if (item == null)
                    throw new Exception("错误/不支持的On表达式");
                onCondition.ObjName = item.Member.Name;

                OnConditions.Add(onCondition);
            }

            return this;
        }

        public SimpleJoinDAC<T, TL> SqlOrderBy(Expression<SimpleJoinOrderByDelegate<TL>> orderByDelegate)
        {
            OrderByConditions = new List<SimpleJoinOrderByCondition>();

            var expression = orderByDelegate as LambdaExpression;
            var newArrayExpression = expression.Body as NewArrayExpression;
            if (newArrayExpression == null)
                throw new Exception("错误/不支持的OrderBy表达式");

            foreach (var ex in newArrayExpression.Expressions)
            {
                var orderby = new SimpleJoinOrderByCondition();
                var expressionBody = ex as MethodCallExpression;
                if (expressionBody == null)
                    throw new Exception("错误/不支持的OrderBy表达式");
                orderby.AscOrNot = expressionBody.Method.Name.Equals("Asc", StringComparison.OrdinalIgnoreCase);

                var arguments = expressionBody.Arguments;
                if (arguments.Count != 1)
                    throw new Exception("错误/不支持的OrderBy表达式");

                var argument0 = arguments[0] as MemberExpression;
                if (argument0 == null)
                {
                    var unaryExpression = arguments[0] as UnaryExpression;
                    if (unaryExpression != null) argument0 = unaryExpression.Operand as MemberExpression;
                }
                if (argument0 == null)
                    throw new Exception("错误/不支持的OrderBy表达式");
                orderby.Column = argument0.Member.Name;

                var item = argument0.Expression as MemberExpression;
                if (item == null)
                {
                    var unaryExpression = argument0.Expression as UnaryExpression;
                    if (unaryExpression != null) item = unaryExpression.Operand as MemberExpression;
                }
                if (item == null)
                    throw new Exception("错误/不支持的OrderBy表达式");
                orderby.ItemName = item.Member.Name;

                OrderByConditions.Add(orderby);
            }

            return this;
        }

        private string GetWhereStr(IDictionary<string, string> tableNames)
        {
            StringBuilder sb = new StringBuilder();

            if (WhereConditions != null && WhereConditions.Any())
            {
                sb.Append(" WHERE ");
                foreach (var c in WhereConditions)
                {
                    if (c != null)
                    {
                        var leftInnerTableAlians = tableNames[c.ItemName];

                        var leftMethodInfo = DACItems[c.ItemName].GetType().GetMethod("GetResultSetColumnMappings");
                        var leftResultSetColumnMappings = new DynamicMethodExecutor(leftMethodInfo).Execute(DACItems[c.ItemName], new object[] { }) as IEnumerable<KeyValuePair<string, string>>;
                        var columnMappings = leftResultSetColumnMappings.ToDictionary(i => i.Key.ToLower(), i => i.Value);

                        if (c is SimpleJoinWhereCondition)
                        {
                            var wc = c as SimpleJoinWhereCondition;
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
                        if (!(wc.Type == WhereConditionType.AntiLike || wc.Type == WhereConditionType.NotAntiLike))
                        {
                            sb.Append(" AND " + leftInnerTableAlians + "." + columnMappings[wc.Column.ToString().ToLower()] + " " + tmp + " " +
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
                            sb.Append(" AND " + ("'" + wc.ObjValue + "'") + " " + tmp + " " + leftInnerTableAlians + "." + columnMappings[wc.Column.ToString().ToLower()] + " ");
                        }
                        }
                        else if (c is SimpleJoinWhereNullCondition)
                        {
                            var wc = c as SimpleJoinWhereNullCondition;
                            sb.Append(" AND " + leftInnerTableAlians + "." + columnMappings[wc.Column.ToString().ToLower()] + " " + (wc.NullOrNot ? "IS" : "IS NOT") + " " + "NULL ");
                        }
                        else if (c is SimpleJoinWhereInCondition)
                        {
                            var wc = c as SimpleJoinWhereInCondition;
                            //if (wc.ObjValueList != null)
                            //{
                            //    var sqlInjectValues =
                            //        wc.ObjValueList.Where(v => v != null && !CommandInfo.ProcessSqlStr(v.ToString()));
                            //    if (sqlInjectValues.Any())
                            //        throw new Exception("发现有SQL注入攻击代码" + wc.Column + ":" + sqlInjectValues.First());
                            //}
                            sb.Append(" AND " + DACBase<object>.SplitSourceInDc(wc.ObjValueList,
                                                                                wc.InOrNot,
                                                                                leftInnerTableAlians + "." + columnMappings[wc.Column.ToString().ToLower()]) +
                                      " ");
                        }
                    }
                }
            }

            return sb.ToString();
        }

        /// <summary>
        ///   根据On条件生成SQL命令语句的On子句
        /// </summary>
        /// <returns> SQL命令语句的On子句 </returns>
        private string GetOnStr(IDictionary<string, string> tableNames)
        {
            StringBuilder sb = new StringBuilder();

            if (OnConditions != null && OnConditions.Any())
            {
                foreach (var wc in OnConditions)
                {
                    if (wc != null)
                    {
                        var leftInnerTableAlians = tableNames[wc.ItemName];
                        var rightInnerTableAlians = tableNames[wc.ObjName];

                        var leftMethodInfo = DACItems[wc.ItemName].GetType().GetMethod("GetResultSetColumnMappings");
                        var leftResultSetColumnMappings = new DynamicMethodExecutor(leftMethodInfo).Execute(DACItems[wc.ItemName], new object[] { }) as IEnumerable<KeyValuePair<string, string>>;
                        var leftInnerMappings = leftResultSetColumnMappings.ToDictionary(i => i.Key.ToLower(), i => i.Value);

                        var rightMethodInfo = DACItems[wc.ObjName].GetType().GetMethod("GetResultSetColumnMappings");
                        var rightResultSetColumnMappings = new DynamicMethodExecutor(rightMethodInfo).Execute(DACItems[wc.ObjName], new object[] { }) as IEnumerable<KeyValuePair<string, string>>;
                        var rightInnerMappings = rightResultSetColumnMappings.ToDictionary(i => i.Key.ToLower(), i => i.Value);

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
                        if (!(wc.Type == WhereConditionType.AntiLike || wc.Type == WhereConditionType.NotAntiLike))
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
            return GetResultSetColumnMappings(DACItems.ToDictionary(i=>i.Key, i=>i.Key)).Select(p => new KeyValuePair<string, string>(p.Key, p.Key));
        }

        private IEnumerable<KeyValuePair<string, string>> GetResultSetColumnMappings(IDictionary<string, string> tableNames)
        {
            var pairs = new Dictionary<string, string>();
            foreach (var pair in DACItems)
            {
                //var property = DbUtil.GetProperties(typeof(TL)).First(p=>p.Name.Equals(pair.Key));
                //var methodInfo = property.PropertyType.GetMethod("GetResultSetColumnMappings");
                var methodInfo = pair.Value.GetType().GetMethod("GetResultSetColumnMappings");
                var resultSetColumnMappings = new DynamicMethodExecutor(methodInfo).Execute(pair.Value, new object[] { }) as IEnumerable<KeyValuePair<string, string>>;
                foreach (var columnMapping in resultSetColumnMappings)
                {
                    var tmpMapping =
                        JoinFieldMappings.FirstOrDefault(
                            m => m.ItemName.Equals(pair.Key) && m.SrcColumn.ToString().Equals(columnMapping.Key, StringComparison.OrdinalIgnoreCase));
                    string tmpColumnName;
                    if (tmpMapping != null)
                    {
                        tmpColumnName = tmpMapping.ObjColumn.ToString();
                        if (!pairs.ContainsKey(tmpColumnName))
                            pairs.Add(tmpColumnName, tableNames[pair.Key] + "." + columnMapping.Value);
                        else
                            pairs[tmpColumnName] = tableNames[pair.Key] + "." + columnMapping.Value;
                    }
                    else
                    {
                        tmpColumnName = columnMapping.Key;
                        if (!pairs.ContainsKey(tmpColumnName))
                            pairs.Add(tmpColumnName, tableNames[pair.Key] + "." + columnMapping.Value);
                    }
                }
            }
            return pairs;
        }

        private string GetOrderByStr(IDictionary<string, string> tableNames)
        {
            StringBuilder sb = new StringBuilder();
            if (OrderByConditions != null && OrderByConditions.Any())
            {
                sb.Append(" ORDER BY ");
                foreach (var o in OrderByConditions)
                {
                    var tableAlians = tableNames[o.ItemName];
                    var methodInfo = DACItems[o.ItemName].GetType().GetMethod("GetResultSetColumnMappings");
                    var resultSetColumnMappings = new DynamicMethodExecutor(methodInfo).Execute(DACItems[o.ItemName], new object[] { }) as IEnumerable<KeyValuePair<string, string>>;
                    var columnMappings = resultSetColumnMappings.ToDictionary(i => i.Key.ToLower(), i => i.Value);
                    sb.Append(" " + tableAlians + "." + columnMappings[o.Column.ToString().ToLower()] + " " + (o.AscOrNot ? "ASC," : "DESC,") + " ");
                }
            }
            return sb.ToString().TrimEnd(", ".ToArray());
        }

        public override string GetSelectSqlStr()
        {
            string providerAlians = DataBaseInstance.ProviderAlians;
            AdoHelper ado = AdoHelper.CreateHelper(providerAlians);

            var tableNames = new Dictionary<string, string>();
            var tableSqls = new List<string>();
            foreach (var pair in DACItems)
            {
                var methodInfo = pair.Value.GetType().GetMethod("JoinNeedChildQuery");
                var needChildQuery = (bool)(new DynamicMethodExecutor(methodInfo).Execute(pair.Value, new object[] { }));
                string innerQueryStr;
                string tableAlians;
                if (needChildQuery)
                {
                    methodInfo = pair.Value.GetType().GetMethod("GetSelectSqlStr");
                    var selectSqlStr = new DynamicMethodExecutor(methodInfo).Execute(pair.Value, new object[] { }) as string;
                    innerQueryStr = "(" + selectSqlStr + ") ";
                    tableAlians = "Tmp_Table_" + Math.Abs(innerQueryStr.GetHashCode());
                    innerQueryStr += tableAlians;
                }
                else
                {
                    methodInfo = pair.Value.GetType().GetMethod("GetDBTableName");
                    var dbTableName = new DynamicMethodExecutor(methodInfo).Execute(pair.Value, new object[] { }) as string;
                    innerQueryStr = dbTableName + " ";
                    tableAlians = "Tmp_Table_" + Math.Abs(innerQueryStr.GetHashCode());
                    innerQueryStr += tableAlians;
                }
                tableNames.Add(pair.Key, tableAlians);
                tableSqls.Add(innerQueryStr);
            }
            var pairs = GetResultSetColumnMappings(tableNames);

            var sb = new StringBuilder();
            sb.Append("SELECT " + (_isDistinct ? "DISTINCT " : ""));

            foreach (KeyValuePair<string, string> pair in pairs)
            {
                sb.Append(pair.Value + " AS " + pair.Key + ", ");
            }

            if (!(_tackNum == -1 || ado is MySql))
                sb.Append(" ROW_NUMBER() OVER ( " + GetOrderByStr(tableNames) + " ) AS RowNumber ");
            var tableStr = tableSqls.Aggregate("", (current, item) => current + item + ", ").TrimEnd(", ".ToArray());
            sb.Append(" FROM " + tableStr + " ");

            var whereStr = GetWhereStr(tableNames);
            var onStr = GetOnStr(tableNames);
            if (!string.IsNullOrEmpty(onStr) && string.IsNullOrEmpty(whereStr))
                whereStr = " WHERE ";

            sb.Append(whereStr);
            sb.Append(onStr);

            if (_tackNum == -1 || ado is MySql)
                sb.Append(GetOrderByStr(tableNames));

            var ret = sb.ToString().Replace(",  FROM", " FROM").Replace("WHERE  AND", "WHERE");

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
            OrderByConditions = new List<SimpleJoinOrderByCondition>();
            WhereConditions = new List<SimpleJoinConditionBase>();
            OnConditions = new List<OnCondition>();
            JoinFieldMappings = new List<SimpleJoinFieldMapping>();
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