using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Metadata;
using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;

namespace Phy.EfBulk
{
    public static class DbBulkExtensions
    {
        /// <summary>
        /// Bulk Insert (Only basic properties are supported, navigation properties are not supported.)
        /// </summary>
        /// <typeparam name="T">Entity Type</typeparam>
        /// <param name="db">Database Context</param>
        /// <param name="entities">Data to be inserted</param>
        /// <returns>The number of rows affected.</returns>
        public static int BulkInsert<T>(this DbContext db, IEnumerable<T> entities, bool disableTran = false) where T : class
        {
            var objType = typeof(T);
            var entityType = db.Model.FindEntityType(objType);
            var result = 0;
            if (entities != null && entities.Any())
            {
                var storeObject = StoreObjectIdentifier.Table(entityType.GetTableName(), entityType.GetSchema());
                var properties = entityType.GetProperties()
                    .Where(x => x.ValueGenerated != Microsoft.EntityFrameworkCore.Metadata.ValueGenerated.OnAdd);
                var columnCount = properties.Count();
                if (columnCount > 0)
                {

                    var columnNames = new List<string>();
                    //var valueNames = new List<string>(); 
                    foreach (var p in properties)
                    {
                        columnNames.Add(p.GetColumnName(storeObject));
                        //valueNames.Add(p.Name);
                    }
                    var sqlBuilder = new StringBuilder();
                    //sqlBuilder.Clear();
                    sqlBuilder.AppendLine($"INSERT INTO {entityType.GetTableName()} ({string.Join(",", columnNames)}) VALUES ");
                    var paras = new List<object>();
                    for (var i = 0; i < entities.Count(); i++)
                    {
                        var item = entities.ElementAt(i);

                        if (i != 0)
                        {
                            sqlBuilder.Append(" , ");
                        }

                        sqlBuilder.Append("(");
                        for (var j = 0; j < columnCount; j++)
                        {
                            if (j == 0)
                            {
                                sqlBuilder.Append($"{{{j + i * columnCount}}}");
                            }
                            else
                            {
                                sqlBuilder.Append($",{{{j + i * columnCount}}}");
                            }
                        }
                        sqlBuilder.Append(")");

                        foreach (var p in properties)
                        {
                            //paras.Add(objType.GetProperty(p.Name).GetValue(item));
                            var typeMapping = p.GetTypeMapping();
                            var originalValue = objType.GetProperty(p.Name).GetValue(item);
                            if (typeMapping?.Converter?.ConvertToProviderExpression != null && originalValue != null)
                            {
                                paras.Add(typeMapping.Converter.ConvertToProviderExpression.Compile().DynamicInvoke(originalValue));
                            }
                            else if (typeMapping?.Converter?.ConvertToProvider != null)
                            {
                                paras.Add(typeMapping.Converter.ConvertToProvider(originalValue));
                            }
                            else
                            {
                                paras.Add(originalValue);
                            }
                        }
                    }
                    //foreach (var item in entities)
                    //{
                    //    sqlBuilder.AppendLine($" ({string.Join(",", valueNames.Select(x => "@" + x))})");
                    //}
                    //sqlBuilder.AppendLine($" ({string.Join(",", valueNames.Select(x => "@" + x))})");

                    result = ExecuteSqlRaw(db, sqlBuilder, disableTran, paras);
                    //using (var tran = db.Database.BeginTransaction())
                    //{
                    //    result = db.Database.ExecuteSqlRaw(sqlBuilder.ToString(), paras);
                    //    tran.Commit();
                    //    //db.Database.ExecuteSqlRaw
                    //}
                }

            }
            return result;
        }

        public static int BulkDelete<T>(this DbContext db, Expression<Func<T, bool>> predicate, bool disableTran = false) where T : class
        {
            var query = db.Set<T>().Where(predicate);
            return BulkDelete(db, query, disableTran);
        }

        public static int BulkDelete<T>(this DbContext db, IQueryable<T> query, bool disableTran = false) where T : class
        {
            var objType = typeof(T);
            var entityType = db.Model.FindEntityType(objType);
            var result = 0;
            var parseSql = ParseSql(query.ToQueryString());
            var queryParas = parseSql.Item1;
            var querySql = parseSql.Item2;
            var storeObject = StoreObjectIdentifier.Table(entityType.GetTableName(), entityType.GetSchema());
            var properties = entityType.GetProperties()
                .ToList();
            var primaryKeyProperties = properties
                .Where(x => x.IsPrimaryKey())
                .ToList();
            var sqlBuilder = new StringBuilder();
            var tableName = entityType.GetTableName();
            var joinTableName = "JoinTable1";
            if (!string.IsNullOrWhiteSpace(queryParas))
            {
                sqlBuilder.AppendLine($"{queryParas} ");
            }
            sqlBuilder.AppendLine($"DELETE {tableName} FROM {tableName} INNER JOIN( {querySql} ) AS {joinTableName} ON ( ");
            for (var i = 0; i < primaryKeyProperties.Count; i++)
            {
                var propItem = primaryKeyProperties[i];
                var columnName = propItem.GetColumnName(storeObject);
                if (i != 0)
                {
                    sqlBuilder.Append($" AND ");
                }
                sqlBuilder.Append($"{tableName}.{columnName} = {joinTableName}.{columnName}");
                //if (propItem.IsColumnNullable()) {
                //    sqlBuilder.Append($"(({tableName}.{columnName} = {joinTableName}.{columnName}) OR ({tableName}.{columnName} IS NULL AND {joinTableName}.{columnName} IS NULL))");
                //} else {
                //    sqlBuilder.Append($"{tableName}.{columnName} = {joinTableName}.{columnName}");
                //}
            }
            sqlBuilder.Append(")");

            result = ExecuteSqlRaw(db, sqlBuilder, disableTran);
            //using (var tran = db.Database.BeginTransaction())
            //{
            //    result = db.Database.ExecuteSqlRaw(sqlBuilder.ToString());
            //    tran.Commit();
            //    //db.Database.ExecuteSqlRaw
            //}
            return result;
        }

        // The method has been canceled. It is very dangerous and will update all the data. 
        //public static int BulkUpdate<T>(this DbContext db, Expression<Func<T, T>> updater, bool disableTran = false) where T : class
        //{
        //    return BulkUpdate(db, db.Set<T>(), updater);
        //}

        public static int BulkUpdate<T>(this DbContext db, Expression<Func<T, bool>> predicate, Expression<Func<T, T>> updater, bool disableTran = false) where T : class
        {
            var query = db.Set<T>().Where(predicate);
            return BulkUpdate(db, query, updater, disableTran);
        }

        public static int BulkUpdate<T>(this DbContext db, IQueryable<T> query, Expression<Func<T, T>> updater, bool disableTran = false) where T : class
        {
            var objType = typeof(T);
            var entityType = db.Model.FindEntityType(objType);
            var result = 0;

            if (updater.Body is MemberInitExpression memberInitExpression)
            {
                var parseSql = ParseSql(query.ToQueryString());
                var queryParas = parseSql.Item1;
                var querySql = parseSql.Item2;
                var paraList = new List<object>();

                var tableName = entityType.GetTableName();
                var storeObject = StoreObjectIdentifier.Table(tableName, entityType.GetSchema());
                var properties = entityType.GetProperties().ToList();
                var primaryKeyProperties = properties
                    .Where(x => x.IsPrimaryKey())
                    .ToList();
                var updateTableAlias = "ut1";
                var joinTableAlias = "jt1";
                var sqlBuilder = new StringBuilder();
                if (!string.IsNullOrWhiteSpace(queryParas))
                {
                    sqlBuilder.AppendLine($"{queryParas} ");
                }
                sqlBuilder.AppendLine($"UPDATE {tableName} AS {updateTableAlias} INNER JOIN ({querySql}) AS {joinTableAlias} ON (");
                for (var i = 0; i < primaryKeyProperties.Count; i++)
                {
                    var propItem = primaryKeyProperties[i];
                    var columnName = primaryKeyProperties[i].GetColumnName(storeObject);
                    if (i != 0)
                    {
                        sqlBuilder.Append($" AND ");
                    }
                    sqlBuilder.Append($"{updateTableAlias}.{columnName} = {joinTableAlias}.{columnName}");
                }
                sqlBuilder.AppendLine(")");
                sqlBuilder.Append(" SET ");

                var bindings = memberInitExpression.Bindings;
                for (var i = 0; i < bindings.Count; i++)
                {
                    var bind = bindings[i];
                    if (i != 0)
                    {
                        sqlBuilder.Append(", ");
                    }
                    //bind.BindingType
                    var property = properties.FirstOrDefault(x => x.Name == bind.Member.Name);
                    var columnName = property.GetColumnName(storeObject);
                    var memberAssignment = bind as MemberAssignment;
                    if (memberAssignment == null)
                    {
                        throw new ArgumentException("Bindings of updater expression must only by type MemberAssignment.", nameof(updater));
                    }
                    Expression memberExpression = memberAssignment.Expression;

                    //ParameterExpression parameterExpression = null;
                    //ExpressionVisitor<ParameterExpression>
                    ParameterExpression parameterExpression = null;

                    new CustomExpressionVisitor<ParameterExpression>((p) =>
                    {
                        if (p.Type == typeof(T))
                        {
                            parameterExpression = p;
                        }
                        return p;
                    })
                    .Visit(memberExpression);
                    if (parameterExpression == null)
                    {
                        object value;
                        if (memberExpression.NodeType == ExpressionType.Constant)
                        {
                            var constantExpression = memberExpression as ConstantExpression;
                            if (constantExpression == null)
                                throw new ArgumentException(
                                    "The MemberAssignment expression is not a ConstantExpression.", "updateExpression");
                            value = constantExpression.Value;
                        }
                        else
                        {
                            LambdaExpression lambda = Expression.Lambda(memberExpression, null);
                            value = lambda.Compile().DynamicInvoke();
                        }

                        if (value != null)
                        {
                            sqlBuilder.Append($"{updateTableAlias}.{columnName} = {{{paraList.Count}}}");
                            paraList.Add(value);
                        }
                        else
                        {
                            sqlBuilder.Append($"{updateTableAlias}.{columnName} = NULL");
                        }
                    }
                    else
                    {
                        //var selectSql = db.Set<T>().Select(updater).ToQueryString();
                        //var objectSet = db.Set<T>();

                        var sql1 = query.Select(updater).ToQueryString();
                        Type[] typeArguments = new[] { entityType.ClrType, memberExpression.Type };

                        //ConstantExpression constantExpression = Expression.Constant(query);

                        LambdaExpression lambdaExpression = Expression.Lambda(memberExpression, parameterExpression);

                        MethodCallExpression selectExpression = Expression.Call(
                            typeof(Queryable),
                            "Select",
                            typeArguments,
                            query.Expression,
                            lambdaExpression);

                        //MethodCallExpression.Invoke(selectExpression,objectSet)
                        //var selectQuery = objectSet.Select(selectExpression);
                        //create query from expression
                        //var q1 = query.Provider.CreateQuery(selectExpression);

                        string selectSql = query.Provider.CreateQuery(selectExpression).ToQueryString();

                        // parse select part of sql to use as update
                        //string regex = @"SELECT\s*(?<ColumnValue>.+)?(\s*AS\s*(?<ColumnAlias>\w+))?\s*FROM\s*(?<TableName>\w+\.\w+|\w+)\s*(AS\s*(?<TableAlias>\w+))?";
                        //string regex = @"SELECT\s*(?<ColumnValue>\S+)(\s*AS\s*(?<ColumnAlias>\S+))?\s*FROM\s*(?<TableName>\S+)?\s*(AS\s*(?<TableAlias>\S+))?";
                        string regex = @"SELECT\s*(?<Column>(\S|\s)+)\s*FROM\s*(?<TableName>(\w+)|(`\w+`)|(\[\w+\]))?\s*(AS\s*(?<TableAlias>(\w+)|(`\w+`)|(\[\w+\])))?";
                        Match match = Regex.Match(selectSql, regex, RegexOptions.IgnoreCase);
                        if (!match.Success)
                        {
                            throw new ArgumentException("The MemberAssignment expression could not be processed.", nameof(updater));
                        }
                        var column = match.Groups["Column"].Value;
                        var columnReg = @"\s*(((?<ColumnValue>[\s\S]+)\s*AS\s*(?<ColumnAlias>[`\[\]\w]+))|(?<ColumnValue>[\s\S]+))\s*$";
                        var colMatch = Regex.Match(column, columnReg, RegexOptions.IgnoreCase);

                        string alias = match.Groups["TableAlias"].Value;
                        string value = colMatch.Groups["ColumnValue"].Value;
                        //string aliasWithDot = $"{alias}.";
                        //if (value.StartsWith(aliasWithDot)) {
                        //    value = value.Substring(aliasWithDot.Length);
                        //}
                        //value = value.Replace(" "+ aliasWithDot, $" {updateTableAlias}.")
                        //    .Replace("\r" + aliasWithDot, $"{updateTableAlias}.")
                        //    .Replace("\n" + aliasWithDot, $"{updateTableAlias}.");
                        if (!string.IsNullOrEmpty(alias))
                        {
                            var aliasReg = @$"(?<!\w){alias}";
                            value = Regex.Replace(value, aliasReg, updateTableAlias);
                        }

                        sqlBuilder.Append($"{updateTableAlias}.{columnName} = {value}");
                    }
                }
                //var sqlRaw = sqlBuilder.ToString();
                result = ExecuteSqlRaw(db, sqlBuilder, disableTran, paraList);
                //using (var tran = db.Database.BeginTransaction())
                //{
                //    result = db.Database.ExecuteSqlRaw(sqlBuilder.ToString(), paraList);
                //    tran.Commit();
                //}
            }

            return result;
        }

        private static Tuple<string, string> ParseSql(string sql)
        {

            var match = Regex.Match(sql, @"^(?<SqlParas>((\s*SET\s+[@\w]+\s*=\s*\S+;\s+)+)?)(?<SqlBody>(\s*SELECT\s+(\S|\s)+))$", RegexOptions.IgnoreCase);
            if (match.Success)
            {
                return new Tuple<string, string>(match.Groups["SqlParas"].Value, match.Groups["SqlBody"].Value);
            }
            return new Tuple<string, string>(string.Empty, sql);
        }

        private static int ExecuteSqlRaw(DbContext db, StringBuilder sqlBuilder, bool disableTran, [NotNullAttribute] params object[] parameters)
        {
            int result;
            if (disableTran)
            {
                result = db.Database.ExecuteSqlRaw(sqlBuilder.ToString(), parameters);
            }
            else
            {
                using (var tran = db.Database.BeginTransaction())
                {
                    result = db.Database.ExecuteSqlRaw(sqlBuilder.ToString(), parameters);
                    tran.Commit();
                }
            }
            return result;
        }

        private static int ExecuteSqlRaw(DbContext db, StringBuilder sqlBuilder, bool disableTran, [NotNullAttribute] IEnumerable<object> parameters)
        {
            int result;
            if (disableTran)
            {
                result = db.Database.ExecuteSqlRaw(sqlBuilder.ToString(), parameters);
            }
            else
            {
                using (var tran = db.Database.BeginTransaction())
                {
                    result = db.Database.ExecuteSqlRaw(sqlBuilder.ToString(), parameters);
                    tran.Commit();
                }
            }
            return result;
        }

    }

}
