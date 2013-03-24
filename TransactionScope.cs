using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Runtime.Remoting.Messaging;
using Com.Dianping.Cat;

namespace DianPing.BA.Framework.DAL
{
    /// <summary>
    ///   数据库事务Scope类
    /// </summary>
    /// <remarks>
    ///   在同一个Scope内，同一个数据库连接字符串下的命令自动形成一个本地事务。如果同一个Scope内有多个数据库连接字符串，那么将产生多个本地事务，这些事务要不一起提交，要么一起回滚，从而形成分布式事务。
    /// </remarks>
    public class TransactionScope : IDisposable
    {
        public TransactionScope()
        {
            Transactions = new List<IDbTransaction>();

            var t = Cat.GetProducer().NewTransaction("TRAN", "TransactionScope");
            t.Status = "0";

            CallContext.SetData("TransactionScope", this);
        }

        public IEnumerable<IDbTransaction> Transactions { get; set; }

        #region IDisposable Members

        public void Dispose()
        {
            RollBack();
            CallContext.FreeNamedDataSlot("TransactionScope");

            Cat.GetManager().PeekTransaction.Complete();
        }

        #endregion

        public void RollBack()
        {
            foreach (var tran in Transactions.Reverse())
            {
                try
                {
                    var conn = tran.Connection;
                    try
                    {
                        tran.Rollback();
                    }
                    finally
                    {
                        conn.Close();
                        tran.Dispose();
                    }
                }
                catch
                {
                    //TODO: 记个日志啥的
                }
            }

            Transactions = new List<IDbTransaction>();
        }

        public void Comit()
        {
            foreach (var tran in Transactions)
            {
                try
                {
                    var conn = tran.Connection;
                    try
                    {
                        tran.Commit();
                    }
                    finally
                    {
                        conn.Close();
                        tran.Dispose();
                    }
                }
                catch
                {
                    //TODO: 记个日志啥的
                }
            }

            Transactions = new List<IDbTransaction>();
        }
    }
}