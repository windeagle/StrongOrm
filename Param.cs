using System.Collections.Generic;
using System.Data;

namespace DianPing.BA.Framework.DAL
{
    public class Param : List<IDataParameter>
    {
        public Param(DbProvideType provider)
        {
            Ado = AdoHelper.CreateHelper(provider);
        }

        public Param()
        {
        }

        //public DbProvideType Provider { get; set; }

        public AdoHelper Ado { get; set; }
    }
}